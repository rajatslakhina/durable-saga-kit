import XCTest
@testable import DurableSaga

final class CoordinatorTests: XCTestCase {
    private func makeCoordinator(_ journal: SagaJournal, _ definition: SagaDefinition,
                                 sleeper: RecordingSleeper = RecordingSleeper(),
                                 retain: Int = 100) throws -> SagaCoordinator {
        try SagaCoordinator(journal: journal, definitions: [definition], retryPolicy: Fixtures.fastPolicy,
                            sleeper: sleeper, random: FixedRandom(value: .max), retainFinished: retain)
    }

    private let allSteps: Set<String> = ["reserve", "authorize", "place", "receipt"]

    func testHappyPathCompletesWithEveryEffectAppliedOnce() async throws {
        let server = FakeServer()
        let journal = InMemoryJournal()
        let coordinator = try makeCoordinator(journal, try Fixtures.checkout(server: server))

        let outcome = try await coordinator.start("checkout", key: "o1")

        guard case let .completed(outputs, ignored) = outcome else { return XCTFail("\(outcome)") }
        XCTAssertEqual(Set(outputs.keys.map(\.rawValue)), allSteps)
        XCTAssertFalse(ignored)
        let applied = await server.totalApplied()
        XCTAssertEqual(applied, 4)
        let records = try await journal.records(for: SagaID(definition: "checkout", key: "o1"))
        XCTAssertEqual(records.map(\.seq), Array(0..<records.count))
        XCTAssertEqual(records.last?.event, .finished(outcome))
    }

    func testIntentIsDurableBeforeTheSideEffectStarts() async throws {
        let journal = InMemoryJournal()
        let seen = Box<[JournalEvent]>()
        let definition = try SagaDefinition(name: "w", steps: [
            .pivot("p", execute: { inv in
                seen.value = try await journal.records(for: inv.sagaID).map(\.event)
                return "done"
            })
        ])
        let coordinator = try makeCoordinator(journal, definition)
        _ = try await coordinator.start("w", key: "k")
        XCTAssertEqual(seen.value?.last, .stepIntent(step: "p", attempt: 1),
                       "the effect ran before its intent was journaled")
    }

    func testTransientFailuresRetryWithTheSameKeyAndBoundedBackoff() async throws {
        let server = FakeServer()
        await server.loseResponse("authorize", times: 2)     // effect applied, response lost twice
        let sleeper = RecordingSleeper()
        let coordinator = try makeCoordinator(InMemoryJournal(), try Fixtures.checkout(server: server), sleeper: sleeper)

        let outcome = try await coordinator.start("checkout", key: "o2")

        guard case .completed = outcome else { return XCTFail("\(outcome)") }
        let calls = await server.calls["authorize"]
        let applied = await server.applyCount[Fixtures.key("o2", "authorize")]
        XCTAssertEqual(calls, 3)
        XCTAssertEqual(applied, 1, "three requests, one effect: the key did not change across retries")
        XCTAssertEqual(sleeper.delays, [1_000, 2_000], "full jitter is capped by base·2^(n-1)")
    }

    func testPermanentFailureBeforePivotCompensatesInReverse() async throws {
        let server = FakeServer()
        await server.script("authorize", failures: [.permanent("card declined")])
        let journal = InMemoryJournal()
        let coordinator = try makeCoordinator(journal, try Fixtures.checkout(server: server))

        let outcome = try await coordinator.start("checkout", key: "o3")

        XCTAssertEqual(outcome, .compensated(reason: .stepFailed(step: "authorize", message: "card declined")))
        let live = await server.liveEffects()
        XCTAssertTrue(live.isEmpty, "rollback left effects in force: \(live)")
        let placeCalls = await server.calls["place"]
        XCTAssertNil(placeCalls, "the pivot must not run after an earlier failure")
        let events = try await journal.records(for: SagaID(definition: "checkout", key: "o3")).map(\.event)
        let undoOrder = events.compactMap { if case let .compensationIntent(s, _) = $0 { return s } else { return nil } }
        XCTAssertEqual(undoOrder, ["authorize", "reserve"])
    }

    func testAmbiguousFailureIsUndoneByKey() async throws {
        // authorize applied its hold, then every response was lost. We never
        // saw an output, but the hold exists and must be released.
        let server = FakeServer()
        await server.loseResponse("authorize", times: 10)
        let coordinator = try makeCoordinator(InMemoryJournal(), try Fixtures.checkout(server: server))

        let outcome = try await coordinator.start("checkout", key: "o4")

        guard case .compensated = outcome else { return XCTFail("\(outcome)") }
        let live = await server.liveEffects()
        XCTAssertTrue(live.isEmpty, "the unobserved hold leaked: \(live)")
    }

    func testPivotTransientExhaustionParksInsteadOfGuessing() async throws {
        let server = FakeServer()
        await server.loseResponse("place", times: 3)          // the order may well exist
        let coordinator = try makeCoordinator(InMemoryJournal(), try Fixtures.checkout(server: server))

        let outcome = try await coordinator.start("checkout", key: "o5")

        guard case let .parked(step, direction, _) = outcome else { return XCTFail("\(outcome)") }
        XCTAssertEqual(step, "place")
        XCTAssertEqual(direction, .forward)
        let undoCalls = await server.undoCalls
        XCTAssertTrue(undoCalls.isEmpty, "rolled back a pivot that may have committed")

        // Operator revives once the dependency is healthy: the pivot is resolved
        // with the same key, and the saga finishes forward.
        let revived = try await coordinator.revive(SagaID(definition: "checkout", key: "o5"))
        guard case .completed = revived else { return XCTFail("\(revived)") }
        let placed = await server.applyCount[Fixtures.key("o5", "place")]
        XCTAssertEqual(placed, 1)
    }

    func testPivotPermanentFailureRollsBack() async throws {
        let server = FakeServer()
        await server.script("place", failures: [.permanent("out of stock")])
        let coordinator = try makeCoordinator(InMemoryJournal(), try Fixtures.checkout(server: server))
        let outcome = try await coordinator.start("checkout", key: "o6")
        XCTAssertEqual(outcome, .compensated(reason: .stepFailed(step: "place", message: "out of stock")))
    }

    func testRetriableStepAfterCommitParksAndNeverCompensates() async throws {
        let server = FakeServer()
        await server.script("receipt", failures: Array(repeating: .transient("mail down"), count: 3))
        let coordinator = try makeCoordinator(InMemoryJournal(), try Fixtures.checkout(server: server))

        let outcome = try await coordinator.start("checkout", key: "o7")

        XCTAssertEqual(outcome, .parked(step: "receipt", direction: .forward, lastError: "mail down"))
        let undoCalls = await server.undoCalls
        XCTAssertTrue(undoCalls.isEmpty)
        let revived = try await coordinator.revive(SagaID(definition: "checkout", key: "o7"))
        guard case .completed = revived else { return XCTFail("\(revived)") }
    }

    func testCompensationThatKeepsFailingParksBackward() async throws {
        let server = FakeServer()
        await server.script("authorize", failures: [.permanent("declined")])
        await server.scriptUndo("reserve", failures: Array(repeating: .transient("503"), count: 3))
        let coordinator = try makeCoordinator(InMemoryJournal(), try Fixtures.checkout(server: server))

        let outcome = try await coordinator.start("checkout", key: "o8")
        XCTAssertEqual(outcome, .parked(step: "reserve", direction: .backward, lastError: "503"))

        let revived = try await coordinator.revive(SagaID(definition: "checkout", key: "o8"))
        XCTAssertEqual(revived, .compensated(reason: .stepFailed(step: "authorize", message: "declined")))
        let live = await server.liveEffects()
        XCTAssertTrue(live.isEmpty)
    }

    func testReviveRejectsASagaThatIsNotParked() async throws {
        let coordinator = try makeCoordinator(InMemoryJournal(), try Fixtures.checkout(server: FakeServer()))
        _ = try await coordinator.start("checkout", key: "o9")
        do {
            _ = try await coordinator.revive(SagaID(definition: "checkout", key: "o9"))
            XCTFail("expected notParked")
        } catch {
            XCTAssertEqual(error as? CoordinatorError, .notParked(SagaID(definition: "checkout", key: "o9")))
        }
    }

    func testUnknownDefinitionAndSaga() async throws {
        let coordinator = try makeCoordinator(InMemoryJournal(), try Fixtures.checkout(server: FakeServer()))
        do { _ = try await coordinator.start("nope", key: "k"); XCTFail() } catch {
            XCTAssertEqual(error as? CoordinatorError, .unknownDefinition("nope"))
        }
        do { try await coordinator.cancel(SagaID(rawValue: "checkout/ghost")); XCTFail() } catch {
            XCTAssertEqual(error as? CoordinatorError, .unknownSaga(SagaID(rawValue: "checkout/ghost")))
        }
        XCTAssertThrowsError(try SagaCoordinator(journal: InMemoryJournal(),
                                                 definitions: [try Fixtures.checkout(server: FakeServer()),
                                                               try Fixtures.checkout(server: FakeServer())]))
    }

    // MARK: Pivot doubt outside the open-intent window (review round 1 blockers)

    func testCancelDuringPivotBackoffDoesNotRollBackAPossiblyPlacedOrder() async throws {
        let server = FakeServer()
        await server.loseResponse("place", times: 1)       // order placed, response lost
        let sleeper = GatedSleeper()
        let c = try SagaCoordinator(journal: InMemoryJournal(), definitions: [try Fixtures.checkout(server: server)],
                                    retryPolicy: Fixtures.fastPolicy, sleeper: sleeper, random: FixedRandom(value: 0))
        let run = Task { try await c.start("checkout", key: "pb1") }
        await sleeper.gate.waitForArrivals(1)              // pivot failed transiently; now in backoff
        try await c.cancel(SagaID(definition: "checkout", key: "pb1"))
        await sleeper.gate.release()

        let outcome = try await run.value
        guard case let .completed(_, ignored) = outcome else { return XCTFail("rolled back a pivot that may exist: \(outcome)") }
        XCTAssertTrue(ignored)
        let undo = await server.undoCalls
        XCTAssertTrue(undo.isEmpty, "voided payment / released stock on a placed order")
    }

    func testCancelWhileParkedAtThePivotThenReviveFinishesForward() async throws {
        let server = FakeServer()
        await server.loseResponse("place", times: 3)
        let c = try makeCoordinator(InMemoryJournal(), try Fixtures.checkout(server: server))
        let id = SagaID(definition: "checkout", key: "pb2")
        let parked = try await c.start("checkout", key: "pb2")
        guard case .parked = parked else { return XCTFail("\(parked)") }
        try await c.cancel(id)
        let outcome = try await c.revive(id)
        guard case let .completed(_, ignored) = outcome else { return XCTFail("\(outcome)") }
        XCTAssertTrue(ignored)
        let live = await server.liveEffects()
        XCTAssertTrue(live.contains(Fixtures.key("pb2", "place")))
        XCTAssertTrue(live.contains(Fixtures.key("pb2", "authorize")), "payment hold must survive with the order")
    }

    func testCancelFromAnAlreadyCancelledCallerIsNotLost() async throws {
        // The caller is cancelled *before* it calls `start`, so the
        // cancellation handler fires the instant it is installed. That is
        // only safe because `started` is journaled first.
        let server = FakeServer()
        let entry = Gate()
        let hold = Gate()
        let c = try makeCoordinator(InMemoryJournal(), try Fixtures.checkout(server: server, gate: hold, gateStep: "reserve"))
        let id = SagaID(definition: "checkout", key: "pc1")
        let caller = Task { () -> SagaOutcome in
            await entry.wait()
            XCTAssertTrue(Task.isCancelled)
            return try await c.start("checkout", key: "pc1")
        }
        await entry.waitForArrivals(1)
        caller.cancel()
        await entry.release()
        // The first step is held, so the run cannot outpace the cancel request.
        await hold.waitForArrivals(1)
        var observed = false
        for _ in 0..<100_000 {
            if try await c.state(of: id)?.cancelRequested == true { observed = true; break }
            await Task.yield()
        }
        XCTAssertTrue(observed, "the caller's cancellation was dropped")
        await hold.release()
        let outcome = try await caller.value
        XCTAssertEqual(outcome, .compensated(reason: .cancelled))
    }
}
