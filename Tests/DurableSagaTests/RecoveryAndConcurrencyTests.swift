import XCTest
@testable import DurableSaga

final class RecoveryAndConcurrencyTests: XCTestCase {
    private func coordinator(_ journal: SagaJournal, _ definition: SagaDefinition, retain: Int = 100) throws -> SagaCoordinator {
        try SagaCoordinator(journal: journal, definitions: [definition], retryPolicy: Fixtures.fastPolicy,
                            sleeper: RecordingSleeper(), random: FixedRandom(value: 0), retainFinished: retain)
    }

    private let o = { (key: String) in SagaID(definition: "checkout", key: key) }

    // MARK: Crash recovery

    func testCrashInTheDoubtWindowIsResolvedByReexecutingWithTheSameKey() async throws {
        let server = FakeServer()
        let journal = InMemoryJournal()
        let definition = try Fixtures.checkout(server: server)
        let first = try coordinator(journal, definition)
        let box = Box<SagaCoordinator>()
        box.value = first
        // The effect lands on the server, then the process dies before the
        // success is journaled.
        await server.afterApply("authorize") { await box.value?.retire() }

        do {
            _ = try await first.start("checkout", key: "c1")
            XCTFail("the retired coordinator must not report an outcome")
        } catch {
            XCTAssertEqual(error as? CoordinatorError, .retired)
        }

        let relaunched = try coordinator(journal, definition)
        let report = try await relaunched.resumeAll()

        guard case .completed = report.outcomes[o("c1")] else { return XCTFail("\(report)") }
        let calls = await server.calls["authorize"]
        let applied = await server.applyCount[Fixtures.key("c1", "authorize")]
        XCTAssertEqual(calls, 2, "re-executed after the crash")
        XCTAssertEqual(applied, 1, "but applied once")
        let intents = try await journal.records(for: o("c1")).compactMap { r -> Int? in
            if case let .stepIntent("authorize", attempt) = r.event { return attempt } else { return nil }
        }
        XCTAssertEqual(intents, [1, 2])
    }

    func testCrashDuringTheFinalAttemptDoesNotSpendAnExtraAttempt() async throws {
        let server = FakeServer()
        let journal = InMemoryJournal()
        let id = o("c2")
        let epoch = try await journal.open()
        let events: [JournalEvent] = [
            .started(definition: "checkout", key: "c2"),
            .stepIntent(step: "reserve", attempt: 1), .stepFailed(step: "reserve", attempt: 1, message: "503", permanent: false),
            .stepIntent(step: "reserve", attempt: 2), .stepFailed(step: "reserve", attempt: 2, message: "503", permanent: false),
            .stepIntent(step: "reserve", attempt: 3)              // process died here
        ]
        for (seq, event) in events.enumerated() {
            try await journal.append(JournalRecord(sagaID: id, seq: seq, event: event), epoch: epoch)
        }

        let report = try await coordinator(journal, try Fixtures.checkout(server: server)).resumeAll()

        XCTAssertEqual(report.outcomes[id], .compensated(reason: .stepFailed(step: "reserve", message: "503")))
        let forwardCalls = await server.calls["reserve"]
        let undoCalls = await server.undoCalls["reserve"]
        XCTAssertNil(forwardCalls, "budget was spent; no fourth attempt")
        XCTAssertEqual(undoCalls, 1, "the possibly-applied third attempt is undone by key")
    }

    func testResumeReportsFinishedParkedAndUnknownSagasWithoutRerunning() async throws {
        let server = FakeServer()
        let journal = InMemoryJournal()
        let definition = try Fixtures.checkout(server: server)
        _ = try await coordinator(journal, definition).start("checkout", key: "done")
        let before = await server.totalApplied()

        // A saga written by an older app version whose definition no longer exists.
        let legacy = SagaID(definition: "legacy", key: "k")
        let epoch = try await journal.open()
        try await journal.append(JournalRecord(sagaID: legacy, seq: 0, event: .started(definition: "legacy", key: "k")),
                                 epoch: epoch)

        let report = try await coordinator(journal, definition).resumeAll()

        guard case .completed = report.outcomes[o("done")] else { return XCTFail("\(report)") }
        XCTAssertNotNil(report.failures[legacy])
        let after = await server.totalApplied()
        XCTAssertEqual(before, after, "resuming a finished saga re-ran a step")
    }

    // MARK: Cancellation

    func testCancelBeforeThePivotRollsBack() async throws {
        let server = FakeServer()
        let gate = Gate()
        let c = try coordinator(InMemoryJournal(), try Fixtures.checkout(server: server, gate: gate, gateStep: "authorize"))
        let run = Task { try await c.start("checkout", key: "x1") }
        await gate.waitForArrivals(1)
        try await c.cancel(o("x1"))
        await gate.release()

        let outcome = try await run.value
        XCTAssertEqual(outcome, .compensated(reason: .cancelled))
        let live = await server.liveEffects()
        XCTAssertTrue(live.isEmpty, "\(live)")
    }

    func testCallerCancellationIsShieldedFromStepsAndBecomesACancelRequest() async throws {
        let server = FakeServer()
        let gate = Gate()
        let observedCancellation = Box<Bool>()
        let definition = try SagaDefinition(name: "checkout", steps: [
            .compensatable("reserve", execute: { inv in
                await gate.wait()
                observedCancellation.value = Task.isCancelled
                return try await server.perform(inv)
            }, compensate: { try await server.undo($0) }),
            .pivot("place", execute: { try await server.perform($0) })
        ])
        let c = try coordinator(InMemoryJournal(), definition)
        let caller = Task { try await c.start("checkout", key: "x2") }
        await gate.waitForArrivals(1)
        caller.cancel()
        // Wait for the cancel request to become durable before letting the step finish.
        while try await c.state(of: o("x2"))?.cancelRequested != true { await Task.yield() }
        await gate.release()

        let outcome = try await caller.value
        XCTAssertEqual(outcome, .compensated(reason: .cancelled), "caller gets the real outcome, not CancellationError")
        XCTAssertEqual(observedCancellation.value, false, "the in-flight side effect saw the caller's cancellation")
        let placed = await server.calls["place"]
        XCTAssertNil(placed)
    }

    func testCancelAfterCommitIsIgnoredAndTheSagaFinishes() async throws {
        let server = FakeServer()
        let gate = Gate()
        let c = try coordinator(InMemoryJournal(), try Fixtures.checkout(server: server, gate: gate, gateStep: "receipt"))
        let run = Task { try await c.start("checkout", key: "x3") }
        await gate.waitForArrivals(1)
        try await c.cancel(o("x3"))
        await gate.release()

        let outcome = try await run.value
        guard case let .completed(_, ignored) = outcome else { return XCTFail("\(outcome)") }
        XCTAssertTrue(ignored)
        let undo = await server.undoCalls
        XCTAssertTrue(undo.isEmpty)
    }

    // MARK: Concurrency

    func testConcurrentStartsWithOneKeyShareOneRun() async throws {
        let server = FakeServer()
        let gate = Gate()
        let journal = InMemoryJournal()
        let c = try coordinator(journal, try Fixtures.checkout(server: server, gate: gate, gateStep: "reserve"))
        let a = Task { try await c.start("checkout", key: "d1") }
        let b = Task { try await c.start("checkout", key: "d1") }
        await gate.waitForArrivals(1)
        for _ in 0..<50 { await Task.yield() }       // give a second drive loop every chance to appear
        let arrivals = await gate.arrivals
        XCTAssertEqual(arrivals, 1, "two drive loops for one saga")
        await gate.release()

        let (ra, rb) = (try await a.value, try await b.value)
        XCTAssertEqual(ra, rb)
        let starts = try await journal.records(for: o("d1")).filter {
            if case .started = $0.event { return true } else { return false }
        }
        XCTAssertEqual(starts.count, 1)
    }

    func testCancelArrivingDuringAnAppendWaitsForTheSagaLock() async throws {
        // Forces the exact interleaving the per-saga lock exists for: the drive
        // loop is suspended inside a journal append when cancel arrives. Without
        // the lock both writers pick the same seq and one fails with
        // `sequenceConflict`.
        let server = FakeServer()
        let stepGate = Gate()
        let appendGate = Gate()
        let journal = GatedJournal()
        let c = try coordinator(journal, try Fixtures.checkout(server: server, gate: stepGate, gateStep: "reserve"))
        let run = Task { try await c.start("checkout", key: "l1") }
        await stepGate.waitForArrivals(1)
        await journal.holdNextAppend(at: appendGate)
        await stepGate.release()
        await appendGate.waitForArrivals(1)                // drive is inside append(stepSucceeded)

        let lockedID = o("l1")
        let cancel = Task { try await c.cancel(lockedID) }
        for _ in 0..<50 { await Task.yield() }
        await appendGate.release()

        try await cancel.value
        let outcome = try await run.value
        XCTAssertEqual(outcome, .compensated(reason: .cancelled))
        let records = try await journal.records(for: o("l1"))
        XCTAssertNoThrow(try SagaState.fold(records, sagaID: o("l1"), definition: try Fixtures.checkout(server: server)))
    }

    func testAZombieCoordinatorIsFencedOutBySuccessor() async throws {
        let server = FakeServer()
        let gate = Gate()
        let journal = InMemoryJournal()
        let definition = try Fixtures.checkout(server: server, gate: gate, gateStep: "reserve")
        let zombie = try coordinator(journal, definition)
        let zombieRun = Task { try await zombie.start("checkout", key: "z1") }
        await gate.waitForArrivals(1)

        let successor = try coordinator(journal, definition)
        let successorRun = Task { try await successor.resumeAll() }
        await gate.waitForArrivals(2)
        await gate.release()

        do {
            _ = try await zombieRun.value
            XCTFail("zombie wrote after being superseded")
        } catch {
            guard case .fenced? = error as? JournalError else { return XCTFail("\(error)") }
        }
        let report = try await successorRun.value
        guard case .completed = report.outcomes[o("z1")] else { return XCTFail("\(report)") }
        let applied = await server.applyCount[Fixtures.key("z1", "reserve")]
        XCTAssertEqual(applied, 1)
    }

    func testInMemoryJournalFencingAndSequenceChecks() async throws {
        let journal = InMemoryJournal()
        let old = try await journal.open()
        let new = try await journal.open()
        XCTAssertLessThan(old, new)
        let r0 = JournalRecord(sagaID: o("f"), seq: 0, event: .started(definition: "checkout", key: "f"))
        do { try await journal.append(r0, epoch: old); XCTFail() } catch {
            XCTAssertEqual(error as? JournalError, .fenced(current: new, attempted: old))
        }
        try await journal.append(r0, epoch: new)
        do { try await journal.append(r0, epoch: new); XCTFail() } catch {
            XCTAssertEqual(error as? JournalError, .sequenceConflict(saga: o("f"), expected: 1, got: 0))
        }
    }

    func testFailedJournalWriteLeavesMemoryConsistentAndResumable() async throws {
        let server = FakeServer()
        let journal = InMemoryJournal()
        let definition = try Fixtures.checkout(server: server)
        let c = try coordinator(journal, definition)
        await server.afterApply("reserve") { await journal.injectAppendFailures(1) }   // the success write fails
        do { _ = try await c.start("checkout", key: "w1"); XCTFail() } catch {
            XCTAssertEqual(error as? JournalError, .io("injected append failure"))
        }
        let state = try await c.state(of: o("w1"))
        XCTAssertEqual(state?.pendingForward, "reserve", "memory must not run ahead of the journal")

        let outcome = try await c.start("checkout", key: "w1")
        guard case .completed = outcome else { return XCTFail("\(outcome)") }
        let applied = await server.applyCount[Fixtures.key("w1", "reserve")]
        XCTAssertEqual(applied, 1)
    }

    func testAReadOnlySuccessorStillFencesThePredecessor() async throws {
        // Fence-then-read: merely loading state in a new coordinator takes the
        // epoch, so the old one cannot append behind the history just read.
        let server = FakeServer()
        let gate = Gate()
        let journal = InMemoryJournal()
        let definition = try Fixtures.checkout(server: server, gate: gate, gateStep: "reserve")
        let old = try coordinator(journal, definition)
        let run = Task { try await old.start("checkout", key: "rf1") }
        await gate.waitForArrivals(1)
        _ = try await coordinator(journal, definition).state(of: o("rf1"))
        await gate.release()
        do { _ = try await run.value; XCTFail("predecessor wrote after a successor read") } catch {
            guard case .fenced? = error as? JournalError else { return XCTFail("\(error)") }
        }
    }

    func testAnUnloadableSagaDoesNotDisableRetention() async throws {
        let journal = InMemoryJournal()
        let epoch = try await journal.open()
        try await journal.append(JournalRecord(sagaID: SagaID(definition: "legacy", key: "k"), seq: 0,
                                               event: .started(definition: "legacy", key: "k")), epoch: epoch)
        let c = try coordinator(journal, try Fixtures.checkout(server: FakeServer()), retain: 1)
        for key in ["u1", "u2", "u3", "u4"] { _ = try await c.start("checkout", key: key) }
        // retain 1 + slack 1: the batch prune fires when a third saga finishes.
        let ids = try await journal.sagaIDs()
        XCTAssertEqual(ids, [SagaID(definition: "checkout", key: "u3"), SagaID(definition: "checkout", key: "u4"),
                             SagaID(definition: "legacy", key: "k")])
    }

    // MARK: Permanent failures already in the journal (review round 2)

    private func seed(_ journal: InMemoryJournal, _ id: SagaID, _ events: [JournalEvent]) async throws {
        let epoch = try await journal.open()
        for (seq, event) in events.enumerated() {
            try await journal.append(JournalRecord(sagaID: id, seq: seq, event: event), epoch: epoch)
        }
    }

    func testCrashAfterAJournaledDeclineDoesNotRetryTheCharge() async throws {
        let server = FakeServer()
        let journal = InMemoryJournal()
        let id = o("pf1")
        try await seed(journal, id, [
            .started(definition: "checkout", key: "pf1"),
            .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
            .stepIntent(step: "authorize", attempt: 1),
            .stepFailed(step: "authorize", attempt: 1, message: "declined", permanent: true)
            // process died before `compensationStarted` was written
        ])
        let report = try await coordinator(journal, try Fixtures.checkout(server: server)).resumeAll()
        XCTAssertEqual(report.outcomes[id], .compensated(reason: .stepFailed(step: "authorize", message: "declined")))
        let authorizeCalls = await server.calls["authorize"]
        XCTAssertNil(authorizeCalls, "re-sent a request the server had definitively declined")
    }

    func testCrashAfterAJournaledPivotRejectionRollsBack() async throws {
        let server = FakeServer()
        let journal = InMemoryJournal()
        let id = o("pf2")
        try await seed(journal, id, [
            .started(definition: "checkout", key: "pf2"),
            .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
            .stepIntent(step: "authorize", attempt: 1), .stepSucceeded(step: "authorize", output: "a"),
            .stepIntent(step: "place", attempt: 1),
            .stepFailed(step: "place", attempt: 1, message: "timeout", permanent: false),
            .stepIntent(step: "place", attempt: 2),
            .stepFailed(step: "place", attempt: 2, message: "timeout", permanent: false),
            .stepIntent(step: "place", attempt: 3),
            .stepFailed(step: "place", attempt: 3, message: "out of stock", permanent: true)
        ])
        let report = try await coordinator(journal, try Fixtures.checkout(server: server)).resumeAll()
        XCTAssertEqual(report.outcomes[id], .compensated(reason: .stepFailed(step: "place", message: "out of stock")),
                       "a definitive rejection on the final attempt must roll back, not park")
        let placeCalls = await server.calls["place"]
        XCTAssertNil(placeCalls)
    }

    func testRevivingAParkedStepAfterAPermanentFailureTriesAgain() async throws {
        let server = FakeServer()
        await server.script("receipt", failures: [.permanent("bad address")])
        let c = try coordinator(InMemoryJournal(), try Fixtures.checkout(server: server))
        let parked = try await c.start("checkout", key: "pf3")
        XCTAssertEqual(parked, .parked(step: "receipt", direction: .forward, lastError: "bad address"))
        let revived = try await c.revive(o("pf3"))       // operator fixed the address
        guard case .completed = revived else { return XCTFail("\(revived)") }
    }

    func testConcurrentRevivesAttachToOneRun() async throws {
        let server = FakeServer()
        await server.loseResponse("place", times: 3)
        let c = try coordinator(InMemoryJournal(), try Fixtures.checkout(server: server))
        _ = try await c.start("checkout", key: "rv1")
        let id = o("rv1")
        async let a = c.revive(id)
        async let b = c.revive(id)
        let (ra, rb) = try await (a, b)
        XCTAssertEqual(ra, rb)
        let applied = await server.applyCount[Fixtures.key("rv1", "place")]
        XCTAssertEqual(applied, 1)
    }

    func testConcurrentPrunesOverEarlierProcessSagasDoNotDeadlock() async throws {
        // Round-3 review: two prunes each collected the same earlier-process
        // saga across a suspension, inserted it twice, and the batched remove
        // then took that saga's lock twice and hung forever.
        let journal = InMemoryJournal()
        let definition = try Fixtures.checkout(server: FakeServer())
        let first = try coordinator(journal, definition)
        for key in ["e1", "e2", "e3"] { _ = try await first.start("checkout", key: key) }

        for _ in 0..<20 {
            let relaunched = try coordinator(journal, definition)
            async let a = relaunched.pruneFinished(keepingLast: 0)
            async let b = relaunched.pruneFinished(keepingLast: 0)
            _ = try await (a, b)
            let ids = try await journal.sagaIDs()
            XCTAssertTrue(ids.isEmpty)
            for key in ["e1", "e2", "e3"] { _ = try await relaunched.start("checkout", key: key) }
        }
    }

    // MARK: Retention

    func testRetentionBoundsTheJournalAndDefinesTheDedupeWindow() async throws {
        let server = FakeServer()
        let journal = InMemoryJournal()
        let c = try coordinator(journal, try Fixtures.checkout(server: server), retain: 2)
        for key in ["r1", "r2", "r3", "r4", "r5"] { _ = try await c.start("checkout", key: key) }
        // retain 2 + slack 1: one batch prune when r4 finishes removes r1 and r2
        // in a single journal call; r5 then brings the backlog back to 3.
        let remaining = try await journal.sagaIDs()
        XCTAssertEqual(remaining, [o("r3"), o("r4"), o("r5")])

        // Inside the window a repeated key attaches to the finished saga...
        let journaledBefore = await journal.recordCount()
        _ = try await c.start("checkout", key: "r5")
        let journaledAfter = await journal.recordCount()
        XCTAssertEqual(journaledBefore, journaledAfter)
        // ...outside it, the key starts a new saga; only server-side dedupe remains.
        _ = try await c.start("checkout", key: "r1")
        let ids = try await journal.sagaIDs()
        XCTAssertTrue(ids.contains(o("r1")))
        let manualPrune = try await c.pruneFinished(keepingLast: 0)
        XCTAssertEqual(manualPrune, 2)
        let empty = try await journal.sagaIDs()
        XCTAssertTrue(empty.isEmpty)
    }
}
