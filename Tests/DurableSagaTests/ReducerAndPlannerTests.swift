import XCTest
@testable import DurableSaga

final class ReducerAndPlannerTests: XCTestCase {
    private let id = SagaID(definition: "checkout", key: "k")
    private var definition: SagaDefinition!

    override func setUpWithError() throws {
        definition = try Fixtures.checkout(server: FakeServer())
    }

    private func state(_ events: [JournalEvent]) throws -> SagaState {
        let records = events.enumerated().map { JournalRecord(sagaID: id, seq: $0.offset, event: $0.element) }
        return try SagaState.fold(records, sagaID: id, definition: definition)
    }

    private func assertRejected(_ events: [JournalEvent], file: StaticString = #filePath, line: UInt = #line) {
        XCTAssertThrowsError(try state(events), file: file, line: line) {
            XCTAssertTrue($0 is SagaStateError, "\($0)", file: file, line: line)
        }
    }

    private let start: JournalEvent = .started(definition: "checkout", key: "k")

    // MARK: Reducer validation

    func testHappyPathFoldsToCompletion() throws {
        let s = try state([start,
                           .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                           .stepIntent(step: "authorize", attempt: 1), .stepSucceeded(step: "authorize", output: "a"),
                           .stepIntent(step: "place", attempt: 1), .stepSucceeded(step: "place", output: "p"),
                           .stepIntent(step: "receipt", attempt: 1), .stepSucceeded(step: "receipt", output: "x"),
                           .finished(.completed(outputs: ["reserve": "r", "authorize": "a", "place": "p", "receipt": "x"],
                                                cancellationIgnored: false))])
        XCTAssertTrue(s.isCommitted)
        XCTAssertEqual(s.nextSeq, 10)
        XCTAssertEqual(SagaPlanner.next(s), .done(.completed(outputs: s.outputs, cancellationIgnored: false)))
    }

    func testReducerRejectsIllegalHistories() {
        assertRejected([.stepIntent(step: "reserve", attempt: 1)])                                  // no started
        assertRejected([start, start])                                                              // started twice
        assertRejected([.started(definition: "other", key: "k")])                                   // wrong definition
        assertRejected([start, .stepSucceeded(step: "reserve", output: "r")])                       // no intent
        assertRejected([start, .stepIntent(step: "authorize", attempt: 1)])                         // skipped a step
        assertRejected([start, .stepIntent(step: "reserve", attempt: 2)])                           // attempt gap
        assertRejected([start, .stepIntent(step: "nope", attempt: 1)])                              // unknown step
        assertRejected([start, .stepIntent(step: "reserve", attempt: 1),
                        .stepFailed(step: "reserve", attempt: 2, message: "", permanent: false)])   // attempt mismatch
        assertRejected([start, .compensationIntent(step: "reserve", attempt: 1)])                   // not compensating
        assertRejected([start, .finished(.parked(step: "reserve", direction: .forward, lastError: ""))])
        assertRejected([start, .stepIntent(step: "reserve", attempt: 1),
                        .finished(.compensated(reason: .cancelled))])                               // finish while in doubt
        assertRejected([start, .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                        .parked(step: "reserve", direction: .forward, message: "")])                // pre-pivot forward park
    }

    func testReducerRejectsSequenceGapsAndForeignRecords() {
        let gap = [JournalRecord(sagaID: id, seq: 0, event: start),
                   JournalRecord(sagaID: id, seq: 2, event: .cancelRequested)]
        XCTAssertThrowsError(try SagaState.fold(gap, sagaID: id, definition: definition))
        let foreign = [JournalRecord(sagaID: SagaID(rawValue: "other/k"), seq: 0, event: start)]
        XCTAssertThrowsError(try SagaState.fold(foreign, sagaID: id, definition: definition))
    }

    func testCannotCompensateACommittedSaga() {
        assertRejected([start,
                        .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                        .stepIntent(step: "authorize", attempt: 1), .stepSucceeded(step: "authorize", output: "a"),
                        .stepIntent(step: "place", attempt: 1), .stepSucceeded(step: "place", output: "p"),
                        .compensationStarted(reason: .cancelled)])
    }

    func testCannotStartCompensationWhileThePivotIsInDoubt() {
        assertRejected([start,
                        .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                        .stepIntent(step: "authorize", attempt: 1), .stepSucceeded(step: "authorize", output: "a"),
                        .stepIntent(step: "place", attempt: 1),
                        .compensationStarted(reason: .cancelled)])
    }

    func testCompensationsMustRunInReverseOrder() {
        assertRejected([start,
                        .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                        .stepIntent(step: "authorize", attempt: 1), .stepSucceeded(step: "authorize", output: "a"),
                        .compensationStarted(reason: .cancelled),
                        .compensationIntent(step: "reserve", attempt: 1)])          // authorize must go first
    }

    func testFinishedOutcomeMustMatchState() {
        assertRejected([start,
                        .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                        .compensationStarted(reason: .cancelled),
                        .finished(.compensated(reason: .cancelled))])                // reserve not yet undone
        assertRejected([start, .compensationStarted(reason: .cancelled),
                        .finished(.compensated(reason: .stepFailed(step: "reserve", message: "x")))])
    }

    func testNothingIsAcceptedAfterFinish() {
        assertRejected([start, .compensationStarted(reason: .cancelled),
                        .finished(.compensated(reason: .cancelled)), .cancelRequested])
    }

    func testReviveResetsOnlyTheBudgetNotTheAttemptNumbering() throws {
        var events: [JournalEvent] = [start]
        for step in ["reserve", "authorize"] {
            events += [.stepIntent(step: StepID(step), attempt: 1), .stepSucceeded(step: StepID(step), output: step)]
        }
        events += [.stepIntent(step: "place", attempt: 1),
                   .stepFailed(step: "place", attempt: 1, message: "t", permanent: false),
                   .parked(step: "place", direction: .forward, message: "t"),
                   .revived]
        let s = try state(events)
        XCTAssertEqual(s.forwardBudgetUsed["place"], 0)
        XCTAssertEqual(s.forwardAttempts["place"], 1)
        XCTAssertEqual(SagaPlanner.next(s), .execute("place"))
    }

    // MARK: Planner

    func testCancelBeforeCommitRollsBack() throws {
        let s = try state([start, .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                           .cancelRequested])
        XCTAssertEqual(SagaPlanner.next(s), .beginCompensation(.cancelled))
    }

    func testCancelWhilePivotInDoubtResolvesThePivotFirst() throws {
        // If the planner rolled back here, an order the server may already have
        // placed would be orphaned with its payment voided.
        let s = try state([start,
                           .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                           .stepIntent(step: "authorize", attempt: 1), .stepSucceeded(step: "authorize", output: "a"),
                           .stepIntent(step: "place", attempt: 1),
                           .cancelRequested])
        XCTAssertEqual(SagaPlanner.next(s), .execute("place"))
    }

    func testPivotStaysInDoubtAfterATransientFailure() throws {
        // The open-intent window is not the only doubt window: after a lost
        // response the intent is closed, but the order may still exist.
        let events: [JournalEvent] = [start,
            .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
            .stepIntent(step: "authorize", attempt: 1), .stepSucceeded(step: "authorize", output: "a"),
            .stepIntent(step: "place", attempt: 1),
            .stepFailed(step: "place", attempt: 1, message: "timeout", permanent: false),
            .cancelRequested]
        let s = try state(events)
        XCTAssertTrue(s.isPivotInDoubt)
        XCTAssertEqual(SagaPlanner.next(s), .execute("place"))
        assertRejected(events + [.compensationStarted(reason: .cancelled)])
    }

    func testPermanentPivotRejectionEndsTheDoubt() throws {
        let s = try state([start,
            .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
            .stepIntent(step: "authorize", attempt: 1), .stepSucceeded(step: "authorize", output: "a"),
            .stepIntent(step: "place", attempt: 1),
            .stepFailed(step: "place", attempt: 1, message: "timeout", permanent: false),
            .stepIntent(step: "place", attempt: 2),
            .stepFailed(step: "place", attempt: 2, message: "out of stock", permanent: true),
            .cancelRequested])
        XCTAssertFalse(s.isPivotInDoubt)
        XCTAssertEqual(SagaPlanner.next(s), .beginCompensation(.cancelled))
    }

    func testAJournaledPermanentFailureIsActedOnNotRetried() throws {
        let s = try state([start,
            .stepIntent(step: "reserve", attempt: 1),
            .stepFailed(step: "reserve", attempt: 1, message: "sold out", permanent: true)])
        XCTAssertEqual(SagaPlanner.next(s),
                       .giveUp(.compensationStarted(reason: .stepFailed(step: "reserve", message: "sold out"))))
    }

    func testCancelAfterCommitIsIgnoredAndReported() throws {
        let s = try state([start,
                           .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                           .stepIntent(step: "authorize", attempt: 1), .stepSucceeded(step: "authorize", output: "a"),
                           .stepIntent(step: "place", attempt: 1), .stepSucceeded(step: "place", output: "p"),
                           .cancelRequested])
        XCTAssertEqual(SagaPlanner.next(s), .execute("receipt"))
    }

    func testCompensationCoversAttemptedStepsNotOnlyObservedSuccesses() throws {
        // authorize timed out: we never saw its output, but the hold may exist.
        let s = try state([start,
                           .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                           .stepIntent(step: "authorize", attempt: 1),
                           .stepFailed(step: "authorize", attempt: 1, message: "timeout", permanent: false),
                           .compensationStarted(reason: .stepFailed(step: "authorize", message: "timeout"))])
        XCTAssertEqual(s.pendingCompensations, ["authorize", "reserve"])
        XCTAssertEqual(SagaPlanner.next(s), .compensate("authorize"))
    }

    func testInDoubtCompensationIsRetriedBeforeMovingOn() throws {
        let s = try state([start,
                           .stepIntent(step: "reserve", attempt: 1), .stepSucceeded(step: "reserve", output: "r"),
                           .stepIntent(step: "authorize", attempt: 1), .stepSucceeded(step: "authorize", output: "a"),
                           .compensationStarted(reason: .cancelled),
                           .compensationIntent(step: "authorize", attempt: 1)])
        XCTAssertEqual(SagaPlanner.next(s), .compensate("authorize"))
    }

    func testGiveUpTable() {
        XCTAssertEqual(SagaPlanner.onGiveUp(step: "c", kind: .compensatable, permanent: false, message: "m"),
                       .compensationStarted(reason: .stepFailed(step: "c", message: "m")))
        XCTAssertEqual(SagaPlanner.onGiveUp(step: "p", kind: .pivot, permanent: true, message: "m"),
                       .compensationStarted(reason: .stepFailed(step: "p", message: "m")))
        XCTAssertEqual(SagaPlanner.onGiveUp(step: "p", kind: .pivot, permanent: false, message: "m"),
                       .parked(step: "p", direction: .forward, message: "m"))
        XCTAssertEqual(SagaPlanner.onGiveUp(step: "r", kind: .retriable, permanent: true, message: "m"),
                       .parked(step: "r", direction: .forward, message: "m"))
    }
}
