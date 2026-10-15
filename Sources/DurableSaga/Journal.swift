import Foundation

/// Why a saga rolled back.
public enum AbortReason: Equatable, Sendable, Codable {
    /// A step before the commit point failed permanently or ran out of retries.
    case stepFailed(step: StepID, message: String)
    /// Cancellation was requested before the commit point.
    case cancelled
}

public enum Direction: String, Equatable, Sendable, Codable {
    case forward
    case backward
}

/// The terminal (or parked) result of a saga.
public enum SagaOutcome: Equatable, Sendable, Codable {
    /// Every step succeeded. `cancellationIgnored` is true when a cancel arrived
    /// once the pivot had been attempted: from then on the order may exist, so
    /// a cancel cannot roll the saga back.
    case completed(outputs: [StepID: String], cancellationIgnored: Bool)
    /// The saga rolled back: every attempted compensatable step was compensated.
    case compensated(reason: AbortReason)
    /// A step exhausted its retries where giving up is not allowed (after the
    /// pivot, or during compensation). The saga is durably parked; nothing
    /// re-drives it until `SagaCoordinator.revive(_:)` is called.
    case parked(step: StepID, direction: Direction, lastError: String)

    var isTerminal: Bool {
        if case .parked = self { return false }
        return true
    }
}

/// One fact in the write-ahead journal. The journal is the only source of
/// truth: the in-memory state of a running saga is always the fold of these
/// events, and recovery after a crash is the same fold.
public enum JournalEvent: Equatable, Sendable, Codable {
    case started(definition: String, key: String)
    /// Written **before** the side effect. An intent with no outcome after it
    /// means "the effect may or may not have happened" (the in-doubt window).
    case stepIntent(step: StepID, attempt: Int)
    case stepSucceeded(step: StepID, output: String)
    case stepFailed(step: StepID, attempt: Int, message: String, permanent: Bool)
    case cancelRequested
    /// The direction switch is itself journaled, so a crash mid-rollback
    /// resumes the rollback instead of re-deciding it.
    case compensationStarted(reason: AbortReason)
    case compensationIntent(step: StepID, attempt: Int)
    case compensationSucceeded(step: StepID)
    case compensationFailed(step: StepID, attempt: Int, message: String)
    case parked(step: StepID, direction: Direction, message: String)
    case revived
    case finished(SagaOutcome)
}

public struct JournalRecord: Equatable, Sendable, Codable {
    public let sagaID: SagaID
    /// Per-saga sequence number, contiguous from 0. A gap means a lost write
    /// and is reported as corruption rather than silently folded.
    public let seq: Int
    public let event: JournalEvent

    public init(sagaID: SagaID, seq: Int, event: JournalEvent) {
        self.sagaID = sagaID
        self.seq = seq
        self.event = event
    }
}

public enum JournalError: Error, Equatable, Sendable {
    /// The append came from an epoch older than the journal's current one.
    case fenced(current: Epoch, attempted: Epoch)
    /// The record's sequence number is not the next one for its saga.
    case sequenceConflict(saga: SagaID, expected: Int, got: Int)
    /// A record in the middle of the log failed its checksum or did not parse.
    case corrupt(line: Int)
    case io(String)
}

/// Durable, append-only storage for saga journals.
///
/// Contract:
/// - `open()` returns an epoch strictly greater than any previously returned;
///   from then on, appends carrying an older epoch throw `.fenced`.
/// - `append` is atomic per record and enforces contiguous per-saga `seq`.
/// - `remove` deletes whole sagas (only finished ones, by retention), in one call.
public protocol SagaJournal: Sendable {
    func open() async throws -> Epoch
    func append(_ record: JournalRecord, epoch: Epoch) async throws
    func records(for saga: SagaID) async throws -> [JournalRecord]
    func sagaIDs() async throws -> [SagaID]
    func remove(_ sagas: [SagaID], epoch: Epoch) async throws
}

/// In-memory journal: the reference implementation, and the fake used by the
/// tests and the demo. It can simulate a write failure for fault injection.
public actor InMemoryJournal: SagaJournal {
    private var bySaga: [SagaID: [JournalRecord]] = [:]
    private var currentEpoch = Epoch(0)
    private var failNextAppends = 0

    public init() {}

    public func open() async throws -> Epoch {
        currentEpoch = Epoch(Saturating.add(currentEpoch.value, 1))
        return currentEpoch
    }

    public func append(_ record: JournalRecord, epoch: Epoch) async throws {
        guard epoch == currentEpoch else {
            throw JournalError.fenced(current: currentEpoch, attempted: epoch)
        }
        if failNextAppends > 0 {
            failNextAppends -= 1
            throw JournalError.io("injected append failure")
        }
        let expected = bySaga[record.sagaID]?.count ?? 0
        guard record.seq == expected else {
            throw JournalError.sequenceConflict(saga: record.sagaID, expected: expected, got: record.seq)
        }
        bySaga[record.sagaID, default: []].append(record)
    }

    public func records(for saga: SagaID) async throws -> [JournalRecord] {
        bySaga[saga] ?? []
    }

    public func sagaIDs() async throws -> [SagaID] {
        bySaga.keys.sorted()
    }

    public func remove(_ sagas: [SagaID], epoch: Epoch) async throws {
        guard epoch == currentEpoch else {
            throw JournalError.fenced(current: currentEpoch, attempted: epoch)
        }
        for saga in sagas { bySaga[saga] = nil }
    }

    /// Fault injection for tests: the next `count` appends throw `.io`.
    func injectAppendFailures(_ count: Int) {
        failNextAppends = max(0, count)
    }

    /// Total records held, for retention tests and the demo's journal view.
    public func recordCount() -> Int {
        bySaga.values.reduce(0) { Saturating.add($0, $1.count) }
    }
}
