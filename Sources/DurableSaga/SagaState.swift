import Foundation

public struct ParkInfo: Equatable, Sendable {
    public let step: StepID
    public let direction: Direction
    public let message: String
}

public struct SagaStateError: Error, Equatable, Sendable, CustomStringConvertible {
    public let seq: Int
    public let reason: String

    public var description: String { "journal record \(seq) rejected: \(reason)" }
}

/// The state of one saga, defined as the left fold of its journal.
///
/// `apply(_:)` is the single place that decides whether an event is legal. The
/// coordinator runs every event through it **before** writing it (so it never
/// persists something recovery would reject), and recovery runs the persisted
/// events through it again. Live execution and crash recovery therefore cannot
/// disagree about what state a saga is in.
public struct SagaState: Equatable, Sendable {
    public let sagaID: SagaID
    public let definitionName: String
    private let stepOrder: [StepID]
    private let kinds: [StepID: StepKind]
    private let pivot: StepID?

    public private(set) var key: String?
    public private(set) var nextSeq = 0
    public private(set) var outputs: [StepID: String] = [:]
    /// Total forward attempts ever journaled per step (survives crashes and revivals).
    public private(set) var forwardAttempts: [StepID: Int] = [:]
    /// Attempts counted against the retry budget; reset by `revived`.
    public private(set) var forwardBudgetUsed: [StepID: Int] = [:]
    public private(set) var compensationAttempts: [StepID: Int] = [:]
    public private(set) var compensationBudgetUsed: [StepID: Int] = [:]
    public private(set) var compensated: Set<StepID> = []
    /// A forward intent with no outcome yet: the in-doubt window.
    public private(set) var pendingForward: StepID?
    public private(set) var pendingCompensation: StepID?
    public private(set) var lastError: [StepID: String] = [:]
    /// Whether the most recent failure of each step was a definitive rejection.
    public private(set) var lastFailurePermanent: [StepID: Bool] = [:]
    public private(set) var abortReason: AbortReason?
    public private(set) var cancelRequested = false
    public private(set) var parked: ParkInfo?
    public private(set) var outcome: SagaOutcome?

    public init(sagaID: SagaID, definition: SagaDefinition) {
        self.sagaID = sagaID
        self.definitionName = definition.name
        self.stepOrder = definition.steps.map(\.id)
        var kinds: [StepID: StepKind] = [:]
        for step in definition.steps { kinds[step.id] = step.kind }
        self.kinds = kinds
        self.pivot = definition.pivotID
    }

    /// Folds a whole journal. Throws on the first illegal record.
    public static func fold(_ records: [JournalRecord], sagaID: SagaID,
                            definition: SagaDefinition) throws -> SagaState {
        var state = SagaState(sagaID: sagaID, definition: definition)
        for record in records { try state.apply(record) }
        return state
    }

    public var hasStarted: Bool { nextSeq > 0 }

    /// Committed means the pivot succeeded: from here the saga can only move forward.
    public var isCommitted: Bool {
        guard let pivot else { return false }
        return outputs[pivot] != nil
    }

    /// The pivot has been attempted, has not succeeded, and was never
    /// definitively rejected: it may have committed on the server. True while
    /// its intent is open, *and* after a transient failure (during backoff, or
    /// while parked), which is the window a naive "is an intent open?" check misses.
    public var isPivotInDoubt: Bool {
        guard let pivot, outputs[pivot] == nil, (forwardAttempts[pivot] ?? 0) > 0 else { return false }
        return lastFailurePermanent[pivot] != true
    }

    /// Steps whose side effect may exist: every step with at least one
    /// journaled intent. Compensation covers these, not just observed successes.
    public var attempted: Set<StepID> {
        Set(forwardAttempts.compactMap { $0.value > 0 ? $0.key : nil })
    }

    /// The first step, in definition order, with no recorded output.
    public var nextUnfinishedStep: StepID? {
        stepOrder.first { outputs[$0] == nil }
    }

    /// The compensatable steps still to undo, in the order they must run
    /// (reverse definition order).
    public var pendingCompensations: [StepID] {
        let attempted = self.attempted
        return stepOrder.reversed().filter {
            kinds[$0] == .compensatable && attempted.contains($0) && !compensated.contains($0)
        }
    }

    func kind(of step: StepID) -> StepKind? { kinds[step] }

    func isAfterPivot(_ step: StepID) -> Bool {
        guard let pivot,
              let p = stepOrder.firstIndex(of: pivot),
              let s = stepOrder.firstIndex(of: step) else { return false }
        return s > p
    }

    // MARK: - The reducer

    public mutating func apply(_ record: JournalRecord) throws {
        func reject(_ reason: String) -> SagaStateError {
            SagaStateError(seq: record.seq, reason: reason)
        }
        guard record.sagaID == sagaID else { throw reject("record belongs to \(record.sagaID)") }
        guard record.seq == nextSeq else { throw reject("expected seq \(nextSeq)") }
        guard outcome == nil else { throw reject("saga already finished") }
        if nextSeq == 0 {
            guard case .started = record.event else { throw reject("first record must be started") }
        }
        func known(_ step: StepID) throws {
            guard kinds[step] != nil else { throw reject("unknown step \(step)") }
        }

        switch record.event {
        case let .started(definition, key):
            guard nextSeq == 0 else { throw reject("started may only be the first record") }
            guard definition == definitionName else { throw reject("definition mismatch: \(definition)") }
            self.key = key

        case let .stepIntent(step, attempt):
            try known(step)
            guard abortReason == nil else { throw reject("forward intent during compensation") }
            guard parked == nil else { throw reject("forward intent while parked") }
            guard pendingForward == nil || pendingForward == step else {
                throw reject("intent for \(step) while \(pendingForward?.rawValue ?? "") is in doubt")
            }
            guard step == nextUnfinishedStep else { throw reject("\(step) is not the next step") }
            let previous = forwardAttempts[step] ?? 0
            guard attempt == Saturating.add(previous, 1) else { throw reject("attempt \(attempt) out of order") }
            forwardAttempts[step] = attempt
            forwardBudgetUsed[step] = Saturating.add(forwardBudgetUsed[step] ?? 0, 1)
            lastFailurePermanent[step] = nil
            pendingForward = step

        case let .stepSucceeded(step, output):
            guard pendingForward == step else { throw reject("success for \(step) without an open intent") }
            outputs[step] = output
            pendingForward = nil
            lastError[step] = nil

        case let .stepFailed(step, attempt, message, permanent):
            guard pendingForward == step else { throw reject("failure for \(step) without an open intent") }
            guard attempt == forwardAttempts[step] else { throw reject("failure attempt mismatch") }
            pendingForward = nil
            lastError[step] = message
            lastFailurePermanent[step] = permanent

        case .cancelRequested:
            cancelRequested = true

        case let .compensationStarted(reason):
            guard abortReason == nil else { throw reject("compensation already started") }
            guard !isCommitted else { throw reject("cannot compensate a committed saga") }
            // Rolling back while the pivot is in doubt would be a guess: it may
            // have committed. Only compensatable steps may be in doubt here, and
            // they are covered by `attempted`.
            guard !isPivotInDoubt else { throw reject("cannot start compensation while the pivot is in doubt") }
            if let pending = pendingForward, kinds[pending] != .compensatable {
                throw reject("cannot start compensation while \(pending) is in doubt")
            }
            abortReason = reason
            pendingForward = nil

        case let .compensationIntent(step, attempt):
            try known(step)
            guard abortReason != nil else { throw reject("compensation intent outside compensation") }
            guard parked == nil else { throw reject("compensation intent while parked") }
            guard pendingCompensation == nil || pendingCompensation == step else {
                throw reject("compensation of \(step) while another is in doubt")
            }
            guard pendingCompensations.first == step else {
                throw reject("\(step) is not the next compensation")
            }
            let previous = compensationAttempts[step] ?? 0
            guard attempt == Saturating.add(previous, 1) else { throw reject("compensation attempt out of order") }
            compensationAttempts[step] = attempt
            compensationBudgetUsed[step] = Saturating.add(compensationBudgetUsed[step] ?? 0, 1)
            pendingCompensation = step

        case let .compensationSucceeded(step):
            guard pendingCompensation == step else { throw reject("compensation success without intent") }
            compensated.insert(step)
            pendingCompensation = nil

        case let .compensationFailed(step, attempt, message):
            guard pendingCompensation == step else { throw reject("compensation failure without intent") }
            guard attempt == compensationAttempts[step] else { throw reject("compensation attempt mismatch") }
            pendingCompensation = nil
            lastError[step] = message

        case let .parked(step, direction, message):
            try known(step)
            guard parked == nil else { throw reject("already parked") }
            switch direction {
            case .forward:
                guard abortReason == nil else { throw reject("forward park during compensation") }
                let parkable = kinds[step] == .pivot || (isCommitted && isAfterPivot(step))
                guard parkable else { throw reject("\(step) cannot be parked forward") }
                guard pendingForward == nil || pendingForward == step else { throw reject("park while another step is in doubt") }
                pendingForward = nil
            case .backward:
                guard abortReason != nil else { throw reject("backward park outside compensation") }
                guard pendingCompensation == nil || pendingCompensation == step else { throw reject("park while another compensation is in doubt") }
                pendingCompensation = nil
            }
            parked = ParkInfo(step: step, direction: direction, message: message)

        case .revived:
            guard let info = parked else { throw reject("revive without park") }
            parked = nil
            switch info.direction {
            case .forward:
                forwardBudgetUsed[info.step] = 0
                lastFailurePermanent[info.step] = nil   // the operator asked for another try
            case .backward: compensationBudgetUsed[info.step] = 0
            }

        case let .finished(result):
            guard pendingForward == nil, pendingCompensation == nil else { throw reject("finish with an effect in doubt") }
            guard parked == nil else { throw reject("finish while parked") }
            switch result {
            case let .completed(recorded, ignored):
                guard abortReason == nil, nextUnfinishedStep == nil else { throw reject("completed with steps unfinished") }
                guard recorded == outputs, ignored == cancelRequested else { throw reject("completed outcome does not match state") }
            case let .compensated(reason):
                guard abortReason == reason else { throw reject("compensated reason mismatch") }
                guard pendingCompensations.isEmpty else { throw reject("compensated with compensations outstanding") }
            case .parked:
                throw reject("a parked outcome is not terminal")
            }
            outcome = result
        }
        nextSeq = Saturating.add(nextSeq, 1)
    }
}
