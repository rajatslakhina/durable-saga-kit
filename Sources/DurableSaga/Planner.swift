import Foundation

/// What the coordinator should do next for a saga.
public enum SagaAction: Equatable, Sendable {
    /// Run (or re-run, with the same idempotency key) a forward step.
    case execute(StepID)
    case compensate(StepID)
    /// Journal the switch from forward to backward.
    case beginCompensation(AbortReason)
    /// Journal the consequence of a failure that is already in the journal
    /// but whose follow-up record was lost to a crash (see `onGiveUp`).
    case giveUp(JournalEvent)
    /// Journal the terminal outcome.
    case finish(SagaOutcome)
    /// Nothing to do until an operator revives the saga.
    case awaitingRevival(SagaOutcome)
    /// Already finished.
    case done(SagaOutcome)
}

/// A pure function from state to the next action. Keeping the decision out of
/// the actor means every recovery branch is unit-testable without async code.
public enum SagaPlanner {
    public static func next(_ state: SagaState) -> SagaAction {
        if let outcome = state.outcome { return .done(outcome) }
        if let park = state.parked {
            return .awaitingRevival(.parked(step: park.step, direction: park.direction, lastError: park.message))
        }

        if let reason = state.abortReason {
            if let inDoubt = state.pendingCompensation { return .compensate(inDoubt) }
            if let nextUndo = state.pendingCompensations.first { return .compensate(nextUndo) }
            return .finish(.compensated(reason: reason))
        }

        // Forward. A cancel before the pivot has been attempted rolls back, unless the
        // pivot is in doubt (attempted, not succeeded, never definitively
        // rejected): then the pivot must be resolved first, re-executed with
        // its idempotency key, because it may already have committed.
        if state.cancelRequested && !state.isCommitted && !state.isPivotInDoubt {
            return .beginCompensation(.cancelled)
        }
        if let inDoubt = state.pendingForward { return .execute(inDoubt) }
        if let step = state.nextUnfinishedStep {
            // A definitive rejection is already journaled: the process died
            // between `stepFailed(permanent: true)` and its follow-up record.
            // Re-executing would re-charge a declined card.
            if state.lastFailurePermanent[step] == true, let kind = state.kind(of: step) {
                return .giveUp(onGiveUp(step: step, kind: kind, permanent: true,
                                        message: state.lastError[step] ?? "permanent failure"))
            }
            return .execute(step)
        }
        return .finish(.completed(outputs: state.outputs, cancellationIgnored: state.cancelRequested))
    }

    /// What a step failure means once retrying is no longer an option
    /// (permanent error, or budget exhausted).
    ///
    /// - compensatable → roll back (its compensation runs by idempotency key,
    ///   so an ambiguous earlier attempt is undone too);
    /// - pivot, **permanent** → roll back: the server said no, nothing committed;
    /// - pivot, **transient** → park forward: the last attempt may have
    ///   committed, so rolling back would be a guess;
    /// - retriable → park forward: past the pivot, backwards is not an option.
    public static func onGiveUp(step: StepID, kind: StepKind, permanent: Bool, message: String) -> JournalEvent {
        switch kind {
        case .compensatable:
            return .compensationStarted(reason: .stepFailed(step: step, message: message))
        case .pivot:
            return permanent
                ? .compensationStarted(reason: .stepFailed(step: step, message: message))
                : .parked(step: step, direction: .forward, message: message)
        case .retriable:
            return .parked(step: step, direction: .forward, message: message)
        }
    }
}
