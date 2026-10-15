import Foundation

public enum CoordinatorError: Error, Equatable, Sendable {
    case duplicateDefinition(String)
    case unknownDefinition(String)
    case unknownSaga(SagaID)
    /// This coordinator was retired (simulated process death, or replaced by
    /// a successor). It performs no further journal writes.
    case retired
    case notParked(SagaID)
}

/// Result of `resumeAll()` after a relaunch.
public struct ResumeReport: Sendable, Equatable {
    public var outcomes: [SagaID: SagaOutcome] = [:]
    /// Sagas that could not be resumed, with the reason: an unknown definition
    /// after an app update, or a history the reducer rejects. (Mid-log
    /// corruption in `FileJournal` is not per-saga: `open()` throws and
    /// `resumeAll` fails as a whole, by design. See the README.)
    public var failures: [SagaID: String] = [:]
}

/// Runs sagas against a write-ahead journal.
///
/// Guarantees, each defended in the README and pinned by a test:
/// 1. **Write-ahead.** An intent is durable before its side effect starts, so
///    after a crash the coordinator knows exactly which effects are in doubt.
/// 2. **At-most-once effects (with a cooperating server).** Retries and
///    post-crash re-execution reuse the step's idempotency key.
/// 3. **Single writer per saga.** One drive task per saga, and every journal
///    append for a saga goes through a FIFO per-saga lock, so a `cancel` that
///    arrives mid-step cannot race the drive loop for a sequence number.
/// 4. **Fencing.** Appends carry this coordinator's epoch; once a successor
///    opens the journal, a zombie predecessor cannot write.
/// 5. **Must-finish once the pivot may have happened.** The caller's task
///    cancellation is translated into a journaled cancel *request*. Before the
///    pivot is attempted it rolls the saga back; once the pivot is in doubt or
///    committed it is recorded and ignored. The steps
///    themselves run in a task the caller does not own, so cancelling the
///    caller never interrupts a half-finished side effect.
public actor SagaCoordinator {
    private let journal: SagaJournal
    private let definitions: [String: SagaDefinition]
    private let policy: RetryPolicy
    private let sleeper: Sleeper
    private let random: RandomSource
    private let retainFinished: Int
    private let pruneSlack: Int

    private var epochTask: Task<Epoch, Error>?
    private var states: [SagaID: SagaState] = [:]
    private var running: [SagaID: Task<SagaOutcome, Error>] = [:]
    private var finishOrder: [SagaID] = []
    private var retired = false

    // Per-saga FIFO lock with direct hand-off (no lost wake-ups).
    private var lockHeld: Set<SagaID> = []
    private var lockWaiters: [SagaID: [CheckedContinuation<Void, Never>]] = [:]

    public init(journal: SagaJournal,
                definitions: [SagaDefinition],
                retryPolicy: RetryPolicy = .default,
                sleeper: Sleeper = TaskSleeper(),
                random: RandomSource = SystemRandomSource(),
                retainFinished: Int = 100) throws {
        var byName: [String: SagaDefinition] = [:]
        for definition in definitions {
            guard byName[definition.name] == nil else {
                throw CoordinatorError.duplicateDefinition(definition.name)
            }
            byName[definition.name] = definition
        }
        self.journal = journal
        self.definitions = byName
        self.policy = retryPolicy
        self.sleeper = sleeper
        self.random = random
        self.retainFinished = max(0, retainFinished)
        self.pruneSlack = max(1, self.retainFinished / 4)
    }

    // MARK: - Public API

    /// Starts the saga for `key`, or attaches to the existing one. Returns its
    /// outcome. Cancelling the calling task requests cancellation of the saga
    /// (honoured only if the pivot has not been attempted) and still waits for the real outcome.
    public func start(_ definitionName: String, key: String) async throws -> SagaOutcome {
        guard definitions[definitionName] != nil else {
            throw CoordinatorError.unknownDefinition(definitionName)
        }
        let id = SagaID(definition: definitionName, key: key)
        // Journal `started` before installing the cancellation handler, so a
        // cancel from an already-cancelled caller always finds the saga.
        try await lockedLoad(id, create: key)
        let task = try driveTask(for: id, create: key)
        return try await withTaskCancellationHandler {
            try await task.value
        } onCancel: {
            Task { try? await self.cancel(id) }
        }
    }

    /// Journals a cancel request. Idempotent; a no-op for a finished saga.
    public func cancel(_ id: SagaID) async throws {
        await acquire(id)
        defer { release(id) }
        try await loadLocked(id, create: nil)
        guard let state = states[id], state.outcome == nil, !state.cancelRequested else { return }
        try await commitLocked(id, .cancelRequested)
    }

    /// After a relaunch: drives every unfinished, unparked saga in the journal.
    public func resumeAll() async throws -> ResumeReport {
        try checkRetired()
        _ = try await currentEpoch()          // fence predecessors before reading
        var report = ResumeReport()
        var tasks: [(SagaID, Task<SagaOutcome, Error>)] = []
        for id in try await journal.sagaIDs() {
            do {
                try await lockedLoad(id, create: nil)
                guard let state = states[id] else { continue }
                if let outcome = state.outcome {
                    report.outcomes[id] = outcome
                } else if let park = state.parked {
                    report.outcomes[id] = .parked(step: park.step, direction: park.direction, lastError: park.message)
                } else {
                    tasks.append((id, try driveTask(for: id, create: nil)))
                }
            } catch {
                report.failures[id] = String(describing: error)
            }
        }
        for (id, task) in tasks {
            do { report.outcomes[id] = try await task.value } catch { report.failures[id] = String(describing: error) }
        }
        return report
    }

    /// Re-drives a parked saga with a fresh retry budget for the parked step.
    /// If the saga is already being driven (a concurrent `revive` or `start`),
    /// attaches to that run and returns its outcome. Throws `notParked` for a
    /// saga that is neither parked nor running.
    public func revive(_ id: SagaID) async throws -> SagaOutcome {
        if let inFlight = try await reviveLocked(id) {
            return try await inFlight.value          // a concurrent revive already started it
        }
        return try await driveTask(for: id, create: nil).value
    }

    public func state(of id: SagaID) async throws -> SagaState? {
        try await lockedLoad(id, create: nil)
        return states[id]
    }

    /// Retention: removes finished sagas from the journal and from memory,
    /// keeping the `keepingLast` most recently finished. Called automatically,
    /// in one batch, once more than `retainFinished + max(1, retainFinished / 4)`
    /// sagas have finished, so neither the journal nor memory grows without
    /// bound and steady state does not rewrite the log on every finish. A saga
    /// whose drive task is still running (including the one whose finish
    /// triggered the prune) is skipped and goes in a later batch. Returns how many were
    /// removed. Sagas finished before this process started count as oldest.
    @discardableResult
    public func pruneFinished(keepingLast: Int) async throws -> Int {
        try checkRetired()
        let keep = max(0, keepingLast)
        // Pick up finished sagas from earlier processes, oldest first.
        // Their relative order is by SagaID, not finish time: the journal
        // records no clock, and inventing one would be a lie.
        var earlier: [SagaID] = []
        for id in try await journal.sagaIDs() where !finishOrder.contains(id) {
            // One unloadable saga (unknown definition after an app update,
            // corrupt history) must not disable retention for all the others.
            guard (try? await lockedLoad(id, create: nil)) != nil else { continue }
            // Re-check after the suspension: the saga may have finished (and
            // been appended by its drive loop) while we were loading it.
            if states[id]?.outcome != nil, !finishOrder.contains(id) { earlier.append(id) }
        }
        // A concurrent prune may have inserted the same sagas while we were
        // suspended; filter again at the insert, which has no suspension point.
        finishOrder.insert(contentsOf: earlier.filter { !finishOrder.contains($0) }, at: 0)
        let excess = max(0, finishOrder.count - keep)
        let victims = finishOrder.prefix(excess).filter { running[$0] == nil }
        guard !victims.isEmpty else { return 0 }
        try await removeLocked(victims)
        let removedSet = Set(victims)
        finishOrder.removeAll { removedSet.contains($0) }
        return victims.count
    }

    /// Simulates process death: stops every drive task and refuses all
    /// further writes. A new coordinator over the same journal recovers.
    public func retire() {
        retired = true
        for task in running.values { task.cancel() }
        running.removeAll()
    }

    public var isRetired: Bool { retired }

    // MARK: - Drive loop

    /// Returns the single drive task for `id`, creating it if needed. Runs
    /// synchronously on the actor (no suspension point between the lookup and
    /// the insert), so two concurrent starts cannot create two drive loops.
    private func driveTask(for id: SagaID, create key: String?) throws -> Task<SagaOutcome, Error> {
        try checkRetired()
        if let existing = running[id] { return existing }
        let task = Task { try await self.drive(id, create: key) }
        running[id] = task
        return task
    }

    private func drive(_ id: SagaID, create key: String?) async throws -> SagaOutcome {
        defer { if !retired { running[id] = nil } }
        try await lockedLoad(id, create: key)

        while true {
            try checkRetired()
            guard let state = states[id], let definition = definitions[state.definitionName] else {
                throw CoordinatorError.unknownSaga(id)
            }
            switch SagaPlanner.next(state) {
            case let .done(outcome), let .awaitingRevival(outcome):
                return outcome
            case let .finish(outcome):
                try await commit(id, .finished(outcome))
                finishOrder.append(id)
                // Hysteresis: prune in batches once the backlog exceeds the
                // retention target by a margin, not on every finish.
                if finishOrder.count > Saturating.add(retainFinished, pruneSlack) {
                    // Retention is housekeeping: a failure here must not turn a
                    // finished saga into a failed call.
                    _ = try? await pruneFinished(keepingLast: retainFinished)
                }
                return outcome
            case let .beginCompensation(reason):
                try await commit(id, .compensationStarted(reason: reason))
            case let .giveUp(event):
                try await commit(id, event)
            case let .execute(stepID):
                try await executeStep(stepID, of: id, definition: definition)
            case let .compensate(stepID):
                try await compensateStep(stepID, of: id, definition: definition)
            }
        }
    }

    private func executeStep(_ stepID: StepID, of id: SagaID, definition: SagaDefinition) async throws {
        guard let step = definition.step(stepID), let state = states[id] else { throw CoordinatorError.unknownSaga(id) }

        if (state.forwardBudgetUsed[stepID] ?? 0) >= policy.maxAttempts {
            // Budget already spent (e.g. the process died during the final attempt).
            let message = state.lastError[stepID] ?? "in doubt after final attempt"
            let permanent = state.pendingForward == nil && state.lastFailurePermanent[stepID] == true
            try await commit(id, SagaPlanner.onGiveUp(step: stepID, kind: step.kind, permanent: permanent, message: message))
            return
        }

        let attempt = Saturating.add(state.forwardAttempts[stepID] ?? 0, 1)
        try await commit(id, .stepIntent(step: stepID, attempt: attempt))

        let invocation = StepInvocation(sagaID: id, step: stepID,
                                        idempotencyKey: Self.idempotencyKey(id, stepID),
                                        attempt: attempt, outputs: state.outputs, forwardOutput: nil)
        // Only the step's own error is a step failure. A journal error while
        // recording the success must propagate instead: misreading it as a
        // failed step would journal a falsehood and spend retry budget.
        let output: String
        do {
            output = try await step.execute(invocation)
        } catch {
            try checkRetired()   // a retired coordinator records nothing
            let permanent = (error as? ClassifiedStepError)?.isPermanent ?? false
            let message = String(describing: error)
            try await commit(id, .stepFailed(step: stepID, attempt: attempt, message: message, permanent: permanent))
            let used = states[id]?.forwardBudgetUsed[stepID] ?? Int.max
            if permanent || used >= policy.maxAttempts {
                try await commit(id, SagaPlanner.onGiveUp(step: stepID, kind: step.kind, permanent: permanent, message: message))
            } else {
                try await sleeper.sleep(nanoseconds: policy.delay(afterAttempt: used, random: random))
            }
            return
        }
        try await commit(id, .stepSucceeded(step: stepID, output: output))
    }

    private func compensateStep(_ stepID: StepID, of id: SagaID, definition: SagaDefinition) async throws {
        guard let step = definition.step(stepID), let undo = step.compensate, let state = states[id] else {
            throw CoordinatorError.unknownSaga(id)
        }
        if (state.compensationBudgetUsed[stepID] ?? 0) >= policy.maxAttempts {
            let message = state.lastError[stepID] ?? "compensation in doubt after final attempt"
            try await commit(id, .parked(step: stepID, direction: .backward, message: message))
            return
        }
        let attempt = Saturating.add(state.compensationAttempts[stepID] ?? 0, 1)
        try await commit(id, .compensationIntent(step: stepID, attempt: attempt))
        let invocation = StepInvocation(sagaID: id, step: stepID,
                                        idempotencyKey: Self.idempotencyKey(id, stepID),
                                        attempt: attempt, outputs: state.outputs,
                                        forwardOutput: state.outputs[stepID])
        do {
            try await undo(invocation)
        } catch {
            try checkRetired()
            let message = String(describing: error)
            try await commit(id, .compensationFailed(step: stepID, attempt: attempt, message: message))
            let used = states[id]?.compensationBudgetUsed[stepID] ?? Int.max
            // A compensation that cannot complete is never "given up" into a
            // terminal state: it parks for an operator, whatever the error says.
            let permanent = (error as? ClassifiedStepError)?.isPermanent ?? false
            if permanent || used >= policy.maxAttempts {
                try await commit(id, .parked(step: stepID, direction: .backward, message: message))
            } else {
                try await sleeper.sleep(nanoseconds: policy.delay(afterAttempt: used, random: random))
            }
            return
        }
        try await commit(id, .compensationSucceeded(step: stepID))
    }

    /// Same key for every attempt of a step, across retries and crashes;
    /// compensation shares the forward step's key so it can undo "by key"
    /// when the forward output was never observed.
    public static func idempotencyKey(_ saga: SagaID, _ step: StepID) -> String {
        saga.rawValue + "#" + step.rawValue
    }

    // MARK: - Journal access (single writer per saga)

    private func commit(_ id: SagaID, _ event: JournalEvent) async throws {
        await acquire(id)
        defer { release(id) }
        try await commitLocked(id, event)
    }

    /// Caller holds the saga lock. Validates through the reducer *before* the
    /// durable write, and applies to memory only *after* it succeeds, so the
    /// in-memory state is never ahead of the journal.
    private func commitLocked(_ id: SagaID, _ event: JournalEvent) async throws {
        try checkRetired()
        guard var next = states[id] else { throw CoordinatorError.unknownSaga(id) }
        let record = JournalRecord(sagaID: id, seq: next.nextSeq, event: event)
        try next.apply(record)
        let epoch = try await currentEpoch()
        try await journal.append(record, epoch: epoch)
        states[id] = next
    }

    /// Caller holds the saga lock.
    private func loadLocked(_ id: SagaID, create key: String?) async throws {
        try checkRetired()
        // Fence first, then read: once our epoch is taken, a predecessor can no
        // longer append behind the history we are about to fold.
        _ = try await currentEpoch()
        if states[id] == nil {
            let records = try await journal.records(for: id)
            if records.isEmpty {
                guard let key else { throw CoordinatorError.unknownSaga(id) }
                let name = String(id.rawValue.prefix { $0 != "/" })
                guard let definition = definitions[name] else { throw CoordinatorError.unknownDefinition(name) }
                states[id] = SagaState(sagaID: id, definition: definition)
                do {
                    try await commitLocked(id, .started(definition: name, key: key))
                } catch {
                    states[id] = nil
                    throw error
                }
            } else {
                guard case let .started(name, _) = records[0].event else {
                    throw SagaStateError(seq: 0, reason: "first record must be started")
                }
                guard let definition = definitions[name] else { throw CoordinatorError.unknownDefinition(name) }
                states[id] = try SagaState.fold(records, sagaID: id, definition: definition)
            }
        }
    }

    private func currentEpoch() async throws -> Epoch {
        if let epochTask { return try await epochTask.value }
        let journal = self.journal
        let task = Task { try await journal.open() }
        epochTask = task
        do {
            return try await task.value
        } catch {
            // Let a later call retry opening instead of caching the failure.
            if epochTask == task { epochTask = nil }
            throw error
        }
    }

    private func checkRetired() throws {
        if retired { throw CoordinatorError.retired }
    }

    private func lockedLoad(_ id: SagaID, create key: String?) async throws {
        await acquire(id)
        defer { release(id) }
        try await loadLocked(id, create: key)
    }

    /// Returns the running drive task if the saga is already being driven.
    private func reviveLocked(_ id: SagaID) async throws -> Task<SagaOutcome, Error>? {
        await acquire(id)
        defer { release(id) }
        try await loadLocked(id, create: nil)
        guard states[id]?.parked != nil else {
            if let running = running[id] { return running }
            throw CoordinatorError.notParked(id)
        }
        try await commitLocked(id, .revived)
        return nil
    }

    /// Removes several sagas in one journal call (one file rewrite for
    /// `FileJournal`). Takes each saga's lock in a fixed (sorted) order, so two
    /// concurrent prunes cannot deadlock.
    private func removeLocked(_ ids: [SagaID]) async throws {
        // Deduplicated: acquiring the same saga's lock twice would self-deadlock.
        let ordered = Array(Set(ids)).sorted()
        for id in ordered { await acquire(id) }
        defer { for id in ordered { release(id) } }
        try checkRetired()
        let epoch = try await currentEpoch()
        try await journal.remove(ordered, epoch: epoch)
        for id in ordered { states[id] = nil }
    }

    private func acquire(_ id: SagaID) async {
        if !lockHeld.contains(id) {
            lockHeld.insert(id)
            return
        }
        await withCheckedContinuation { continuation in
            lockWaiters[id, default: []].append(continuation)
        }
        // Ownership was handed to us by `release`; `lockHeld` still contains id.
    }

    private func release(_ id: SagaID) {
        if var waiters = lockWaiters[id], !waiters.isEmpty {
            let next = waiters.removeFirst()
            lockWaiters[id] = waiters.isEmpty ? nil : waiters
            next.resume()
        } else {
            lockHeld.remove(id)
        }
    }
}
