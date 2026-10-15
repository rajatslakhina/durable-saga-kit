import Foundation
@testable import DurableSaga

/// A server that deduplicates on idempotency key, like a well-behaved payment
/// or inventory API. `applied` is the ground truth the tests assert against.
actor FakeServer {
    private(set) var applied: [String: String] = [:]      // key -> output
    private(set) var applyCount: [String: Int] = [:]      // effects actually applied per key
    private(set) var calls: [StepID: Int] = [:]           // requests received per step
    private(set) var undone: Set<String> = []
    private(set) var undoCalls: [StepID: Int] = [:]
    private var scriptedFailures: [StepID: [StepFailure]] = [:]
    private var scriptedUndoFailures: [StepID: [StepFailure]] = [:]
    /// Failures thrown *after* the effect was applied (lost response / timeout).
    private var failAfterApply: [StepID: Int] = [:]
    private var hooks: [StepID: @Sendable () async -> Void] = [:]

    func script(_ step: StepID, failures: [StepFailure]) { scriptedFailures[step] = failures }
    func scriptUndo(_ step: StepID, failures: [StepFailure]) { scriptedUndoFailures[step] = failures }
    func loseResponse(_ step: StepID, times: Int) { failAfterApply[step] = times }
    /// Runs after the effect is applied and before the response returns.
    func afterApply(_ step: StepID, _ hook: @escaping @Sendable () async -> Void) { hooks[step] = hook }

    func perform(_ inv: StepInvocation, key: String? = nil) async throws -> String {
        let key = key ?? inv.idempotencyKey
        calls[inv.step, default: 0] += 1
        if var queue = scriptedFailures[inv.step], !queue.isEmpty {
            let failure = queue.removeFirst()
            scriptedFailures[inv.step] = queue
            throw failure
        }
        let output: String
        if let existing = applied[key], !undone.contains(key) {
            output = existing                       // dedupe: same key, same answer
        } else {
            output = "\(inv.step.rawValue)-ok"
            applied[key] = output
            undone.remove(key)
            applyCount[key, default: 0] += 1
            if let hook = hooks[inv.step] {
                hooks[inv.step] = nil
                await hook()
            }
        }
        if let remaining = failAfterApply[inv.step], remaining > 0 {
            failAfterApply[inv.step] = remaining - 1
            throw StepFailure.transient("timeout after apply")
        }
        return output
    }

    func undo(_ inv: StepInvocation) throws {
        undoCalls[inv.step, default: 0] += 1
        if var queue = scriptedUndoFailures[inv.step], !queue.isEmpty {
            let failure = queue.removeFirst()
            scriptedUndoFailures[inv.step] = queue
            throw failure
        }
        // Undo by key; "nothing to undo" is success.
        if applied[inv.idempotencyKey] != nil { undone.insert(inv.idempotencyKey) }
    }

    /// Effects currently in force (applied and not undone), by step name prefix.
    func liveEffects() -> Set<String> {
        Set(applied.keys.filter { !undone.contains($0) })
    }

    func totalApplied() -> Int { applyCount.values.reduce(0, +) }
}

/// A one-shot gate a step can wait on, so tests control interleavings
/// deterministically instead of sleeping.
actor Gate {
    private var open = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private(set) var arrivals = 0

    func wait() async {
        arrivals += 1
        if open { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func release() {
        open = true
        let pending = waiters
        waiters.removeAll()
        for waiter in pending { waiter.resume() }
    }

    func waitForArrivals(_ count: Int) async {
        while arrivals < count { await Task.yield() }
    }
}

final class RecordingSleeper: Sleeper, @unchecked Sendable {
    private let lock = NSLock()
    private var _delays: [UInt64] = []
    var delays: [UInt64] { lock.lock(); defer { lock.unlock() }; return _delays }
    private func record(_ value: UInt64) {
        lock.lock(); _delays.append(value); lock.unlock()
    }
    func sleep(nanoseconds: UInt64) async throws {
        record(nanoseconds)
        try Task.checkCancellation()
    }
}

/// A sleeper whose first sleep blocks on a gate, to hold a saga in its backoff window.
final class GatedSleeper: Sleeper, @unchecked Sendable {
    let gate = Gate()
    private let lock = NSLock()
    private var first = true
    private func takeFirst() -> Bool {
        lock.lock(); defer { lock.unlock() }
        let wasFirst = first
        first = false
        return wasFirst
    }
    func sleep(nanoseconds: UInt64) async throws {
        if takeFirst() { await gate.wait() }
    }
}

struct FixedRandom: RandomSource {
    let value: UInt64
    func next(upTo upperBound: UInt64) -> UInt64 { value }
}

/// Holds a reference set after construction (to let a step retire "its" coordinator).
final class Box<T: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var _value: T?
    var value: T? {
        get { lock.lock(); defer { lock.unlock() }; return _value }
        set { lock.lock(); _value = newValue; lock.unlock() }
    }
}

/// Journal wrapper whose appends can be held at a gate, to force a concurrent
/// writer into the window between "seq chosen" and "append done".
actor GatedJournal: SagaJournal {
    let inner = InMemoryJournal()
    private var gate: Gate?
    private(set) var appendsStarted = 0

    func holdNextAppend(at gate: Gate) { self.gate = gate }

    func open() async throws -> Epoch { try await inner.open() }
    func append(_ record: JournalRecord, epoch: Epoch) async throws {
        appendsStarted += 1
        if let gate {
            self.gate = nil
            await gate.wait()
        }
        try await inner.append(record, epoch: epoch)
    }
    func records(for saga: SagaID) async throws -> [JournalRecord] { try await inner.records(for: saga) }
    func sagaIDs() async throws -> [SagaID] { try await inner.sagaIDs() }
    func remove(_ sagas: [SagaID], epoch: Epoch) async throws { try await inner.remove(sagas, epoch: epoch) }
}

enum Fixtures {
    static let fastPolicy = RetryPolicy(maxAttempts: 3, baseDelayNanoseconds: 1_000, maxDelayNanoseconds: 4_000)

    /// reserve (C) → authorize (C) → place (P) → receipt (R)
    static func checkout(server: FakeServer, gate: Gate? = nil, gateStep: StepID? = nil) throws -> SagaDefinition {
        @Sendable func run(_ inv: StepInvocation) async throws -> String {
            if let gate, inv.step == gateStep { await gate.wait() }
            return try await server.perform(inv)
        }
        return try SagaDefinition(name: "checkout", steps: [
            .compensatable("reserve", execute: run, compensate: { try await server.undo($0) }),
            .compensatable("authorize", execute: run, compensate: { try await server.undo($0) }),
            .pivot("place", execute: run),
            .retriable("receipt", execute: run)
        ])
    }

    static func key(_ saga: String, _ step: String) -> String {
        SagaCoordinator.idempotencyKey(SagaID(definition: "checkout", key: saga), StepID(step))
    }
}
