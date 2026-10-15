import Foundation

/// The three step kinds of the classic saga model (Garcia-Molina & Salem, 1987).
///
/// A valid definition is `compensatable* pivot? retriable*`:
/// - **compensatable** steps can be undone, so a failure before the pivot rolls
///   the saga back;
/// - the **pivot** is the point of no return (e.g. "place order"). It cannot be
///   undone; once it succeeds the saga is *committed*;
/// - **retriable** steps run after the pivot and must eventually succeed. They
///   are retried, and parked for an operator when retries run out, but never
///   compensated, because rolling back past a pivot is impossible.
public enum StepKind: String, Sendable, Codable {
    case compensatable
    case pivot
    case retriable
}

/// Everything a step needs to perform its side effect safely.
public struct StepInvocation: Sendable, Equatable {
    public let sagaID: SagaID
    public let step: StepID
    /// Deterministic per (saga, step): **not** per attempt. A retry after a
    /// timeout, or a re-execution after a crash, presents the same key, so a
    /// server that deduplicates on it applies the effect at most once.
    public let idempotencyKey: String
    /// 1-based attempt number, counted across crashes (it comes from the journal).
    public let attempt: Int
    /// Outputs of earlier steps that have succeeded, keyed by step.
    public let outputs: [StepID: String]
    /// For a compensation: the output of the forward step being undone, if it
    /// was ever observed. `nil` means the forward step was attempted but its
    /// result is unknown (timeout, crash), and the compensation must undo
    /// *by idempotency key*.
    public let forwardOutput: String?

    public init(sagaID: SagaID, step: StepID, idempotencyKey: String, attempt: Int,
                outputs: [StepID: String], forwardOutput: String?) {
        self.sagaID = sagaID
        self.step = step
        self.idempotencyKey = idempotencyKey
        self.attempt = attempt
        self.outputs = outputs
        self.forwardOutput = forwardOutput
    }
}

public typealias StepAction = @Sendable (StepInvocation) async throws -> String
public typealias CompensationAction = @Sendable (StepInvocation) async throws -> Void

/// One step of a saga.
public struct SagaStep: Sendable {
    public let id: StepID
    public let kind: StepKind
    let execute: StepAction
    let compensate: CompensationAction?

    /// A step that can be undone. The compensation must be idempotent and must
    /// treat "nothing to undo" as success: it runs for every step that was
    /// *attempted*, not only for steps observed to succeed.
    public static func compensatable(_ id: StepID,
                                     execute: @escaping StepAction,
                                     compensate: @escaping CompensationAction) -> SagaStep {
        SagaStep(id: id, kind: .compensatable, execute: execute, compensate: compensate)
    }

    /// The point of no return.
    public static func pivot(_ id: StepID, execute: @escaping StepAction) -> SagaStep {
        SagaStep(id: id, kind: .pivot, execute: execute, compensate: nil)
    }

    /// A must-finish step after the pivot.
    public static func retriable(_ id: StepID, execute: @escaping StepAction) -> SagaStep {
        SagaStep(id: id, kind: .retriable, execute: execute, compensate: nil)
    }
}

public enum DefinitionError: Error, Equatable, Sendable {
    case emptyName
    case nameContainsSeparator
    case noSteps
    case duplicateStep(StepID)
    case multiplePivots
    /// The kinds are not in `compensatable* pivot? retriable*` order.
    case outOfOrder(StepID)
    /// Retriable steps exist without a pivot, so there is no well-defined
    /// commit point separating "roll back" from "push forward".
    case retriableWithoutPivot
}

/// A validated saga definition. Construction fails rather than producing a
/// definition whose recovery semantics would be ambiguous.
public struct SagaDefinition: Sendable {
    public let name: String
    public let steps: [SagaStep]
    /// Index of the pivot, if any.
    public let pivotIndex: Int?
    private let indexByID: [StepID: Int]

    public init(name: String, steps: [SagaStep]) throws {
        guard !name.isEmpty else { throw DefinitionError.emptyName }
        guard !name.contains("/") else { throw DefinitionError.nameContainsSeparator }
        guard !steps.isEmpty else { throw DefinitionError.noSteps }

        var index: [StepID: Int] = [:]
        var pivot: Int?
        var highestRank = 0
        for (position, step) in steps.enumerated() {
            guard index[step.id] == nil else { throw DefinitionError.duplicateStep(step.id) }
            index[step.id] = position
            let rank: Int
            switch step.kind {
            case .compensatable: rank = 0
            case .pivot:
                guard pivot == nil else { throw DefinitionError.multiplePivots }
                pivot = position
                rank = 1
            case .retriable: rank = 2
            }
            // Ranks must be non-decreasing: C* P? R*.
            guard rank >= highestRank else { throw DefinitionError.outOfOrder(step.id) }
            highestRank = rank
        }
        if highestRank == 2 && pivot == nil { throw DefinitionError.retriableWithoutPivot }

        self.name = name
        self.steps = steps
        self.pivotIndex = pivot
        self.indexByID = index
    }

    func index(of step: StepID) -> Int? { indexByID[step] }

    public func step(at index: Int) -> SagaStep? {
        steps.indices.contains(index) ? steps[index] : nil
    }

    public func step(_ id: StepID) -> SagaStep? {
        guard let i = indexByID[id] else { return nil }
        return step(at: i)
    }

    /// The pivot's id, if the definition has one.
    public var pivotID: StepID? {
        guard let p = pivotIndex else { return nil }
        return step(at: p)?.id
    }
}
