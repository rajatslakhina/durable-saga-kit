import Foundation

/// Identifies one step inside a saga definition. Stable across app versions:
/// it is written to the journal, so renaming a step is a schema change.
public struct StepID: Hashable, Sendable, Codable, CustomStringConvertible, ExpressibleByStringLiteral {
    public let rawValue: String

    public init(_ rawValue: String) { self.rawValue = rawValue }
    public init(stringLiteral value: String) { self.rawValue = value }

    public init(from decoder: Decoder) throws {
        rawValue = try decoder.singleValueContainer().decode(String.self)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    public var description: String { rawValue }
}

/// Identifies one saga instance. Derived deterministically from the
/// definition name and the caller's idempotency key, so starting the same
/// intent twice (double tap, retry after relaunch) attaches to one instance
/// instead of creating a second one.
public struct SagaID: Hashable, Sendable, Codable, Comparable, CustomStringConvertible {
    public let rawValue: String

    public init(definition: String, key: String) {
        self.rawValue = definition + "/" + key
    }

    public init(rawValue: String) { self.rawValue = rawValue }

    public init(from decoder: Decoder) throws {
        rawValue = try decoder.singleValueContainer().decode(String.self)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    public static func < (lhs: SagaID, rhs: SagaID) -> Bool { lhs.rawValue < rhs.rawValue }

    public var description: String { rawValue }
}

/// A fencing token. Every coordinator that opens a journal gets a strictly
/// larger epoch, and the journal rejects appends from any older one, so a
/// coordinator that should be dead (the pre-crash instance, a zombie task)
/// cannot write after its successor has taken over.
public struct Epoch: Hashable, Sendable, Codable, Comparable, CustomStringConvertible {
    public let value: Int

    public init(_ value: Int) { self.value = value }

    public static func < (lhs: Epoch, rhs: Epoch) -> Bool { lhs.value < rhs.value }

    public var description: String { "epoch \(value)" }
}
