import Foundation

/// Saturating integer arithmetic. Every counter and delay in this package goes
/// through these helpers so no input, however hostile, can trap on overflow.
public enum Saturating {
    public static func add(_ a: Int, _ b: Int) -> Int {
        let (r, overflow) = a.addingReportingOverflow(b)
        guard overflow else { return r }
        return b > 0 ? Int.max : Int.min
    }

    public static func add(_ a: UInt64, _ b: UInt64) -> UInt64 {
        let (r, overflow) = a.addingReportingOverflow(b)
        return overflow ? UInt64.max : r
    }

    static func multiply(_ a: UInt64, _ b: UInt64) -> UInt64 {
        let (r, overflow) = a.multipliedReportingOverflow(by: b)
        return overflow ? UInt64.max : r
    }

    /// `base * 2^exponent`, saturating at `UInt64.max`. Negative exponents are
    /// treated as 0.
    public static func shiftedLeft(_ base: UInt64, by exponent: Int) -> UInt64 {
        guard base > 0 else { return 0 }
        var value = base
        var remaining = max(0, exponent)
        while remaining > 0 {
            if value > UInt64.max / 2 { return UInt64.max }
            value *= 2
            remaining -= 1
        }
        return value
    }
}

/// Source of randomness for jitter, injectable so tests are deterministic.
public protocol RandomSource: Sendable {
    /// A value in `0...upperBound` (inclusive).
    func next(upTo upperBound: UInt64) -> UInt64
}

public struct SystemRandomSource: RandomSource {
    public init() {}
    public func next(upTo upperBound: UInt64) -> UInt64 {
        UInt64.random(in: 0...upperBound)
    }
}

/// Suspends between retries, injectable so tests do not wait in real time.
public protocol Sleeper: Sendable {
    func sleep(nanoseconds: UInt64) async throws
}

public struct TaskSleeper: Sleeper {
    public init() {}
    public func sleep(nanoseconds: UInt64) async throws {
        try await Task.sleep(nanoseconds: nanoseconds)
    }
}

/// Bounded retries with full-jitter exponential backoff.
///
/// Full jitter (`random(0...min(cap, base·2^n))`) rather than equal jitter or
/// no jitter: when a dependency recovers, thousands of devices that failed at
/// the same moment should not retry at the same moment.
public struct RetryPolicy: Sendable, Equatable {
    /// Attempts per step, including the first. Clamped to at least 1.
    public let maxAttempts: Int
    public let baseDelayNanoseconds: UInt64
    public let maxDelayNanoseconds: UInt64

    public init(maxAttempts: Int, baseDelayNanoseconds: UInt64, maxDelayNanoseconds: UInt64) {
        self.maxAttempts = max(1, maxAttempts)
        self.baseDelayNanoseconds = baseDelayNanoseconds
        self.maxDelayNanoseconds = max(baseDelayNanoseconds, maxDelayNanoseconds)
    }

    public static let `default` = RetryPolicy(maxAttempts: 4,
                                              baseDelayNanoseconds: 200_000_000,
                                              maxDelayNanoseconds: 5_000_000_000)

    /// The exponential ceiling before jitter for the retry that follows
    /// `failedAttempt` (1-based).
    public func ceiling(afterAttempt failedAttempt: Int) -> UInt64 {
        // `failedAttempt - 1` would trap for Int.min.
        let exponent = max(0, Saturating.add(failedAttempt, -1))
        return min(maxDelayNanoseconds, Saturating.shiftedLeft(baseDelayNanoseconds, by: exponent))
    }

    public func delay(afterAttempt failedAttempt: Int, random: RandomSource) -> UInt64 {
        let cap = ceiling(afterAttempt: failedAttempt)
        // A misbehaving RandomSource must not push us past the cap.
        return min(cap, random.next(upTo: cap))
    }

    func hasAttemptsLeft(after attempt: Int) -> Bool {
        attempt < maxAttempts
    }
}

/// How a step's error should be treated. Errors that do not conform to
/// `ClassifiedStepError` are treated as transient: an unknown error may be a
/// timeout, and a timeout is ambiguous, so retrying with the same idempotency
/// key is the only safe move.
public protocol ClassifiedStepError: Error {
    /// `true` when the server definitively rejected the request (validation,
    /// card declined). Retrying cannot help.
    var isPermanent: Bool { get }
}

public struct StepFailure: ClassifiedStepError, Equatable, CustomStringConvertible {
    public let message: String
    public let isPermanent: Bool

    public init(_ message: String, permanent: Bool) {
        self.message = message
        self.isPermanent = permanent
    }

    public static func transient(_ message: String) -> StepFailure { StepFailure(message, permanent: false) }
    public static func permanent(_ message: String) -> StepFailure { StepFailure(message, permanent: true) }

    public var description: String { message }
}
