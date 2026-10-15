import XCTest
@testable import DurableSaga

final class DefinitionTests: XCTestCase {
    private let ok: StepAction = { _ in "ok" }
    private let undo: CompensationAction = { _ in }

    func testValidShapesAreAccepted() throws {
        _ = try SagaDefinition(name: "a", steps: [.compensatable("c", execute: ok, compensate: undo)])
        _ = try SagaDefinition(name: "b", steps: [.pivot("p", execute: ok)])
        let full = try SagaDefinition(name: "c", steps: [
            .compensatable("c1", execute: ok, compensate: undo),
            .compensatable("c2", execute: ok, compensate: undo),
            .pivot("p", execute: ok),
            .retriable("r1", execute: ok),
            .retriable("r2", execute: ok)
        ])
        XCTAssertEqual(full.pivotIndex, 2)
        XCTAssertEqual(full.pivotID, "p")
        XCTAssertEqual(full.index(of: "r2"), 4)
        XCTAssertNil(full.step(at: 5))
        XCTAssertNil(full.step(at: -1))
        XCTAssertNil(full.step("missing"))
    }

    func testInvalidShapesAreRejected() {
        func rejects(_ steps: [SagaStep], name: String = "x", _ expected: DefinitionError,
                     file: StaticString = #filePath, line: UInt = #line) {
            XCTAssertThrowsError(try SagaDefinition(name: name, steps: steps), file: file, line: line) {
                XCTAssertEqual($0 as? DefinitionError, expected, file: file, line: line)
            }
        }
        rejects([], .noSteps)
        rejects([.pivot("p", execute: ok)], name: "", .emptyName)
        rejects([.pivot("p", execute: ok)], name: "a/b", .nameContainsSeparator)
        rejects([.pivot("p", execute: ok), .pivot("p", execute: ok)], .duplicateStep("p"))
        rejects([.pivot("p", execute: ok), .pivot("q", execute: ok)], .multiplePivots)
        rejects([.pivot("p", execute: ok), .compensatable("c", execute: ok, compensate: undo)], .outOfOrder("c"))
        rejects([.pivot("p", execute: ok), .retriable("r", execute: ok),
                 .compensatable("c", execute: ok, compensate: undo)], .outOfOrder("c"))
        rejects([.compensatable("c", execute: ok, compensate: undo), .retriable("r", execute: ok)], .retriableWithoutPivot)
    }
}

final class SaturatingAndRetryTests: XCTestCase {
    func testSaturatingArithmeticNeverTraps() {
        XCTAssertEqual(Saturating.add(Int.max, 1), Int.max)
        XCTAssertEqual(Saturating.add(Int.min, -1), Int.min)
        XCTAssertEqual(Saturating.add(2, 3), 5)
        XCTAssertEqual(Saturating.add(UInt64.max, 1), UInt64.max)
        XCTAssertEqual(Saturating.multiply(UInt64.max, 2), UInt64.max)
        XCTAssertEqual(Saturating.shiftedLeft(1, by: 10), 1024)
        XCTAssertEqual(Saturating.shiftedLeft(1, by: Int.max), UInt64.max)
        XCTAssertEqual(Saturating.shiftedLeft(3, by: -5), 3)
        XCTAssertEqual(Saturating.shiftedLeft(0, by: Int.max), 0)
    }

    func testRetryPolicyClampsAndCaps() {
        let policy = RetryPolicy(maxAttempts: 0, baseDelayNanoseconds: 100, maxDelayNanoseconds: 10)
        XCTAssertEqual(policy.maxAttempts, 1, "zero attempts would mean a step never runs")
        XCTAssertEqual(policy.maxDelayNanoseconds, 100, "cap is never below base")
        XCTAssertFalse(policy.hasAttemptsLeft(after: 1))

        let p = RetryPolicy(maxAttempts: 5, baseDelayNanoseconds: 100, maxDelayNanoseconds: 1_000)
        XCTAssertEqual(p.ceiling(afterAttempt: 1), 100)
        XCTAssertEqual(p.ceiling(afterAttempt: 2), 200)
        XCTAssertEqual(p.ceiling(afterAttempt: 4), 800)
        XCTAssertEqual(p.ceiling(afterAttempt: 5), 1_000)
        XCTAssertEqual(p.ceiling(afterAttempt: Int.max), 1_000, "huge attempt counts must not trap")
        XCTAssertEqual(p.ceiling(afterAttempt: Int.min), 100)
        // A RandomSource that ignores its bound cannot push the delay past the cap.
        XCTAssertEqual(p.delay(afterAttempt: 2, random: FixedRandom(value: .max)), 200)
        XCTAssertEqual(p.delay(afterAttempt: 2, random: FixedRandom(value: 7)), 7)
    }
}

final class CodecTests: XCTestCase {
    private let id = SagaID(definition: "checkout", key: "k1")

    private func sample() -> [JournalRecord] {
        [JournalRecord(sagaID: id, seq: 0, event: .started(definition: "checkout", key: "k1")),
         JournalRecord(sagaID: id, seq: 1, event: .stepIntent(step: "reserve", attempt: 1)),
         JournalRecord(sagaID: id, seq: 2, event: .stepSucceeded(step: "reserve", output: "line\nbreak \"quoted\""))]
    }

    func testCRC32MatchesTheStandardCheckValue() {
        // The published check value for CRC-32/ISO-HDLC. A table or polynomial
        // bug changes it, so this is a real test of the implementation.
        XCTAssertEqual(CRC32.checksum(Array("123456789".utf8)), 0xCBF4_3926)
        XCTAssertEqual(CRC32.checksum([UInt8]()), 0)
    }

    func testRoundTripPreservesRecordsIncludingNewlinesInPayload() throws {
        var data = Data()
        for record in sample() { data.append(try JournalLineCodec.encode(record)) }
        let decoded = try JournalLineCodec.decode(data)
        XCTAssertEqual(decoded.records, sample())
        XCTAssertEqual(decoded.discardedTailBytes, 0)
    }

    func testTornTailIsDiscardedNotFatal() throws {
        var data = Data()
        for record in sample() { data.append(try JournalLineCodec.encode(record)) }
        let full = data.count
        let torn = data.prefix(full - 7)            // crash mid-append of the last record
        let decoded = try JournalLineCodec.decode(Data(torn))
        XCTAssertEqual(decoded.records, Array(sample().prefix(2)))
        XCTAssertGreaterThan(decoded.discardedTailBytes, 0)
    }

    func testBadChecksumOnFinalTerminatedLineIsTreatedAsTorn() throws {
        var lines = try sample().map { try JournalLineCodec.encode($0) }
        var last = [UInt8](lines[2])
        last[20] = last[20] == UInt8(ascii: "a") ? UInt8(ascii: "b") : UInt8(ascii: "a")
        lines[2] = Data(last)
        let decoded = try JournalLineCodec.decode(lines.reduce(Data(), +))
        XCTAssertEqual(decoded.records.count, 2)
    }

    func testCorruptionInTheMiddleIsReportedNotSkipped() throws {
        var lines = try sample().map { try JournalLineCodec.encode($0) }
        var middle = [UInt8](lines[1])
        middle[0] = UInt8(ascii: "z")                // checksum field no longer hex
        lines[1] = Data(middle)
        XCTAssertThrowsError(try JournalLineCodec.decode(lines.reduce(Data(), +))) {
            XCTAssertEqual($0 as? JournalError, .corrupt(line: 2))
        }
    }

    func testEmptyAndTinyInputs() throws {
        XCTAssertEqual(try JournalLineCodec.decode(Data()).records, [])
        XCTAssertEqual(try JournalLineCodec.decode(Data("x".utf8)).discardedTailBytes, 1)
        XCTAssertEqual(try JournalLineCodec.decode(Data("\n".utf8)).discardedTailBytes, 1)
    }
}
