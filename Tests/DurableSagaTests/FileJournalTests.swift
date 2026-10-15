import XCTest
@testable import DurableSaga

final class FileJournalTests: XCTestCase {
    private var directory: URL!

    override func setUpWithError() throws {
        directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("durable-saga-tests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        try? FileManager.default.removeItem(at: directory)
    }

    private var logURL: URL { directory.appendingPathComponent("saga-journal.log") }
    private let id = SagaID(definition: "checkout", key: "f1")

    private func coordinator(_ journal: SagaJournal, _ server: FakeServer) throws -> SagaCoordinator {
        try SagaCoordinator(journal: journal, definitions: [try Fixtures.checkout(server: server)],
                            retryPolicy: Fixtures.fastPolicy, sleeper: RecordingSleeper(), random: FixedRandom(value: 0))
    }

    func testRecordsSurviveReopen() async throws {
        let server = FakeServer()
        let first = FileJournal(directory: directory)
        _ = try await coordinator(first, server).start("checkout", key: "f1")
        let written = try await first.records(for: id)

        let reopened = FileJournal(directory: directory)
        _ = try await reopened.open()
        let read = try await reopened.records(for: id)
        XCTAssertEqual(read, written)
        XCTAssertFalse(read.isEmpty)
    }

    func testTornTailIsRepairedOnOpen() async throws {
        let server = FakeServer()
        let first = FileJournal(directory: directory)
        _ = try await coordinator(first, server).start("checkout", key: "f1")
        let intact = try await first.records(for: id)

        let handle = try FileHandle(forWritingTo: logURL)
        try handle.seekToEnd()
        try handle.write(contentsOf: Data("0badc0de {\"sagaID\":\"checkout/f1\",\"se".utf8))   // crash mid-append
        try handle.close()

        let reopened = FileJournal(directory: directory)
        _ = try await reopened.open()
        let discarded = await reopened.lastDiscardedTailBytes
        XCTAssertGreaterThan(discarded, 0)
        let read = try await reopened.records(for: id)
        XCTAssertEqual(read, intact)
        let onDisk = try JournalLineCodec.decode(try Data(contentsOf: logURL))
        XCTAssertEqual(onDisk.discardedTailBytes, 0, "open() must physically drop the torn tail before appending after it")
    }

    func testSecondInstanceFencesTheFirstWithoutSharedMemory() async throws {
        let a = FileJournal(directory: directory)
        let b = FileJournal(directory: directory)
        let ea = try await a.open()
        let eb = try await b.open()
        XCTAssertLessThan(ea, eb)
        let record = JournalRecord(sagaID: id, seq: 0, event: .started(definition: "checkout", key: "f1"))
        do { try await a.append(record, epoch: ea); XCTFail("stale instance appended") } catch {
            XCTAssertEqual(error as? JournalError, .fenced(current: eb, attempted: ea))
        }
        try await b.append(record, epoch: eb)
        let read = try await b.records(for: id)
        XCTAssertEqual(read, [record])
    }

    func testEndToEndCrashRecoveryAcrossJournalInstances() async throws {
        let server = FakeServer()
        let firstJournal = FileJournal(directory: directory)
        let first = try coordinator(firstJournal, server)
        let box = Box<SagaCoordinator>()
        box.value = first
        await server.afterApply("place") { await box.value?.retire() }
        do { _ = try await first.start("checkout", key: "f1"); XCTFail() } catch {}

        let relaunched = try coordinator(FileJournal(directory: directory), server)
        let report = try await relaunched.resumeAll()
        guard case .completed = report.outcomes[id] else { return XCTFail("\(report)") }
        let placed = await server.applyCount[Fixtures.key("f1", "place")]
        XCTAssertEqual(placed, 1)
    }

    func testRemoveRewritesTheLogWithoutTheSaga() async throws {
        let server = FakeServer()
        let journal = FileJournal(directory: directory)
        let c = try coordinator(journal, server)
        _ = try await c.start("checkout", key: "f1")
        _ = try await c.start("checkout", key: "f2")
        let removed = try await c.pruneFinished(keepingLast: 1)
        XCTAssertEqual(removed, 1)

        let reopened = FileJournal(directory: directory)
        _ = try await reopened.open()
        let ids = try await reopened.sagaIDs()
        XCTAssertEqual(ids, [SagaID(definition: "checkout", key: "f2")])
    }
}
