import Foundation

/// CRC-32 (IEEE 802.3, reflected, polynomial 0xEDB88320). Hand-rolled so the
/// package has no dependency beyond Foundation and behaves identically on
/// Apple platforms and Linux.
public enum CRC32 {
    private static let table: [UInt32] = (0..<256).map { index -> UInt32 in
        var c = UInt32(index)
        for _ in 0..<8 {
            c = (c & 1) != 0 ? (0xEDB8_8320 ^ (c >> 1)) : (c >> 1)
        }
        return c
    }

    public static func checksum<S: Sequence>(_ bytes: S) -> UInt32 where S.Element == UInt8 {
        var crc: UInt32 = 0xFFFF_FFFF
        for byte in bytes {
            let slot = Int((crc ^ UInt32(byte)) & 0xFF)   // always 0...255: in bounds
            crc = table[slot] ^ (crc >> 8)
        }
        return crc ^ 0xFFFF_FFFF
    }
}

/// Line format for the file journal: `<crc32 as 8 hex digits> <json>\n`.
///
/// Recovery rule, the one a staff engineer has to be able to defend:
/// - a **final** line that is unterminated or fails its checksum is a torn
///   write from a crash mid-append. It is discarded: its intent never became
///   durable, so its side effect never started (write-ahead).
/// - a bad line **anywhere else** is corruption, not a crash artefact. The
///   decoder refuses to guess and reports it.
public enum JournalLineCodec {
    public struct Decoded: Equatable, Sendable {
        public var records: [JournalRecord]
        /// Bytes dropped from the end of the log as a torn write.
        public var discardedTailBytes: Int
    }

    private static func encoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return encoder
    }

    public static func encode(_ record: JournalRecord) throws -> Data {
        let json = try encoder().encode(record)
        let crc = String(format: "%08x", CRC32.checksum(json))
        var line = Data(crc.utf8)
        line.append(UInt8(ascii: " "))
        line.append(json)
        line.append(UInt8(ascii: "\n"))
        return line
    }

    public static func decode(_ data: Data) throws -> Decoded {
        let bytes = [UInt8](data)
        var records: [JournalRecord] = []
        var lineStart = 0
        var lineNumber = 0
        let decoder = JSONDecoder()

        while lineStart < bytes.count {
            lineNumber += 1
            guard let newline = bytes[lineStart...].firstIndex(of: UInt8(ascii: "\n")) else {
                // Unterminated final line: torn write.
                return Decoded(records: records, discardedTailBytes: bytes.count - lineStart)
            }
            let line = bytes[lineStart..<newline]
            let isLast = newline == bytes.count - 1
            if let record = parse(line, decoder: decoder) {
                records.append(record)
            } else if isLast {
                return Decoded(records: records, discardedTailBytes: bytes.count - lineStart)
            } else {
                throw JournalError.corrupt(line: lineNumber)
            }
            lineStart = newline + 1
        }
        return Decoded(records: records, discardedTailBytes: 0)
    }

    private static func parse(_ line: ArraySlice<UInt8>, decoder: JSONDecoder) -> JournalRecord? {
        // 8 hex digits, one space, then at least "{}".
        guard line.count >= 11 else { return nil }
        let start = line.startIndex
        let hexEnd = start + 8
        guard line[hexEnd] == UInt8(ascii: " "),
              let hex = String(bytes: line[start..<hexEnd], encoding: .ascii),
              let expected = UInt32(hex, radix: 16) else { return nil }
        let json = line[(hexEnd + 1)...]
        guard CRC32.checksum(json) == expected else { return nil }
        return try? decoder.decode(JournalRecord.self, from: Data(json))
    }
}

/// A durable journal: one append-only log file plus an epoch file.
///
/// - `open()` drops a torn tail by atomically rewriting the log, then bumps
///   the epoch on disk. Each `append` re-reads the epoch file, so a second
///   `FileJournal` over the same directory (a relaunched process) fences this
///   one even though they share no memory.
/// - `remove` (retention) rewrites the log atomically without that saga.
///
/// Throughput is not the goal: a client runs a handful of sagas, and every
/// append is fsynced because the whole design rests on "intent is durable
/// before the effect starts".
public actor FileJournal: SagaJournal {
    private let logURL: URL
    private let epochURL: URL
    private var cache: [SagaID: [JournalRecord]]?
    private var heldEpoch: Epoch?
    public private(set) var lastDiscardedTailBytes = 0

    public init(directory: URL) {
        self.logURL = directory.appendingPathComponent("saga-journal.log")
        self.epochURL = directory.appendingPathComponent("saga-journal.epoch")
    }

    public func open() async throws -> Epoch {
        // Take the epoch *before* reading, so an older instance can no longer
        // append between our read (and tail repair) and our first write.
        let next = Epoch(Saturating.add(try readDiskEpoch().value, 1))
        try writeAtomically(Data(String(next.value).utf8), to: epochURL)
        heldEpoch = next
        let records = try loadFromDisk(repairTail: true)
        cache = Dictionary(grouping: records, by: \.sagaID)
        return next
    }

    public func append(_ record: JournalRecord, epoch: Epoch) async throws {
        let onDisk = try readDiskEpoch()
        guard epoch == onDisk, epoch == heldEpoch else {
            throw JournalError.fenced(current: onDisk, attempted: epoch)
        }
        var all = try cached()
        let expected = all[record.sagaID]?.count ?? 0
        guard record.seq == expected else {
            throw JournalError.sequenceConflict(saga: record.sagaID, expected: expected, got: record.seq)
        }
        let line = try JournalLineCodec.encode(record)
        do {
            if !FileManager.default.fileExists(atPath: logURL.path) {
                guard FileManager.default.createFile(atPath: logURL.path, contents: nil) else {
                    throw JournalError.io("cannot create \(logURL.lastPathComponent)")
                }
            }
            let handle = try FileHandle(forWritingTo: logURL)
            defer { try? handle.close() }
            let offset = try handle.seekToEnd()
            do {
                try handle.write(contentsOf: line)
                try handle.synchronize()
            } catch {
                // A partial write (disk full) must not stay in the file: the
                // next append would land after it and turn a torn tail into
                // mid-log corruption that blocks every saga on the next open.
                try? handle.truncate(atOffset: offset)
                try? handle.synchronize()
                throw error
            }
        } catch let error as JournalError {
            throw error
        } catch {
            throw JournalError.io(String(describing: error))
        }
        all[record.sagaID, default: []].append(record)
        cache = all
    }

    public func records(for saga: SagaID) async throws -> [JournalRecord] {
        try cached()[saga] ?? []
    }

    public func sagaIDs() async throws -> [SagaID] {
        try cached().keys.sorted()
    }

    public func remove(_ sagas: [SagaID], epoch: Epoch) async throws {
        let onDisk = try readDiskEpoch()
        guard epoch == onDisk, epoch == heldEpoch else {
            throw JournalError.fenced(current: onDisk, attempted: epoch)
        }
        var all = try cached()
        for saga in sagas { all[saga] = nil }
        var data = Data()
        for id in all.keys.sorted() {
            for record in all[id] ?? [] { data.append(try JournalLineCodec.encode(record)) }
        }
        try writeAtomically(data, to: logURL)
        cache = all
    }

    // MARK: - Private

    private func cached() throws -> [SagaID: [JournalRecord]] {
        if let cache { return cache }
        let loaded = Dictionary(grouping: try loadFromDisk(repairTail: false), by: \.sagaID)
        cache = loaded
        return loaded
    }

    private func loadFromDisk(repairTail: Bool) throws -> [JournalRecord] {
        guard FileManager.default.fileExists(atPath: logURL.path) else { return [] }
        let data: Data
        do { data = try Data(contentsOf: logURL) } catch { throw JournalError.io(String(describing: error)) }
        let decoded = try JournalLineCodec.decode(data)
        lastDiscardedTailBytes = decoded.discardedTailBytes
        if repairTail && decoded.discardedTailBytes > 0 {
            var clean = Data()
            for record in decoded.records { clean.append(try JournalLineCodec.encode(record)) }
            try writeAtomically(clean, to: logURL)
        }
        return decoded.records
    }

    private func readDiskEpoch() throws -> Epoch {
        guard FileManager.default.fileExists(atPath: epochURL.path) else { return Epoch(0) }
        do {
            let text = try String(contentsOf: epochURL, encoding: .utf8)
            guard let value = Int(text.trimmingCharacters(in: .whitespacesAndNewlines)) else {
                throw JournalError.io("unreadable epoch file")
            }
            return Epoch(value)
        } catch let error as JournalError {
            throw error
        } catch {
            throw JournalError.io(String(describing: error))
        }
    }

    private func writeAtomically(_ data: Data, to url: URL) throws {
        do { try data.write(to: url, options: .atomic) } catch { throw JournalError.io(String(describing: error)) }
    }
}
