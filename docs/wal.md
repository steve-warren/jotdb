# Write-Ahead Log

## Overview

The Write-Ahead Log is designed to ensure data durability and consistency in
the database system by writing transaction records sequentially to
non-volatile storage before applying them to the main database files. The
WAL uses a single-writer, multi-reader architecture with the following key
components:

- **Transaction Queue**: A queue that stores incoming transactions from the
  database engine.
- **WAL Writer Thread**: A dedicated thread responsible for monitoring the
  transaction queue, merging transactions, and writing log records to disk.

### Rotation

To manage the size of the WAL file, a rotation mechanism is employed:

- When the WAL file reaches its maximum size, a new WAL file is created
  asynchronously with a timestamped suffix: `jotdb_yyyyMMddHHmmssff.wal`.
- The newest WAL file is used for writing new log records.
- Previous WAL files are preserved and can be replayed during recovery.

### Transaction Merging

To minimize the number of disk I/O operations, the WAL writer thread employs
transaction merging:

- All incoming database transactions are placed in the transaction queue.
- The WAL writer thread monitors the queue and merges multiple transactions
  into a single, atomic write to disk when possible.

### Direct Disk Access

The WAL writer bypasses operating system and disk caching mechanisms by
employing specific file handle flags:

**Windows**:

- `FILE_FLAG_NO_BUFFERING`: Disables intermediate buffering of data.
- `FILE_FLAG_WRITE_THROUGH`: Ensures each write operation completes immediately
  without caching.

**macOS**:

- Uses the `fcntl` function with the `F_NOCACHE` flag to disable caching for the
  WAL file.

### Sequential Writing

The WAL writer appends data sequentially to the log files, utilizing the
performance benefits of sequential I/O operations and
reducing fragmentation.
