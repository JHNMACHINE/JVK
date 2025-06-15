# Key-Value Store Improvement Checklist

Improvemente for our simple key-value store with memtable, WAL, and SSTables:

- [ ] **1. Make concurrent access thread-safe**  
  Synchronize critical methods or use locks (e.g. `ReentrantReadWriteLock`)  
  Consider concurrent data structures like `ConcurrentSkipListMap`

- [ ] **7. Implement multi-level and asynchronous compaction**  
  Manage L0, L1, etc. levels to reduce compaction costs  
  Run compaction in the background to avoid blocking operations

- [ ] **10. Optimize caching and metadata updates**  
  Cache SSTable file lists in memory  
  Avoid rebuilding indexes on every lookup  
  Consider asynchronous refresh and invalidation strategies

---

## ✅ Completed steps

- [x] **2. Improve WAL management and crash recovery**  
  Always write to the WAL before updating the memtable  
  Flush and clear memtable only after reaching threshold  
  Use `FileDescriptor.sync()` for durable writes

- [x] **3. Use a more structured and possibly compressed file format**  
  Use binary encoding with fixed structure  
  Implement SSTable file compression (.bin and .idx)  
  Add magic number (`0x4A4B565F`) for file validation

- [x] **4. Clearly handle tombstones for deletions**  
  Defined explicit tombstone value `"__TOMBSTONE__"`  
  Persist tombstones in SSTables to avoid resurrection of deleted keys

- [x] **5. Optimize SSTable search**  
  Maintain `.idx` index files mapping key → offset  
  Load index into memory at startup  
  Use `RandomAccessFile.seek()` for direct access  
  ➕ Future: Evaluate `MappedByteBuffer` for large SSTables

- [x] **6. Improve exception handling and logging**  
  Use SLF4J for logging (with warnings for missing/corrupt files)  
  Retry file deletions with backoff  
  Log meaningful messages for all I/O operations

- [X] **8. Expand API and functionality**  
  Add methods like `containsKey()`, `size()`, `clear()`, and iterators  
  Support batch operations like `putAll(Map<String,String>)`

- [x] **9. Modularize the codebase**  
  Extracted logic into `WalManager`, `MemTable`, `SSTableManager`, `SSTable`  
  Easier to test and extend

---

## 🔄 Potential Future Improvements

- [ ] **Memory-mapped I/O (MMAP)**  
  Replace `RandomAccessFile` with `MappedByteBuffer` for very large files  
  Improve OS-level caching and random read performance

- [ ] **Bloom Filters**  
  Add per-SSTable Bloom filter to avoid unnecessary `.bin` seeks

- [ ] **Snapshot Support**  
  Allow saving and restoring consistent snapshots of MemTable + SSTables

- [ ] **Write Throttling & Backpressure**  
  Prevent write spikes from overwhelming flush/compaction

- [ ] **Configuration & Metrics**  
  Externalize settings (flush size, WAL path, etc.)  
  Expose basic runtime metrics (SSTable count, key count, etc.)

---

By completing these steps, our key-value store will become more reliable, efficient, and production-ready.
