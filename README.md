# Key-Value Store Improvement Checklist

Here are 10 key steps to improve our simple key-value store with memtable, WAL, and SSTables:

- [ ] **1. Make concurrent access thread-safe**  
  Synchronize critical methods or use locks (e.g. `ReentrantReadWriteLock`)  
  Consider concurrent data structures like `ConcurrentSkipListMap`

- [X] **2. Improve WAL management and crash recovery**  
  Always write to the WAL before updating the memtable  
  Flush and clear memtable only after reaching threshold  
  Consider using `FileDescriptor.sync()` for durable writes

- [ ] **3. Use a more structured and possibly compressed file format**  
  Consider binary or JSON formats with proper escaping  
  Implement SSTable file compression  
  Add checksums or magic numbers to detect corrupted files

- [ ] **4. Clearly handle tombstones for deletions**  
  Define an explicit tombstone value (e.g. `"__TOMBSTONE__"`)  
  Persist tombstones in SSTables to prevent resurrecting deleted keys

- [ ] **5. Optimize SSTable search**  
  Maintain in-memory or on-disk index for faster lookups  
  Consider memory-mapped files for efficient access

- [ ] **6. Improve exception handling and logging**  
  Use a logging framework (e.g. SLF4J, Log4J)  
  Handle I/O errors robustly with retries or fallback mechanisms

- [ ] **7. Implement multi-level and asynchronous compaction**  
  Manage L0, L1, etc. levels to reduce compaction costs  
  Run compaction in the background to avoid blocking operations

- [ ] **8. Expand API and functionality**  
  Add methods like `containsKey()`, `size()`, `clear()`, and iterators  
  Support batch operations like `putAll(Map<String,String>)`

- [ ] **9. Modularize the codebase**  
  Separate concerns into classes like `WalManager`, `MemTable`, `SSTableManager`  
  Facilitate testing and maintenance

- [ ] **10. Optimize caching and metadata updates**  
  Cache SSTable file lists in memory  
  Avoid rebuilding indexes on every lookup  
  Consider asynchronous refresh and invalidation strategies

---

By completing these steps, our key-value store will become more reliable, efficient, and maintainable.
