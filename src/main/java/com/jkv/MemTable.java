package com.jkv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TreeMap;

public class MemTable {
    private static final Logger logger = LoggerFactory.getLogger(MemTable.class);
    private final TreeMap<String, String> memtable = new TreeMap<>();
    private final int memtableLimit;
    private final SSTableManager sstableManager;
    private static final String TOMBSTONE = "__TOMBSTONE__";

    public MemTable(int memtableLimit, SSTableManager sstableManager) {
        this.memtableLimit = memtableLimit;
        this.sstableManager = sstableManager;
    }

    public void put(String key, String value) {
        memtable.put(key, value);
    }

    public String get(String key) {
        String value = memtable.get(key);
        if (value == null) return null;
        if (TOMBSTONE.equals(value)) return null;
        return value;
    }
    public boolean isFull() {
        return memtable.size() >= memtableLimit;
    }

    public void flush() throws IOException {
        sstableManager.flush(memtable);
        memtable.clear();
        logger.info("MemTable flushed and cleared.");
    }

    public boolean containsKey(String key) {
        return memtable.containsKey(key);
    }

}
