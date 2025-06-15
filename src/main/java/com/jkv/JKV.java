package com.jkv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class JKV {
    private static final Logger logger = LoggerFactory.getLogger(JKV.class);

    private final WalManager walManager;
    private final MemTable memTable;
    private final SSTableManager sstableManager;

    private static final int MEMTABLE_LIMIT = 1000;
    private static final String TOMBSTONE = "__TOMBSTONE__";

    public JKV() throws IOException {
        File sstableDir = new File("sstables");
        this.sstableManager = new SSTableManager(sstableDir);
        this.memTable = new MemTable(MEMTABLE_LIMIT, sstableManager);
        File walFile = new File("wal.log");
        this.walManager = new WalManager(walFile);

        walManager.replay(entry -> memTable.put(entry.key(), entry.value()));
    }

    public void put(String key, String value) throws IOException {
        walManager.appendPut(key, value);
        memTable.put(key, value);

        if (memTable.isFull()) {
            logger.info("MemTable full, flushing to SSTable...");
            memTable.flush();
            walManager.clear();
            sstableManager.compactIfNeeded();
            logger.info("Flush and compaction complete.");
        }
    }



    public void del(String key) throws IOException {
        put(key, TOMBSTONE);
    }

    public String get(String key) throws IOException {
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }

        return sstableManager.getFromSSTables(key);
    }


}
