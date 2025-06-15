package com.jkv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class JKV {
    private static final Logger logger = LoggerFactory.getLogger(JKV.class);

    private final WalManager walManager;
    private final MemTable memTable;
    private final SSTableManager sstableManager;

    private static final int MEMTABLE_LIMIT = 5000;
    private static final String TOMBSTONE = "__TOMBSTONE__";

    public JKV() throws IOException {
        File sstableDir = new File("sstables");
        this.sstableManager = new SSTableManager(sstableDir);
        this.memTable = new MemTable(MEMTABLE_LIMIT, sstableManager);
        File walFile = new File("wal.log");
        this.walManager = new WalManager(walFile);

        logger.info("Replaying WAL to restore MemTable...");
        walManager.replay(entry -> memTable.put(entry.key(), entry.value()));
        logger.info("WAL replay completed. MemTable restored with {} entries.", memTable.size());
    }

    public void put(String key, String value) throws IOException {

        walManager.appendPut(key, value);
        memTable.put(key, value);

        if (memTable.isFull()) {
            logger.info("MemTable reached limit ({} entries). Flushing to SSTable.", MEMTABLE_LIMIT);
            memTable.flush();
            logger.info("MemTable flushed successfully.");

            walManager.clear();
            logger.info("WAL cleared after flush.");

            sstableManager.compactIfNeeded();
            logger.info("SSTable compaction completed if needed.");
        }
    }

    public void del(String key) throws IOException {
        logger.debug("Deleting key='{}' (marking as tombstone)", key);
        put(key, TOMBSTONE);
    }

    public String get(String key) {
        if (memTable.containsKey(key)) {
            String value = memTable.get(key);
            logger.debug("Found key='{}' in MemTable with value='{}'", key, value);
            return value;
        }

        String value = sstableManager.getFromSSTables(key);
        if (value != null) {
            logger.debug("Found key='{}' in SSTables with value='{}'", key, value);
        } else {
            logger.debug("Key='{}' not found in SSTables.", key);
        }
        return value;
    }
}
