package com.jkv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
        put(key, TOMBSTONE);
    }

    public String get(String key) {
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }

        return sstableManager.getFromSSTables(key);
    }

    public boolean containsKey(String key) {
        String val = memTable.get(key);
        if (val != null) return true;
        if (memTable.containsKey(key)) return false; // è tombstone

        return sstableManager.getFromSSTables(key) != null;
    }

    public int size() {
        Set<String> seen = new HashSet<>();
        int count = 0;

        for (Map.Entry<String, String> entry : memTable.entrySet()) {
            if (!SSTableManager.TOMBSTONE.equals(entry.getValue())) {
                seen.add(entry.getKey());
                count++;
            }
        }

        for (SSTable sstable : sstableManager.getSSTables()) {
            for (Map.Entry<String, String> entry : sstable.iterate()) {
                if (seen.contains(entry.getKey())) continue;
                if (!SSTableManager.TOMBSTONE.equals(entry.getValue())) {
                    seen.add(entry.getKey());
                    count++;
                }
            }
        }

        return count;
    }

    public void putAll(Map<String, String> map) throws IOException {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    public Set<String> keySet() {
        Map<String, String> merged = new TreeMap<>();
        for (SSTable sstable : sstableManager.getSSTables()) {
            for (Map.Entry<String, String> e : sstable.iterate()) {
                merged.put(e.getKey(), e.getValue());
            }
        }
        merged.putAll(memTable.getMemtable());
        merged.values().removeIf(SSTableManager.TOMBSTONE::equals);
        return merged.keySet();
    }


    public void clear() throws IOException {
        for (String key : keySet()) {
            del(key);  // usa il metodo del() per cancellare ogni chiave
        }
    }


    public Iterable<? extends Map.Entry<String, String>> entrySet() {
        // Usa una TreeMap per mantenere l'ordine e gestire conflitti (memTable prevale su SSTable)
        Map<String, String> merged = new TreeMap<>();

        // Prima carica tutte le entry dagli SSTable
        for (SSTable sstable : sstableManager.getSSTables()) {
            for (Map.Entry<String, String> e : sstable.iterate()) {
                merged.put(e.getKey(), e.getValue());
            }
        }

        // Sovrascrivi con i dati più recenti in memTable (inclusi tombstone)
        merged.putAll(memTable.getMemtable());

        // Rimuovi i tombstone (cioè le chiavi con valore tombstone)
        merged.values().removeIf(SSTableManager.TOMBSTONE::equals);

        // Restituisci l'entrySet del map risultante, che è Iterable<Map.Entry<String,String>>
        return merged.entrySet();
    }
}
