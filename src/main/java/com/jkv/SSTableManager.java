package com.jkv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SSTableManager {
    private static final Logger logger = LoggerFactory.getLogger(SSTableManager.class);
    private final File sstableDir;
    private static final int MAGIC = 0x4A4B565F; // "JKV_"
    static final String TOMBSTONE = "__TOMBSTONE__";
    private final List<SSTable> sstables = new ArrayList<>();

    public SSTableManager(File sstableDir) throws IOException {
        this.sstableDir = sstableDir;
        if (!sstableDir.exists()) {
            boolean created = sstableDir.mkdirs();
            if (!created) {
                throw new IOException("Failed to create directory tree: " + sstableDir.getAbsolutePath());
            }
        }
        loadSSTables();
    }

    private void loadSSTables() throws IOException {
        File[] binFiles = sstableDir.listFiles((_, name) -> name.endsWith(".bin"));
        if (binFiles == null) return;

        for (File binFile : binFiles) {
            File idxFile = new File(binFile.getAbsolutePath().replace(".bin", ".idx"));
            if (!idxFile.exists()) {
                logger.warn("Missing index file for SSTable: {}", binFile.getName());
                continue;
            }
            sstables.add(new SSTable(binFile, idxFile));
        }
    }

    public void flush(TreeMap<String, String> memtable) throws IOException {
        long ts = System.currentTimeMillis();
        File flushFile = new File(sstableDir, "sstable_" + ts + ".bin");
        File indexFile = new File(sstableDir, "sstable_" + ts + ".idx");

        writeSSTableWithIndex(flushFile, indexFile, memtable);

        logger.info("Flushed MemTable with index: {} and {}", flushFile.getName(), indexFile.getName());
        sstables.add(new SSTable(flushFile, indexFile));
    }

    public String getFromSSTables(String key) {
        for (int i = sstables.size() - 1; i >= 0; i--) {
            String val = sstables.get(i).search(key);
            if (val != null) {
                if (TOMBSTONE.equals(val)) return null;
                return val;
            }
        }
        return null;
    }

    public void compactIfNeeded() throws IOException {
        File[] files = sstableDir.listFiles((_, name) -> name.endsWith(".bin"));
        if (files == null || files.length < 2) return;

        Arrays.sort(files, Comparator.comparing(File::getName));

        Map<String, String> mergedMap = new TreeMap<>();

        // Carica tutte le SSTable in mergedMap
        for (File file : files) {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                int magic = in.readInt();
                if (magic != MAGIC) {
                    logger.warn("Skipping corrupted SSTable: {}", file.getName());
                    continue;
                }

                int count = in.readInt();
                for (int i = 0; i < count; i++) {
                    int keyLen = in.readInt();
                    byte[] keyBytes = in.readNBytes(keyLen);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    int valLen = in.readInt();
                    String value = (valLen == -1) ? null : new String(in.readNBytes(valLen), StandardCharsets.UTF_8);

                    mergedMap.put(key, value);
                }
            }
        }

        // Nuovo nome per la SSTable compatta
        String baseName = "sstable_" + System.currentTimeMillis() + "_merged";
        File compactedBin = new File(sstableDir, baseName + ".bin");
        File compactedIdx = new File(sstableDir, baseName + ".idx");

        writeSSTableWithIndex(compactedBin, compactedIdx, mergedMap);

        logger.info("Compacted SSTables into: {} and {}", compactedBin.getName(), compactedIdx.getName());

        // Cancella vecchi file .bin e .idx
        for (File file : files) {
            File idxFile = new File(file.getAbsolutePath().replace(".bin", ".idx"));

            if (deleteWithRetry(file)) {
                logger.warn("Failed to delete old SSTable: {}", file.getName());
            }
            if (idxFile.exists() && deleteWithRetry(idxFile)) {
                logger.warn("Failed to delete old SSTable index: {}", idxFile.getName());
            }
        }

        // Ricarica SSTables per aggiornare la lista in memoria
        sstables.clear();
        loadSSTables();
    }

    private void writeSSTableWithIndex(File binFile, File idxFile, Map<String, String> data) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(binFile, "rw");
             DataOutputStream idxOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(idxFile)))) {

            raf.writeInt(MAGIC);
            raf.writeInt(data.size());

            for (Map.Entry<String, String> entry : data.entrySet()) {
                long pos = raf.getFilePointer();

                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] valBytes = entry.getValue() == null ? null : entry.getValue().getBytes(StandardCharsets.UTF_8);

                raf.writeInt(keyBytes.length);
                raf.write(keyBytes);

                if (valBytes == null) {
                    raf.writeInt(-1);
                } else {
                    raf.writeInt(valBytes.length);
                    raf.write(valBytes);
                }

                // Scrivi indice: chiave + posizione
                idxOut.writeInt(keyBytes.length);
                idxOut.write(keyBytes);
                idxOut.writeLong(pos);
            }
        }
    }


    private boolean deleteWithRetry(File file) {
        for (int i = 0; i < 3; i++) {
            if (file.delete()) return false;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return true;
            }
        }
        return true;
    }

    public List<SSTable> getSSTables() {
        return Collections.unmodifiableList(sstables);
    }

}
