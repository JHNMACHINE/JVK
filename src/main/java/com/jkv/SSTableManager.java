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
    private static final String TOMBSTONE = "__TOMBSTONE__";

    public SSTableManager(File sstableDir) throws IOException {
        this.sstableDir = sstableDir;
        if (!sstableDir.exists()) {
            boolean created = sstableDir.mkdirs();
            if (!created) {
                throw new IOException("Failed to create directory tree: " + sstableDir.getAbsolutePath());
            }
        }
    }

    public void flush(TreeMap<String, String> memtable) throws IOException {
        long ts = System.currentTimeMillis();
        File flushFile = new File(sstableDir, "sstable_" + ts + ".bin");

        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(flushFile)))) {
            out.writeInt(MAGIC);
            out.writeInt(memtable.size());

            for (Map.Entry<String, String> entry : memtable.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = entry.getValue() == null ? null : entry.getValue().getBytes(StandardCharsets.UTF_8);

                out.writeInt(keyBytes.length);
                out.write(keyBytes);

                if (valueBytes == null) {
                    out.writeInt(-1); // tombstone
                } else {
                    out.writeInt(valueBytes.length);
                    out.write(valueBytes);
                }
            }
        }

        logger.info("MemTable flushed to binary SSTable: {}", flushFile.getName());
    }

    public String search(File file, String key) throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IOException("Corrupted SSTable: " + file.getName());
            }

            int numEntries = in.readInt();

            for (int i = 0; i < numEntries; i++) {
                int keyLen = in.readInt();
                byte[] keyBytes = in.readNBytes(keyLen);
                String currentKey = new String(keyBytes, StandardCharsets.UTF_8);

                int valLen = in.readInt();
                byte[] valueBytes = (valLen == -1) ? null : in.readNBytes(valLen);
                String currentValue = (valueBytes == null) ? null : new String(valueBytes, StandardCharsets.UTF_8);

                int cmp = currentKey.compareTo(key);
                if (cmp == 0) return currentValue;
                if (cmp > 0) break;
            }
        }

        return null;
    }

    public String getFromSSTables(String key) throws IOException {
        File[] files = sstableDir.listFiles((_, name) -> name.startsWith("sstable_"));
        if (files == null) return null;

        List<File> sortedFiles = Arrays.stream(files)
                .sorted(Comparator.comparing(File::getName).reversed())
                .toList();

        for (File f : sortedFiles) {
            String value = search(f, key);
            if (value != null) {
                if (TOMBSTONE.equals(value)) return null;
                return value;
            }
        }

        return null;
    }

    public void compactIfNeeded() throws IOException {
        File[] files = sstableDir.listFiles((_, name) -> name.endsWith(".bin"));
        if (files == null || files.length < 2) return;

        Arrays.sort(files, Comparator.comparing(File::getName));

        Map<String, String> mergedMap = new TreeMap<>();

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

        File compacted = new File(sstableDir, "sstable_" + System.currentTimeMillis() + "_merged.bin");
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(compacted)))) {
            out.writeInt(MAGIC);
            out.writeInt(mergedMap.size());

            for (Map.Entry<String, String> entry : mergedMap.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                out.writeInt(keyBytes.length);
                out.write(keyBytes);

                if (entry.getValue() == null) {
                    out.writeInt(-1); // tombstone
                } else {
                    byte[] valueBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    out.writeInt(valueBytes.length);
                    out.write(valueBytes);
                }
            }
        }

        logger.info("Compacted SSTables into: {}", compacted.getName());

        // Delete old SSTables
        for (File file : files) {
            if (!deleteWithRetry(file)) {
                logger.warn("Failed to delete old SSTable: {}", file.getName());
            }
        }
    }

    private boolean deleteWithRetry(File file) {
        for (int i = 0; i < 3; i++) {
            if (file.delete()) return true;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }
}
