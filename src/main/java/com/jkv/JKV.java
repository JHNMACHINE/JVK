package com.jkv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class JKV {
    private static final Logger logger = LoggerFactory.getLogger(JKV.class);
    private final File walFile = new File("wal.log");
    private final TreeMap<String, String> memtable = new TreeMap<>();
    private static final int MEMTABLE_LIMIT = 1000;
    private final File sstableDir = new File("sstables");

    private static final String TOMBSTONE = "__TOMBSTONE__";
    private static final int MAGIC = 0x4A4B565F; // "JKV_"

    public JKV() throws IOException {
        if (!sstableDir.exists()) {
            boolean created = sstableDir.mkdirs();
            if (!created) {
                throw new IOException("Failed to create directory tree: " + sstableDir.getAbsolutePath());
            }
        }
        replayWAL();
    }

    private void replayWAL() throws IOException {
        if (!walFile.exists()) return;

        try (BufferedReader reader = new BufferedReader(new FileReader(walFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("PUT ")) {
                    String[] parts = line.substring(4).split("=", 2);
                    if (parts.length == 2) {
                        String value = parts[1];
                        if ("null".equals(value)) {
                            value = null;  // gestisci tombstone nel WAL come stringa "null"
                        }
                        memtable.put(parts[0], value);
                    }
                }
            }
        }
    }

    public void put(String key, String value) throws IOException {
        // 1. Scrivi prima sul WAL con sync su disco
        try (FileOutputStream fos = new FileOutputStream(walFile, true);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos))) {
            writer.write("PUT " + key + "=" + (value == null ? "null" : value));
            writer.newLine();
            writer.flush();      // svuota il buffer del writer
            fos.getFD().sync();  // forza la scrittura fisica su disco
        }

        // 2. Aggiorna la memtable in memoria
        memtable.put(key, value);

        // 3. Se la memtable supera la dimensione, flush + clear WAL + compattazione
        if (memtable.size() >= MEMTABLE_LIMIT) {
            flushMemTableToDisk();
            memtable.clear();
            clearWAL();
            compactIfNeeded();
        }
    }


    public void del(String key) throws IOException {
        put(key, TOMBSTONE);
    }

    public String get(String key) throws IOException {
        if (memtable.containsKey(key)) {
            String value = memtable.get(key);
            if (Objects.equals(value, TOMBSTONE)) return null;
            else return value;
        }

        File[] files = sstableDir.listFiles((_, name) -> name.startsWith("sstable_"));
        if (files == null) return null;

        List<File> sortedFiles = Arrays.stream(files)
                .sorted(Comparator.comparing(File::getName).reversed())
                .toList();

        for (File f : sortedFiles) {
            String value = searchSSTable(f, key);
            if (value != null) {
                if (TOMBSTONE.equals(value)) return null; // key is deleted
                return value;
            }
        }

        return null;
    }

    private String searchSSTable(File file, String key) throws IOException {
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




    private void flushMemTableToDisk() throws IOException {
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


    private void clearWAL() throws IOException {
        new FileWriter(walFile, false).close();
    }

    public void compactIfNeeded() throws IOException {
        File[] files = sstableDir.listFiles((_, name) -> name.endsWith(".bin"));
        if (files == null || files.length < 2) return;

        // Sort SSTables by name (timestamp-based)
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

                    // We overwrite because later files are newer
                    mergedMap.put(key, value);
                }
            }
        }

        // Write merged SSTable
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
