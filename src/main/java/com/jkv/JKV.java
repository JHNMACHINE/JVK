package com.jkv;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class JKV {
    private final File walFile = new File("wal.log");
    private final TreeMap<String, String> memtable = new TreeMap<>();
    private static final int MEMTABLE_LIMIT = 1000;
    private final File sstableDir = new File("sstables");
    private static final int MAX_SSTABLES = 3;

    private static final String TOMBSTONE = null;

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
        if (memtable.size() >= MEMTABLE_LIMIT) {
            flushMemTableToDisk();
            memtable.clear();
            clearWAL();
            compactIfNeeded();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(walFile, true))) {
            writer.write("PUT " + key + "=" + (value == null ? "null" : value));
            writer.newLine();
        }

        memtable.put(key, value);
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
            String v = searchSSTable(f, key);
            if (v != null) {
                if (v.equals("null")) return null; // tombstone
                return v;
            }
        }

        return null;
    }

    private String searchSSTable(File file, String key) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=", 2);
                if (parts.length == 2) {
                    int cmp = parts[0].compareTo(key);
                    if (cmp == 0) return parts[1];
                    if (cmp > 0) break; // sorting
                }
            }
        }
        return null;
    }


    private void flushMemTableToDisk() throws IOException {
        long ts = System.currentTimeMillis();
        File flushFile = new File(sstableDir, "sstable_" + ts + ".db");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(flushFile))) {
            for (Map.Entry<String, String> entry : memtable.entrySet()) {
                // tombstone
                if (!Objects.equals(entry.getValue(), TOMBSTONE)) {
                    writer.write(entry.getKey() + "=" + entry.getValue());
                    writer.newLine();
                }
            }
        }
        System.out.println("MemTable flushed to " + flushFile.getName());
    }

    private void clearWAL() throws IOException {
        new FileWriter(walFile, false).close();
    }

    public void compactIfNeeded() throws IOException {
        File[] files = sstableDir.listFiles((_, name) -> name.startsWith("sstable_"));
        if (files == null || files.length <= MAX_SSTABLES) return;

        List<File> sortedFiles = Arrays.stream(files)
                .sorted(Comparator.comparing(File::getName))
                .collect(Collectors.toList());

        List<File> toMerge = sortedFiles.subList(0, 2);

        File mergedFile = new File(sstableDir, "sstable_" + System.currentTimeMillis() + ".db");
        mergeSSTables(toMerge, mergedFile);

        for (File f : toMerge) {
            boolean deleted = f.delete();
            if (deleted) {
                System.out.println("Deleted old SSTable " + f.getName());
            } else {
                System.err.println("Failed to delete SSTable file: " + f.getAbsolutePath());
            }
        }

        System.out.println("Compaction done, created " + mergedFile.getName());
    }

    private void mergeSSTables(List<File> inputs, File output) throws IOException {
        List<BufferedReader> readers = new ArrayList<>();
        try {
            for (File f : inputs) {
                readers.add(new BufferedReader(new FileReader(f)));
            }

            String[] currentLines = new String[readers.size()];
            String[] currentKeys = new String[readers.size()];
            String[] currentValues = new String[readers.size()];

            for (int i = 0; i < readers.size(); i++) {
                currentLines[i] = readers.get(i).readLine();
                if (currentLines[i] != null) {
                    String[] parts = currentLines[i].split("=", 2);
                    currentKeys[i] = parts[0];
                    currentValues[i] = parts[1];
                } else {
                    currentKeys[i] = null;
                    currentValues[i] = null;
                }
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {
                while (true) {
                    String minKey = null;
                    int minIndex = -1;
                    for (int i = 0; i < currentKeys.length; i++) {
                        if (currentKeys[i] != null) {
                            if (minKey == null || currentKeys[i].compareTo(minKey) < 0) {
                                minKey = currentKeys[i];
                                minIndex = i;
                            }
                        }
                    }
                    if (minKey == null) break;
                    String finalKey = minKey;
                    String finalValue = currentValues[minIndex];
                    currentLines[minIndex] = readers.get(minIndex).readLine();
                    if (currentLines[minIndex] != null) {
                        String[] parts = currentLines[minIndex].split("=", 2);
                        currentKeys[minIndex] = parts[0];
                        currentValues[minIndex] = parts[1];
                    } else {
                        currentKeys[minIndex] = null;
                        currentValues[minIndex] = null;
                    }

                    for (int i = 0; i < currentKeys.length; i++) {
                        if (i != minIndex && finalKey.equals(currentKeys[i])) {
                            finalValue = currentValues[i];
                            currentLines[i] = readers.get(i).readLine();
                            if (currentLines[i] != null) {
                                String[] parts = currentLines[i].split("=", 2);
                                currentKeys[i] = parts[0];
                                currentValues[i] = parts[1];
                            } else {
                                currentKeys[i] = null;
                                currentValues[i] = null;
                            }
                        }
                    }

                    if (!"null".equals(finalValue)) {
                        writer.write(finalKey + "=" + finalValue);
                        writer.newLine();
                    }
                }
            }
        } finally {
            for (BufferedReader r : readers) {
                if (r != null) r.close();
            }
        }
    }

}
