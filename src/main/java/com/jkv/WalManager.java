package com.jkv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.function.Consumer;

public class WalManager {
    private static final Logger logger = LoggerFactory.getLogger(WalManager.class);
    private final File walFile;

    public WalManager(File walFile) {
        this.walFile = walFile;
    }

    public void appendPut(String key, String value) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(walFile, true);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos))) {
            writer.write("PUT " + key + "=" + (value == null ? "null" : value));
            writer.newLine();
            writer.flush();
            fos.getFD().sync();
        }
    }

    public void replay(Consumer<Entry> entryConsumer) throws IOException {
        if (!walFile.exists()) return;

        try (BufferedReader reader = new BufferedReader(new FileReader(walFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("PUT ")) {
                    String[] parts = line.substring(4).split("=", 2);
                    if (parts.length == 2) {
                        String value = parts[1];
                        if ("null".equals(value)) {
                            value = null;
                        }
                        entryConsumer.accept(new Entry(parts[0], value));
                    }
                }
            }
        }
    }

    public void clear() throws IOException {
        new FileWriter(walFile, false).close();
    }

    public record Entry(String key, String value) {}

}
