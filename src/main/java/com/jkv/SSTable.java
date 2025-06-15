package com.jkv;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;

public class SSTable {
    final File binFile;
    final File idxFile;
    final TreeMap<String, Long> index = new TreeMap<>();
    MappedByteBuffer mappedBuffer;

    SSTable(File binFile, File idxFile) throws IOException {
        this.binFile = binFile;
        this.idxFile = idxFile;
        loadIndex();
        mapFile();
    }

    private void loadIndex() throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(idxFile)))) {
            while (in.available() > 0) {
                int keyLen = in.readInt();
                byte[] keyBytes = in.readNBytes(keyLen);
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                long offset = in.readLong();
                index.put(key, offset);
            }
        }
    }

    private void mapFile() throws IOException {
        try (FileChannel channel = FileChannel.open(binFile.toPath(), StandardOpenOption.READ)) {
            mappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
    }

    public String search(String key) {
        Long offset = index.get(key);
        if (offset == null) return null;
        try (RandomAccessFile raf = new RandomAccessFile(binFile, "r")) {
            raf.seek(offset);
            int keyLen = raf.readInt();
            raf.skipBytes(keyLen); // Salta la chiave

            int valLen = raf.readInt();
            if (valLen == -1) return "__TOMBSTONE__";
            byte[] valBytes = new byte[valLen];
            raf.readFully(valBytes);
            return new String(valBytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }
}
