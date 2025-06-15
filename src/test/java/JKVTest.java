import com.jkv.JKV;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class JKVTest {

    JKV db;

    @BeforeEach
    void setup() throws IOException {
        // Pulisci directory e file WAL
        File sstablesDir = new File("sstables");
        if (sstablesDir.exists()) {
            for (File f : Objects.requireNonNull(sstablesDir.listFiles())) {
                boolean deleted = f.delete();
                if (!deleted) {
                    System.err.println("WARNING: Failed to delete file " + f.getAbsolutePath());
                    // oppure usa logger.warn se hai logger a disposizione
                }
            }
        }

        File wal = new File("wal.log");
        if (wal.exists()) {
            boolean deleted = wal.delete();
            if (!deleted) {
                System.err.println("WARNING: Failed to delete WAL file " + wal.getAbsolutePath());
            }
        }

        db = new JKV();
    }


    @Test
    void testPutAndGet() throws IOException {
        db.put("key1", "value1");
        db.put("key2", "value2");
        Assertions.assertEquals("value1", db.get("key1"));
        Assertions.assertEquals("value2", db.get("key2"));
        Assertions.assertNull(db.get("key3")); // non esistente
    }

    @Test
    void testDelete() throws IOException {
        db.put("key1", "value1");
        Assertions.assertEquals("value1", db.get("key1"));
        db.del("key1");
        Assertions.assertNull(db.get("key1"));
    }

    @Test
    void testFlushAndGetFromSSTable() throws IOException {
        // Inserisci abbastanza dati per forzare il flush (MEMTABLE_LIMIT = 1000)
        for (int i = 0; i < 1000; i++) {
            db.put("key" + i, "val" + i);
        }
        // Ora la memtable dovrebbe essere svuotata e dati scritti su disco
        Assertions.assertEquals("val0", db.get("key0"));
        Assertions.assertEquals("val999", db.get("key999"));

        // Verifica che chiave cancellata non venga restituita
        db.del("key0");
        Assertions.assertNull(db.get("key0"));
    }

    @Test
    void testCompactionRemovesTombstones() throws IOException {
        int entriesPerFlush = 1000;

        // Inserisce tanti dati per creare più SSTable e triggerare flush automatici e compaction interna
        for (int i = 0; i < entriesPerFlush * 4; i++) {
            db.put("key" + i, "val" + i);
        }

        // Cancella alcune chiavi sparse (inserisce tombstone)
        for (int i = 0; i < entriesPerFlush * 4; i += 500) {
            db.del("key" + i);
        }

        // Inserisce ancora dati per forzare il flush e la compaction interni (scatenati da memTable.isFull())
        for (int i = entriesPerFlush * 4; i < entriesPerFlush * 5; i++) {
            db.put("key" + i, "val" + i);
        }

        // Ora verifica che le chiavi cancellate siano effettivamente rimosse (tombstone = null)
        for (int i = 0; i < entriesPerFlush * 4; i += 500) {
            Assertions.assertNull(db.get("key" + i), "La chiave cancellata key" + i + " dovrebbe essere null");
        }

        // Verifica che alcune chiavi non cancellate siano ancora presenti
        Assertions.assertEquals("val1", db.get("key1"));
        Assertions.assertEquals("val999", db.get("key999"));
    }


    @Test
    void testContainsKey() throws IOException {
        db.put("a", "1");
        db.put("b", "2");
        db.del("b");

        Assertions.assertTrue(db.containsKey("a"));
        Assertions.assertFalse(db.containsKey("b")); // cancellata
        Assertions.assertFalse(db.containsKey("c")); // mai esistita
    }

    @Test
    void testSize() throws IOException {
        db.put("a", "1");
        db.put("b", "2");
        db.put("c", "3");
        db.del("b");

        Assertions.assertEquals(2, db.size());
    }

    @Test
    void testPutAll() throws IOException {
        Map<String, String> batch = new HashMap<>();
        batch.put("x", "100");
        batch.put("y", "200");
        batch.put("z", "300");

        db.putAll(batch);

        Assertions.assertEquals("100", db.get("x"));
        Assertions.assertEquals("200", db.get("y"));
        Assertions.assertEquals("300", db.get("z"));
        Assertions.assertEquals(3, db.size());
    }

    @Test
    void testKeySet() throws IOException {
        db.put("k1", "v1");
        db.put("k2", "v2");
        db.put("k3", "v3");
        db.del("k2");

        Set<String> keys = db.keySet();

        Assertions.assertTrue(keys.contains("k1"));
        Assertions.assertTrue(keys.contains("k3"));
        Assertions.assertFalse(keys.contains("k2"));
        Assertions.assertEquals(2, keys.size());
    }

    @Test
    void testClear() throws IOException {
        db.put("alpha", "a");
        db.put("beta", "b");
        db.put("gamma", "c");

        db.clear();

        Assertions.assertEquals(0, db.size());
        Assertions.assertFalse(db.containsKey("alpha"));
        Assertions.assertFalse(db.containsKey("beta"));
        Assertions.assertFalse(db.containsKey("gamma"));
    }

    @Test
    void testEntrySetLikeIteration() throws IOException {
        db.put("a", "1");
        db.put("b", "2");
        db.put("c", "3");
        db.del("b");

        Set<String> keys = new HashSet<>();
        Set<String> values = new HashSet<>();

        for (Map.Entry<String, String> entry : db.entrySet()) {
            keys.add(entry.getKey());
            values.add(entry.getValue());
        }

        Assertions.assertTrue(keys.contains("a"));
        Assertions.assertTrue(keys.contains("c"));
        Assertions.assertFalse(keys.contains("b")); // era tombstone

        Assertions.assertTrue(values.contains("1"));
        Assertions.assertTrue(values.contains("3"));
        Assertions.assertEquals(2, keys.size());
    }


}
