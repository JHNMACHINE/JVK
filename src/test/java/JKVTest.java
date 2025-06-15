import com.jkv.JKV;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class JKVTest {

    JKV db;

    @BeforeEach
    void setup() throws IOException {
        // Pulisci directory e file WAL
        File sstablesDir = new File("sstables");
        if (sstablesDir.exists()) {
            for (File f : Objects.requireNonNull(sstablesDir.listFiles())) {
                f.delete();
            }
        }

        File wal = new File("wal.log");
        if (wal.exists()) {
            wal.delete();
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
        // Forza la creazione di più SSTable per attivare la compaction
        int entriesPerFlush = 1000;
        for (int i = 0; i < entriesPerFlush * 4; i++) {
            db.put("key" + i, "val" + i);
        }

        // Cancella alcune chiavi sparse
        for (int i = 0; i < entriesPerFlush * 4; i += 500) {
            db.del("key" + i);
        }

        // Forza la compaction (chiamandola direttamente, o inserendo più dati)
        db.compactIfNeeded();

        // Verifica che chiavi cancellate siano assenti
        for (int i = 0; i < (entriesPerFlush * 4); i += 500) {
            Assertions.assertNull(db.get("key" + i), "La chiave cancellata key" + i + " dovrebbe essere null");
        }

        // Verifica che altre chiavi siano ancora presenti
        Assertions.assertEquals("val1", db.get("key1"));
        Assertions.assertEquals("val999", db.get("key999"));
    }
}
