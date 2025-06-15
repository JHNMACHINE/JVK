package com.jkv;


import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {
        JKV db = new JKV();

        // Inserimento massivo con put
        for (int i = 0; i < 10000; i++) {
            db.put("key" + i, "value" + i);
        }

        System.out.println("key42 = " + db.get("key42"));
        System.out.println("key2048 = " + db.get("key2048"));
        System.out.println("key3499 = " + db.get("key3499"));
        System.out.println("key5000 = " + db.get("key5000"));

        // Controllo containsKey e size
        System.out.println("Contains key42? " + db.containsKey("key42"));
        System.out.println("Current DB size: " + db.size());

        // Lettura massiva con get (solo per dimostrazione)
        for (int i = 0; i < 10000; i++) {
            String val = db.get("key" + i);
            if (val == null) {
                System.err.println("Missing key: key" + i);
            }
        }

        // Test batch putAll
        Map<String, String> batch = new HashMap<>();
        batch.put("alpha", "a");
        batch.put("beta", "b");
        batch.put("gamma", "c");
        db.putAll(batch);

        System.out.println("After putAll:");
        System.out.println("alpha = " + db.get("alpha"));
        System.out.println("beta = " + db.get("beta"));
        System.out.println("gamma = " + db.get("gamma"));

        // Test del
        db.del("alpha");
        System.out.println("After deleting alpha, containsKey(alpha)? " + db.containsKey("alpha"));

        // Stampa tutte le chiavi (keySet)
        System.out.println("Sample keys:");
        int count = 0;
        for (String key : db.keySet()) {
            System.out.print(key + " ");
            if (++count >= 10) break;
        }
        System.out.println();

        // Stampa alcune entry (entrySet)
        System.out.println("Sample entries:");
        count = 0;
        for (var entry : db.entrySet()) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
            if (++count >= 10) break;
        }

        // Test clear
        System.out.println("Clearing database...");
        db.clear();
        System.out.println("DB size after clear: " + db.size());
    }
}
