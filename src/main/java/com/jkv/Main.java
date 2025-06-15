package com.jkv;


import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        JKV db = new JKV();

        for (int i = 0; i < 10000; i++) {
            db.put("key" + i, "value" + i);
        }

        System.out.println("key42 = " + db.get("key42"));
        System.out.println("key2048 = " + db.get("key2048"));
        System.out.println("key3499 = " + db.get("key3499"));
        System.out.println("key5000 = " + db.get("key5000"));

        for (int i = 0; i < 10000; i++) {
            db.get("key" + i);
        }
    }
}