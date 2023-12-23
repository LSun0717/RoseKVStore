package com.gzu.rosekv.api;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @Classname RoseKVStoreTest
 * @Description TODO
 * @Version 1.0.0
 * @Date 12/24/2023 1:08 AM
 * @Created by LIONS7
 */
public class RoseKVStoreTest {

    @Test
    public void set() throws IOException {
        var roseKV = new RoseKvStore("D:\\Lesson\\rosekvdb\\db\\" ,4, 3);
        for (int i = 0; i < 11; i++) {
            roseKV.set(i + "", i + "");
        }
        for (int i = 0; i < 11; i++) {
            assertEquals(i + "", roseKV.get(i + ""));
        }
        for (int i = 0; i < 11; i++) {
            roseKV.rm(i + "");
        }
        for (int i = 0; i < 11; i++) {
            assertNull(roseKV.get(i + ""));
        }
        roseKV.close();
        roseKV = new RoseKvStore("D:\\Lesson\\rosekvdb\\db\\", 4, 3);
        for (int i = 0; i < 11; i++) {
            assertNull(roseKV.get(i + ""));
        }
        roseKV.close();
    }
}
