package com.gzu.rosekv.model.sstable;

import com.gzu.rosekv.model.command.Command;
import com.gzu.rosekv.model.command.RmCommand;
import com.gzu.rosekv.model.command.SetCommand;
import org.junit.Test;

import java.io.IOException;
import java.util.TreeMap;

/**
 * @Classname SstableTest
 * @Description TODO
 * @Version 1.0.0
 * @Date 12/24/2023 12:49 AM
 * @Created by LIONS7
 */
public class SstableTest {
    @Test
    public void createFromIndex() throws IOException {
        var index = new TreeMap<String, Command>();
        for (int i = 0; i < 10; i++) {
            var setCommand = new SetCommand("key" + i, "value" + i);
            index.put(setCommand.getKey(), setCommand);
        }
        index.put("key100", new SetCommand("key100", "value100"));
        index.put("key100", new RmCommand("key100"));
        SsTable ssTable = SsTable.createFromIndex("test.txt", 3, index);
    }

    @Test
    public void query() throws IOException {
        var ssTable = SsTable.createFromFile("test.txt");
        System.out.println(ssTable.query("key0"));
        System.out.println(ssTable.query("key5"));
        System.out.println(ssTable.query("key9"));
        System.out.println(ssTable.query("key100"));
    }
}
