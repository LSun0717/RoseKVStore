package com.gzu.rosekv.api;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONB;
import com.alibaba.fastjson2.JSONObject;
import com.gzu.rosekv.model.command.Command;
import com.gzu.rosekv.model.command.RmCommand;
import com.gzu.rosekv.model.command.SetCommand;
import com.gzu.rosekv.model.sstable.SsTable;
import com.gzu.rosekv.util.ConvertUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Classname RoseKvStore
 * @Description TODO
 * @Version 1.0.0
 * @Date 12/23/2023 4:59 PM
 * @Created by LIONS7
 */
public class RoseKvStore implements KvStore{

    public static final String TABLE = ".table";

    public static final String WAL = "wal";

    public static final String RW_MODE = "rw";

    public static final String WAL_TMP = "walTmp";

    /**
     * 内存表
     */
    private TreeMap<String, Command> index;

    /**
     * 不可变内存表，用于持久化内存表中暂存数据
     */
    private TreeMap<String, Command> immutableIndex;

    /**
     * SSTable列表
     */
    private final LinkedList<SsTable> ssTables;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁
     */
    private final ReadWriteLock indexLock;

    /**
     * 持久化阈值
     */
    private final int storeThreshold;

    /**
     * 数据分区大小
     */
    private final int partSize;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile wal;

    /**
     * 暂存数据日志文件
     */
    private File walFile;

    public RoseKvStore(String dataDir, int storeThreshold, int partSize) throws IOException {
        this.dataDir = dataDir;
        this.storeThreshold = storeThreshold;
        this.partSize = partSize;
        this.indexLock = new ReentrantReadWriteLock();
        var dir = new File(dataDir);
        var files = dir.listFiles();
        ssTables = new LinkedList<>();
        index = new TreeMap<>();

        // 目录为空无需加载SSTable
        if (files == null || files.length == 0) {
            walFile = new File(dataDir + WAL);
            wal = new RandomAccessFile(walFile, RW_MODE);
            return;
        }

        // 从大到小加载SSTable
        var sstableTreeMap = new TreeMap<Long, SsTable>(Comparator.reverseOrder());
        for (File file : files) {
            var fileName = file.getName();
            // 从暂存的WAL中恢复数据，一般是持久化SSTable过程中异常才会留下walTmp
            if (file.isFile() && fileName.equals(WAL_TMP)) {
                restoreFromWal(new RandomAccessFile(file, RW_MODE));
            }
            // 加载SSTable
            if (file.isFile() && fileName.endsWith(TABLE)) {
                var dotIndex = fileName.indexOf(".");
                var time = Long.parseLong(fileName.substring(0, dotIndex));
                sstableTreeMap.put(time, SsTable.createFromFile(file.getAbsolutePath()));
            } else if (file.isFile() && fileName.equals(WAL)) {
                // 加载WAL
                walFile = file;
                wal = new RandomAccessFile(file, RW_MODE);
                restoreFromWal(wal);
            }
            ssTables.addAll(sstableTreeMap.values());
        }
    }

    /**
     * 从暂存日志中恢复数据
     * @param wal 暂存日志
     */
    private void restoreFromWal(RandomAccessFile wal) throws IOException {
        var len = wal.length();
        var start = 0L;
        wal.seek(start);
        while (start < len) {
            var valueLen = wal.readInt();
            var bytes = new byte[valueLen];
            wal.read(bytes);
            var value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
            var command = ConvertUtil.jsonToCommand(value);
            if (command != null) {
                index.put(command.getKey(), command);
            }
            start += 4;
            start += valueLen;
        }
        wal.seek(wal.length());
    }


    /**
     * 保存数据
     * @param key 数据键
     * @param value 数据值
     */
    @Override
    public void set(String key, String value) {
        var command = new SetCommand(key, value);
        var commandBytes = JSONObject.toJSONString(command).getBytes(StandardCharsets.UTF_8);
        indexLock.writeLock().lock();
        try {
            // 先保存数据到WAL中
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            index.put(key, command);

            // 内存表大小超过阈值进行持久化
            if (index.size() > storeThreshold) {
                switchIndex();
                storeToSstable();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * 保存数据到SSTable
     * @throws IOException
     */
    private void storeToSstable() throws IOException {
        // SSTable按照时间命名，这样可以保证名称递增
        var filePath = dataDir + System.currentTimeMillis() + TABLE;
        var ssTable = SsTable.createFromIndex(filePath, partSize, immutableIndex);
        ssTables.addFirst(ssTable);

        // 持久化完成，删除暂存的内存表和WAL_TMP
        immutableIndex = null;
        var tmpWal = new File(dataDir + WAL_TMP);
        if (tmpWal.exists()) {
            if (!tmpWal.delete()) {
                throw new RuntimeException("删除文件失败：walTmp");
            }
        }
    }

    /**
     * 切换内存表，新建一个内存表，暂存老内存表
     */
    private void switchIndex() {
        indexLock.writeLock().lock();
        // 切换内存表
        immutableIndex = index;
        index = new TreeMap<>();
        try {
            wal.close();
            // 切换内存表的同时，切换WAL
            var tmpWal = new File(dataDir + WAL_TMP);
            if (tmpWal.exists()) {
                if (!tmpWal.delete()) {
                    throw new RuntimeException("删除文件失败：walTmp");
                }
            }
            if (!walFile.renameTo(tmpWal)) {
                throw new RuntimeException("重命名文件失败：walTmp");
            }
            walFile = new File(dataDir + WAL);
            wal = new RandomAccessFile(walFile, RW_MODE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * 查询数据
     * @param key 数据键
     * @return 数据值
     */
    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            // 先从索引中获取
            var command = index.get(key);
            // 再尝试从不可变索引中取，此时可能处于持久化SSTable的过程中
            if (command == null && immutableIndex != null) {
                command = immutableIndex.get(key);
            }
            if (command == null) {
                // 索引中没有，尝试从SSTable中获取，从新的SSTable找到老的
                for (SsTable ssTable : ssTables) {
                    command = ssTable.query(key);
                    if (command != null) {
                        break;
                    }
                }
            }
            if (command instanceof SetCommand) {
                return ((SetCommand) command).getValue();
            }
            if (command instanceof RmCommand) {
                return null;
            }
            // 找不到说明不存在
            return null;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
    }

    /**
     * 删除数据
     *
     * @param key 数据键
     */
    @Override
    public void rm(String key) {
        try {
            // 先保存数据到WAL中
            indexLock.writeLock().lock();
            var rmCommand = new RmCommand(key);
            var commandBytes = JSONObject.toJSONString(rmCommand).getBytes(StandardCharsets.UTF_8);
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            index.put(key, rmCommand);

            // 内存表大小超过阈值进行持久化
            if (index.size() > storeThreshold) {
                switchIndex();
                storeToSstable();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        wal.close();
        for (SsTable ssTable : ssTables) {
            ssTable.close();
        }
    }
}
