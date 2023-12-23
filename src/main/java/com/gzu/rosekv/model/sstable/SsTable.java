package com.gzu.rosekv.model.sstable;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.TypeReference;
import com.gzu.rosekv.model.Position;
import com.gzu.rosekv.model.command.Command;
import com.gzu.rosekv.model.command.RmCommand;
import com.gzu.rosekv.model.command.SetCommand;
import com.gzu.rosekv.util.ConvertUtil;
import com.gzu.rosekv.util.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.TreeMap;


/**
 * @Classname SsTable
 * @Description SSTable 具体实现
 * @Version 1.0.0
 * @Date 12/23/2023 2:30 PM
 * @Created by LIONS7
 */
public class SsTable implements Closeable {

    public static final String RW  = "rw";

    private final Logger LOGGER = LoggerFactory.getLogger(SsTable.class);

    /**
     * 元信息
     */
    private TableMetaInfo tableMetaInfo;

    /**
     * 字段稀疏索引，底层数据结构为红黑树
     */
    private TreeMap<String, Position> sparseIndex;

    /**
     * 文件句柄
     */
    private final RandomAccessFile tableFile;

    /**
     * 数据库文件路径
     */
    private final String filePath;

    /**
     *
     * @param filePath 表文件路径
     * @param partSize 数据分区大小
     */
    private SsTable(String filePath, int partSize) {
        this.tableMetaInfo = new TableMetaInfo();
        this.tableMetaInfo.setPartSize(partSize);
        this.filePath = filePath;
        try {
            this.tableFile = new RandomAccessFile(filePath, RW);
            tableFile.seek(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从MemTable构建SSTable
     * @param filePath 文件路径
     * @param partSize 数据分区大小
     * @param index 稀疏索引
     * @return SSTable obj
     * @throws IOException
     */
    public static SsTable createFromIndex(String filePath,
                                          int partSize,
                                          TreeMap<String, Command> index) throws IOException {
        SsTable ssTable = new SsTable(filePath, partSize);
        ssTable.doCreateFromIndex(index);
        return ssTable;
    }

    /**
     * 从文件中构建SSTable
     * @param filePath 文件路径
     * @return SsTable obj
     */
    public static SsTable createFromFile(String filePath) throws IOException {
        var ssTable = new SsTable(filePath, 0);
        ssTable.doCreateFromFile();
        return ssTable;
    }
// TODO
    public Command query(String key) throws IOException {
        var sparseKeyPositionList = new LinkedList<Position>();
        Position lastSmallPosition = null;
        Position firstBigPosition = null;

        // 从稀疏索引中找到最后一个小于key的位置，以及第一个大于key的位置
        for (String k : sparseIndex.keySet()) {
            if (k.compareTo(key) <= 0) {
                lastSmallPosition = sparseIndex.get(k);
            } else {
                firstBigPosition = sparseIndex.get(k);
                break;
            }
        }
        if (lastSmallPosition != null) {
            sparseKeyPositionList.add(lastSmallPosition);
        }
        if (firstBigPosition != null) {
            sparseKeyPositionList.add(firstBigPosition);
        }
        if (sparseKeyPositionList.isEmpty()) {
            return null;
        }

        LoggerUtil.debug(LOGGER,
                "[SsTable][restoreFromFile][sparseKeyPositionList]: {}",
                sparseKeyPositionList);
        var firstKeyPosition = sparseKeyPositionList.getFirst();
        var lastKeyPosition = sparseKeyPositionList.getLast();
        var start = 0L;
        var len = 0L;
        start = firstKeyPosition.getStart();
        if (firstKeyPosition.equals(lastKeyPosition)) {
            len = firstKeyPosition.getLen();
        } else {
            len = lastKeyPosition.getStart() + lastKeyPosition.getLen();
        }
        // key如果存在必定位于区间内，所以只需要读取区间内的数据，减少io
        var dataPart = new byte[(int) len];
        tableFile.seek(start);
        tableFile.read(dataPart);
        var pStart = 0;

        // 读取分区数据
        for (var position : sparseKeyPositionList) {
            var dataPartJson = JSONObject.parseObject(new String(dataPart, pStart, (int) position.getLen()));
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][dataPartJson]: {}", dataPartJson);
            if (dataPartJson.containsKey(key)) {
                var value = dataPartJson.getJSONObject(key);
                return ConvertUtil.jsonToCommand(value);
            }
            pStart += (int) position.getLen();
        }
        return null;
    }

    /**
     * 从MemTable转化为SSTable的具体实现
     * @param index 稀疏索引
     * @throws IOException
     */
    private void doCreateFromIndex(TreeMap<String, Command> index) throws IOException {
        var partData = new JSONObject();
        tableMetaInfo.setDataStart(tableFile.getFilePointer());
        for (var command : index.values()) {
            if (command instanceof SetCommand set) {
                partData.put(set.getKey(), set);
            }

            if (command instanceof RmCommand rm) {
                partData.put(rm.getKey(), rm);
            }

            if (partData.size() >= tableMetaInfo.getPartSize()) {
                writeDataPart(partData);
            }
        }
        // 遍历完之后如果有剩余的数据（尾部数据不一定到达分段条件）写入文件中
        if (partData.size() > 0) {
            writeDataPart(partData);
        }
        var dataPartLen = tableFile.getFilePointer() - tableMetaInfo.getDataStart();
        tableMetaInfo.setDataLen(dataPartLen);

        // 保存稀疏索引
        var indexBytes = JSONObject.toJSONString(sparseIndex).getBytes(StandardCharsets.UTF_8);
        tableMetaInfo.setIndexStart(tableFile.getFilePointer());
        tableFile.write(indexBytes);
        tableMetaInfo.setIndexLen(indexBytes.length);
        LoggerUtil.debug(LOGGER, "[SsTable][initFromIndex][sparseIndex]: {}", sparseIndex);

        // 保存元信息
        tableMetaInfo.writeToFile(tableFile);
        LoggerUtil.info(LOGGER,
                        "[SsTable][initFromIndex]: {},{}",
                        filePath,
                        tableMetaInfo);
    }

    /**
     * 将数据分区写入文件中
     * @param partData 分区数据
     * @throws IOException
     */
    private void writeDataPart(JSONObject partData) throws IOException {
        var partDataBytes = partData.toJSONString().getBytes(StandardCharsets.UTF_8);
        var start = tableFile.getFilePointer();
        tableFile.write(partDataBytes);

        // 记录数据段的第一个key到稀疏索引中
        var firstKey = partData.keySet().stream().findFirst();
        firstKey.ifPresent(s -> sparseIndex.put(s, new Position(start, partDataBytes.length)));
        partData.clear();
    }

    /**
     * 从文件中构建SSTable的具体实现
     * @throws IOException
     */
    private void doCreateFromFile() throws IOException {
        // 读取元信息
        var tableMetaInfo = TableMetaInfo.readFromFile(tableFile);
        LoggerUtil.debug(LOGGER,
                "[SsTable][restoreFromFile][tableMetaInfo]: {}",
                tableMetaInfo);

        // 读取稀疏索引
        var indexBytes = new byte[(int) tableMetaInfo.getIndexLen()];
        tableFile.seek(tableMetaInfo.getIndexStart());
        tableFile.read(indexBytes);
        var indexStr = new String(indexBytes, StandardCharsets.UTF_8);
        LoggerUtil.debug(LOGGER,
                "[SsTable][restoreFromFile][indexStr]: {}",
                indexStr);
        sparseIndex = JSONObject.parseObject(indexStr,
                new TypeReference<>() {
                });
        this.tableMetaInfo = tableMetaInfo;
        LoggerUtil.debug(LOGGER,
                "[SsTable][restoreFromFile][sparseIndex]: {}",
                sparseIndex);
    }

    @Override
    public void close() throws IOException {
        tableFile.close();
    }
}
