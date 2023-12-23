package com.gzu.rosekv.model.sstable;

import lombok.Data;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @Classname TableMetaInfo
 * @Description SSTable元信息
 * @Version 1.0.0
 * @Date 12/23/2023 2:17 PM
 * @Created by LIONS7
 */
@Data
public class TableMetaInfo {

    /**
     * 版本号
     */
    private long version;

    /**
     * 数据区开始
     */
    private long dataStart;

    /**
     * 数据区长度
     */
    private long dataLen;

    /**
     * 索引区开始
     */
    private long indexStart;

    /**
     * 索引去长度
     */
    private long indexLen;

    /**
     * 分段大小
     */
    private long partSize;

    /**
     * 元信息写入文件中，倒序写入
     * @param file SSTable文件
     * @throws IOException
     */
    public void writeToFile(RandomAccessFile file) throws IOException {
       file.writeLong(partSize);
       file.writeLong(dataStart);
       file.writeLong(dataLen);
       file.writeLong(indexStart);
       file.writeLong(indexLen);
       file.writeLong(version);
    }

    /**
     * 从文件中读取元信息，按照写入的顺序倒着读出来，Long定长8个字节
     * @param file 随机存取文件
     * @return TableMetaInfo obj
     * @throws IOException
     */
    public static TableMetaInfo readFromFile(RandomAccessFile file) throws IOException {
        TableMetaInfo tableMetaInfo = new TableMetaInfo();
        long fileLen = file.length();

        file.seek(fileLen - 8);
        tableMetaInfo.setVersion(file.readLong());

        file.seek((fileLen - 8 * 2));
        tableMetaInfo.setIndexLen(file.readLong());

        file.seek(fileLen - 8 * 3);
        tableMetaInfo.setIndexStart(file.readLong());

        file.seek(fileLen - 8 * 4);
        tableMetaInfo.setDataStart(file.readLong());

        file.seek(fileLen - 8 * 5);
        tableMetaInfo.setDataLen(file.readLong());

        file.seek(fileLen - 8 * 6);
        tableMetaInfo.setPartSize(file.readLong());

        return tableMetaInfo;
    }


}
