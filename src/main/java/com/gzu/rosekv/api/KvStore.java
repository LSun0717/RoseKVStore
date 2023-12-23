package com.gzu.rosekv.api;

import java.io.Closeable;

/**
 * @Classname KvStore
 * @Description KV存储接口定义
 * @Version 1.0.0
 * @Date 12/23/2023 4:56 PM
 * @Created by LIONS7
 */
public interface KvStore extends Closeable {
    /**
     * 保存数据
     * @param key 数据键
     * @param value 数据值
     */
    void set(String key, String value);

    /**
     * 查询数据
     * @param key 数据键
     * @return 数据值
     */
    String get(String key);

    /**
     * 删除数据
     * @param key 数据键
     */
    void rm(String key);
}
