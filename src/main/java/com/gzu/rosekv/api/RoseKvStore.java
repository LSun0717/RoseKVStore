package com.gzu.rosekv.api;

/**
 * @Classname RoseKvStore
 * @Description TODO
 * @Version 1.0.0
 * @Date 12/23/2023 4:59 PM
 * @Created by LIONS7
 */
public class RoseKvStore implements KvStore{
    /**
     * 保存数据
     *
     * @param key   数据键
     * @param value 数据值
     */
    @Override
    public void set(String key, String value) {

    }

    /**
     * 查询数据
     *
     * @param key 数据键
     * @return 数据值
     */
    @Override
    public String get(String key) {
        return null;
    }

    /**
     * 删除数据
     *
     * @param key 数据键
     */
    @Override
    public void rm(String key) {

    }
}
