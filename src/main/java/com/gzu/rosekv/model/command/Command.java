package com.gzu.rosekv.model.command;

/**
 * @Classname Command
 * @Description 命令接口
 * @Version 1.0.0
 * @Date 12/23/2023 2:07 PM
 * @Created by LIONS7
 */
public interface Command {

    /**
     * 获取数据Key
     * @return key
     */
    String getKey();
}
