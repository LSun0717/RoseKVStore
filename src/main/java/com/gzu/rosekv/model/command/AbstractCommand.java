package com.gzu.rosekv.model.command;

import com.alibaba.fastjson2.JSON;
import lombok.Getter;
import lombok.Setter;

/**
 * @Classname AbstractCommand
 * @Description TODO
 * @Version 1.0.0
 * @Date 12/23/2023 2:10 PM
 * @Created by LIONS7
 */
@Getter
@Setter
public abstract class AbstractCommand implements Command{

    /**
     * 命令类型
     */
    private CommandTypeEnum commandType;

    public AbstractCommand(CommandTypeEnum commandType) {
        this.commandType = commandType;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
