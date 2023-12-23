package com.gzu.rosekv.model.command;

import lombok.Getter;
import lombok.Setter;

/**
 * @Classname RmCommand
 * @Description TODO
 * @Version 1.0.0
 * @Date 12/23/2023 2:12 PM
 * @Created by LIONS7
 */
@Getter
@Setter
public class RmCommand extends AbstractCommand{

    /**
     * 数据key
     */
    private String key;

    public RmCommand(String key) {
        super(CommandTypeEnum.RM);
        this.key = key;
    }
}
