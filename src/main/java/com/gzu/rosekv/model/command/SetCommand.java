package com.gzu.rosekv.model.command;

import lombok.Getter;
import lombok.Setter;

/**
 * @Classname SetCommand
 * @Description TODO
 * @Version 1.0.0
 * @Date 12/23/2023 2:14 PM
 * @Created by LIONS7
 */
@Getter
@Setter
public class SetCommand extends AbstractCommand{
    /**
     * 数据key
     */
    private String key;

    /**
     * 数据值
     */
    private String value;

    public SetCommand(String key, String value) {
        super(CommandTypeEnum.SET);
        this.key = key;
        this.value = value;
    }
}
