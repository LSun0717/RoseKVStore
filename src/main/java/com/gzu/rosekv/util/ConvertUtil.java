package com.gzu.rosekv.util;

import com.alibaba.fastjson2.JSONObject;
import com.gzu.rosekv.model.command.Command;
import com.gzu.rosekv.model.command.CommandTypeEnum;
import com.gzu.rosekv.model.command.RmCommand;
import com.gzu.rosekv.model.command.SetCommand;

/**
 * @Classname ConvertUtil
 * @Description 反序列化工具
 * @Version 1.0.0
 * @Date 12/23/2023 5:30 PM
 * @Created by LIONS7
 */
public class ConvertUtil {
    public static final String COMMAND_TYPE = "type";

    /**
     * 将json字符串序列化为Command对象
     * @param value json str
     * @return Command obj
     */
    public static Command jsonToCommand(JSONObject value) {
        if (value.getString(COMMAND_TYPE).equals(CommandTypeEnum.SET.name())) {
            return value.toJavaObject(SetCommand.class);
        } else if (value.getString(COMMAND_TYPE).equals(CommandTypeEnum.RM.name())) {
            return value.toJavaObject(RmCommand.class);
        }
        return null;
    }
}
