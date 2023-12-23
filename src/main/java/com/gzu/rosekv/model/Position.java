package com.gzu.rosekv.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Classname Position
 * @Description 位置
 * @Version 1.0.0
 * @Date 12/23/2023 2:41 PM
 * @Created by LIONS7
 */
@Data
@AllArgsConstructor
public class Position {

    /**
     * 开始
     */
    private long start;

    /**
     * 长度
     */
    private long len;
}
