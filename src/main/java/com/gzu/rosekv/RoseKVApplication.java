package com.gzu.rosekv;

import java.util.LinkedList;

/**
 * @Classname RoseKVApplication
 * @Description TODO
 * @Version 1.0.0
 * @Date 12/23/2023 5:16 PM
 * @Created by LIONS7
 */
public class RoseKVApplication {
    public static void main(String[] args) {
        var list = new LinkedList<String>();
        for (String s : list) {
            System.out.println(s.getClass());
        }
    }
}
