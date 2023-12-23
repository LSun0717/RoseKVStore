package com.gzu.rosekv.util;

import org.slf4j.Logger;

/**
 * @Classname LoggerUtil
 * @Description 日志工具类
 * @Version 1.0.0
 * @Date 12/23/2023 4:30 PM
 * @Created by LIONS7
 */
public class LoggerUtil {
    public static void debug(Logger logger, String format, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(format, args);
        }
    }

    public static void info(Logger logger, String format, Object... args) {
        if (logger.isInfoEnabled()) {
            logger.info(format, args);
        }
    }

    public static void error(Logger logger,
                             Throwable t,
                             String format,
                             Object... args) {
        if (logger.isErrorEnabled()) {
            logger.error(format, args, t);
        }
    }
}
