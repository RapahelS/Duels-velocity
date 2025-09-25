package com.meteordevelopments.duels.velocity.util;

import com.meteordevelopments.duels.common.database.DatabaseLogger;
import org.slf4j.Logger;

public class VelocityDatabaseLogger implements DatabaseLogger {

    private final Logger logger;

    public VelocityDatabaseLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void info(String message) {
        logger.info(message);
    }

    @Override
    public void warn(String message) {
        logger.warn(message);
    }

    @Override
    public void error(String message, Throwable throwable) {
        logger.error(message, throwable);
    }
}
