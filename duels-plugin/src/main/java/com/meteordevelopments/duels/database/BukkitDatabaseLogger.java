package com.meteordevelopments.duels.database;

import com.meteordevelopments.duels.common.database.DatabaseLogger;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.logging.Logger;

/**
 * Bukkit-side implementation of {@link DatabaseLogger} that bridges to the
 * plugin's {@link Logger} instance.
 */
public class BukkitDatabaseLogger implements DatabaseLogger {

    private final Logger logger;

    public BukkitDatabaseLogger(JavaPlugin plugin) {
        this.logger = plugin.getLogger();
    }

    @Override
    public void info(String message) {
        logger.info(message);
    }

    @Override
    public void warn(String message) {
        logger.warning(message);
    }

    @Override
    public void error(String message, Throwable throwable) {
        logger.severe(message);
        if (throwable != null) {
            logger.log(java.util.logging.Level.SEVERE, message, throwable);
        }
    }
}
