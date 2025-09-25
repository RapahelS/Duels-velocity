package com.meteordevelopments.duels.util.database;

import com.meteordevelopments.duels.DuelsPlugin;
import com.meteordevelopments.duels.common.database.DatabaseLogger;

public class BukkitDatabaseLogger implements DatabaseLogger {

    private final DuelsPlugin plugin;

    public BukkitDatabaseLogger(DuelsPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public void info(String message) {
        plugin.info(message);
    }

    @Override
    public void warn(String message) {
        plugin.warn(message);
    }

    @Override
    public void error(String message, Throwable throwable) {
        plugin.error(message, throwable);
    }
}
