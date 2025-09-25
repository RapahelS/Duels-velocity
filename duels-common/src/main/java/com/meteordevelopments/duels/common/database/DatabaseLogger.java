package com.meteordevelopments.duels.common.database;

/**
 * Simple logger abstraction used by the shared database layer so it can
 * integrate with both Bukkit's {@code java.util.logging.Logger} and Velocity's
 * SLF4J {@code Logger} without pulling in additional dependencies.
 */
public interface DatabaseLogger {

    void info(String message);

    void warn(String message);

    void error(String message, Throwable throwable);
}
