package com.meteordevelopments.duels.common.database;

import lombok.Builder;
import lombok.Value;

/**
 * Configuration holder for SQL connection parameters shared by the Velocity and
 * Bukkit plugins.
 */
@Value
@Builder
public class DatabaseSettings {
    String host;
    int port;
    String database;
    String username;
    String password;
    boolean useSsl;
    int maximumPoolSize;
    int minimumIdle;
    long connectionTimeoutMs;
    long idleTimeoutMs;
}
