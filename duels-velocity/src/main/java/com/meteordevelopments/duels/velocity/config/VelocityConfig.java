package com.meteordevelopments.duels.velocity.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.meteordevelopments.duels.common.database.DatabaseSettings;
import com.meteordevelopments.duels.velocity.DuelsVelocityPlugin;
import lombok.Getter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class VelocityConfig {

    private final DuelsVelocityPlugin plugin;
    private final Path configFile;
    private final ObjectMapper mapper = new ObjectMapper();
    
    @Getter
    private JsonNode config;

    // Database configuration
    @Getter
    private String databaseHost = "localhost";
    @Getter
    private int databasePort = 3306;
    @Getter
    private String databaseName = "duels";
    @Getter
    private String databaseUsername = "duels";
    @Getter
    private String databasePassword = "";
    @Getter
    private boolean databaseUseSsl = false;
    @Getter
    private int databaseMaximumPoolSize = 10;
    @Getter
    private int databaseMinimumIdle = 2;
    @Getter
    private long databaseConnectionTimeoutMs = 10_000L;
    @Getter
    private long databaseIdleTimeoutMs = 600_000L;

    // Server configuration
    @Getter
    private List<String> duelServers = new ArrayList<>();
    @Getter
    private String defaultDuelServer = "";

    // Network settings
    @Getter
    private boolean debugMode = false;
    @Getter
    private int networkTimeout = 30;

    public VelocityConfig(DuelsVelocityPlugin plugin) {
        this.plugin = plugin;
        this.configFile = plugin.getDataDirectory().resolve("config.json");
    }

    public void load() {
        try {
            // Create data directory if it doesn't exist
            Files.createDirectories(plugin.getDataDirectory());

            // Create default config if it doesn't exist
            if (!Files.exists(configFile)) {
                createDefaultConfig();
            }

            // Load configuration
            config = mapper.readTree(Files.readAllBytes(configFile));
            loadValues();

            plugin.getLogger().info("Configuration loaded successfully!");
        } catch (IOException e) {
            plugin.getLogger().error("Failed to load configuration", e);
        }
    }

    private void createDefaultConfig() throws IOException {
        ObjectNode defaultConfig = mapper.createObjectNode();

    // Database configuration
    ObjectNode database = mapper.createObjectNode();
    database.put("host", databaseHost);
    database.put("port", databasePort);
    database.put("name", databaseName);
    database.put("username", databaseUsername);
    database.put("password", databasePassword);
    database.put("use-ssl", databaseUseSsl);

    ObjectNode pool = mapper.createObjectNode();
    pool.put("maximum-pool-size", databaseMaximumPoolSize);
    pool.put("minimum-idle", databaseMinimumIdle);
    pool.put("connection-timeout-ms", databaseConnectionTimeoutMs);
    pool.put("idle-timeout-ms", databaseIdleTimeoutMs);
    database.set("pool", pool);

    defaultConfig.set("database", database);

        // Server configuration
        ObjectNode servers = mapper.createObjectNode();
        List<String> defaultServers = List.of("duel1", "duel2");
        servers.set("duel-servers", mapper.valueToTree(defaultServers));
        servers.put("default-duel-server", "duel1");
        defaultConfig.set("servers", servers);

        // Network settings
        ObjectNode network = mapper.createObjectNode();
        network.put("debug-mode", debugMode);
        network.put("timeout-seconds", networkTimeout);
        defaultConfig.set("network", network);

        // Write default configuration
        Files.write(configFile, mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(defaultConfig));
        plugin.getLogger().info("Created default configuration file");
    }

    private void loadValues() {
        // Load database configuration
        JsonNode database = config.get("database");
        if (database != null) {
            databaseHost = database.path("host").asText(databaseHost);
            databasePort = database.path("port").asInt(databasePort);
            databaseName = database.path("name").asText(databaseName);
            databaseUsername = database.path("username").asText(databaseUsername);
            databasePassword = database.path("password").asText(databasePassword);
            databaseUseSsl = database.path("use-ssl").asBoolean(databaseUseSsl);

            JsonNode pool = database.get("pool");
            if (pool != null && pool.isObject()) {
                databaseMaximumPoolSize = pool.path("maximum-pool-size").asInt(databaseMaximumPoolSize);
                databaseMinimumIdle = pool.path("minimum-idle").asInt(databaseMinimumIdle);
                databaseConnectionTimeoutMs = pool.path("connection-timeout-ms").asLong(databaseConnectionTimeoutMs);
                databaseIdleTimeoutMs = pool.path("idle-timeout-ms").asLong(databaseIdleTimeoutMs);
            }
        }

        // Load server configuration
        JsonNode servers = config.get("servers");
        if (servers != null) {
            JsonNode duelServersNode = servers.get("duel-servers");
            if (duelServersNode != null && duelServersNode.isArray()) {
                duelServers.clear();
                duelServersNode.forEach(node -> duelServers.add(node.asText()));
            }
            defaultDuelServer = servers.path("default-duel-server").asText(defaultDuelServer);
        }

        // Load network settings
        JsonNode network = config.get("network");
        if (network != null) {
            debugMode = network.path("debug-mode").asBoolean(debugMode);
            networkTimeout = network.path("timeout-seconds").asInt(networkTimeout);
        }
    }

    public boolean isDuelServer(String serverName) {
        return duelServers.contains(serverName);
    }

    public DatabaseSettings buildDatabaseSettings() {
        return DatabaseSettings.builder()
            .host(databaseHost)
            .port(databasePort)
            .database(databaseName)
            .username(databaseUsername)
            .password(databasePassword)
            .useSsl(databaseUseSsl)
            .maximumPoolSize(databaseMaximumPoolSize)
            .minimumIdle(databaseMinimumIdle)
            .connectionTimeoutMs(databaseConnectionTimeoutMs)
            .idleTimeoutMs(databaseIdleTimeoutMs)
            .build();
    }
}