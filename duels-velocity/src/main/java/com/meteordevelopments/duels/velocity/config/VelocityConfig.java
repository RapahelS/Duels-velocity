package com.meteordevelopments.duels.velocity.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    // Redis configuration
    @Getter
    private String redisHost = "localhost";
    @Getter
    private int redisPort = 6379;
    @Getter
    private String redisPassword = "";
    @Getter
    private int redisDatabase = 0;
    @Getter
    private String redisPrefix = "duels:";

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

        // Redis configuration
        ObjectNode redis = mapper.createObjectNode();
        redis.put("host", redisHost);
        redis.put("port", redisPort);
        redis.put("password", redisPassword);
        redis.put("database", redisDatabase);
        redis.put("prefix", redisPrefix);
        defaultConfig.set("redis", redis);

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
        // Load Redis configuration
        JsonNode redis = config.get("redis");
        if (redis != null) {
            redisHost = redis.path("host").asText(redisHost);
            redisPort = redis.path("port").asInt(redisPort);
            redisPassword = redis.path("password").asText(redisPassword);
            redisDatabase = redis.path("database").asInt(redisDatabase);
            redisPrefix = redis.path("prefix").asText(redisPrefix);
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
}