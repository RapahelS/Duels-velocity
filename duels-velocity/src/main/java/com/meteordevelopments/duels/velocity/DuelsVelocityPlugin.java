package com.meteordevelopments.duels.velocity;

import com.google.inject.Inject;
import com.meteordevelopments.duels.common.database.DatabaseManager;
import com.meteordevelopments.duels.common.database.DatabaseSettings;
import com.meteordevelopments.duels.velocity.config.VelocityConfig;
import com.meteordevelopments.duels.velocity.listeners.PlayerConnectionListener;
import com.meteordevelopments.duels.velocity.network.NetworkManager;
import com.meteordevelopments.duels.velocity.network.PacketHandler;
import com.meteordevelopments.duels.velocity.util.VelocityDatabaseLogger;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.ProxyServer;
import lombok.Getter;
import org.slf4j.Logger;

import java.nio.file.Path;

@Plugin(
    id = "duels-velocity",
    name = "Duels Velocity",
    version = "@VERSION@",
    description = "Velocity proxy plugin for cross-server Duels support",
    authors = {"Realized", "DUMBO", "RaphaelS"}
)
public class DuelsVelocityPlugin {

    @Getter
    private final ProxyServer server;
    @Getter
    private final Logger logger;
    @Getter
    private final Path dataDirectory;

    @Getter
    private VelocityConfig config;
    @Getter
    private DatabaseManager databaseManager;
    @Getter
    private NetworkManager networkManager;
    @Getter
    private PacketHandler packetHandler;

    @Inject
    public DuelsVelocityPlugin(ProxyServer server, Logger logger, @DataDirectory Path dataDirectory) {
        this.server = server;
        this.logger = logger;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public void onProxyInitialization(ProxyInitializeEvent event) {
        logger.info("Initializing Duels Velocity Plugin...");

        // Load configuration
        config = new VelocityConfig(this);
        config.load();

        // Initialize database connection
        DatabaseSettings databaseSettings = config.buildDatabaseSettings();
        databaseManager = new DatabaseManager(databaseSettings, new VelocityDatabaseLogger(logger));
        try {
            databaseManager.connect();
        } catch (Exception exception) {
            logger.error("Failed to connect to the database. Network features will be disabled.", exception);
        }

        // Initialize network manager
        networkManager = new NetworkManager(this);
        
        // Initialize packet handler
        packetHandler = new PacketHandler(this);

        // Register listeners
        server.getEventManager().register(this, new PlayerConnectionListener(this));

        logger.info("Duels Velocity Plugin enabled successfully!");
    }

    @Subscribe
    public void onProxyShutdown(ProxyShutdownEvent event) {
        logger.info("Shutting down Duels Velocity Plugin...");

        if (databaseManager != null) {
            databaseManager.disconnect();
        }

        logger.info("Duels Velocity Plugin disabled.");
    }
}