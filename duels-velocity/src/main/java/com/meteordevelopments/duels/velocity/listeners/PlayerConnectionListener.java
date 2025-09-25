package com.meteordevelopments.duels.velocity.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.meteordevelopments.duels.common.database.DatabaseManager;
import com.meteordevelopments.duels.velocity.DuelsVelocityPlugin;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.DisconnectEvent;
import com.velocitypowered.api.event.player.ServerConnectedEvent;
import com.velocitypowered.api.proxy.Player;

import java.time.Instant;

public class PlayerConnectionListener {

    private final DuelsVelocityPlugin plugin;
    private final DatabaseManager databaseManager;
    private final ObjectMapper mapper = new ObjectMapper();

    public PlayerConnectionListener(DuelsVelocityPlugin plugin) {
        this.plugin = plugin;
        this.databaseManager = plugin.getDatabaseManager();
    }

    @Subscribe
    public void onPlayerServerConnect(ServerConnectedEvent event) {
        Player player = event.getPlayer();
        String serverName = event.getServer().getServerInfo().getName();

        if (databaseManager == null || !databaseManager.isConnected()) {
            return;
        }

        long now = Instant.now().toEpochMilli();
        databaseManager.upsertPlayer(player.getUniqueId(), player.getUsername(), serverName, now);

        ObjectNode payload = mapper.createObjectNode();
        payload.put("playerId", player.getUniqueId().toString());
        payload.put("playerName", player.getUsername());
        payload.put("serverName", serverName);
        payload.put("timestamp", now);
        databaseManager.publishEvent("duels:player-join", payload);

        if (plugin.getConfig().isDebugMode()) {
            plugin.getLogger().info("Player {} connected to server {}", 
                player.getUsername(), serverName);
        }
    }

    @Subscribe
    public void onPlayerDisconnect(DisconnectEvent event) {
        Player player = event.getPlayer();

        if (databaseManager == null || !databaseManager.isConnected()) {
            return;
        }

        databaseManager.removePlayer(player.getUniqueId());

        ObjectNode payload = mapper.createObjectNode();
        payload.put("playerId", player.getUniqueId().toString());
        payload.put("playerName", player.getUsername());
        payload.put("timestamp", Instant.now().toEpochMilli());
        databaseManager.publishEvent("duels:player-leave", payload);

        if (plugin.getConfig().isDebugMode()) {
            plugin.getLogger().info("Player {} disconnected from the network", 
                player.getUsername());
        }
    }
}