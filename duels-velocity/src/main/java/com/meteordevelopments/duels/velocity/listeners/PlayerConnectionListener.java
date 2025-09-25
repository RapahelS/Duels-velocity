package com.meteordevelopments.duels.velocity.listeners;

import com.meteordevelopments.duels.velocity.DuelsVelocityPlugin;
import com.meteordevelopments.duels.velocity.data.RedisManager;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.DisconnectEvent;
import com.velocitypowered.api.event.player.ServerConnectedEvent;
import com.velocitypowered.api.proxy.Player;

import java.util.HashMap;
import java.util.Map;

public class PlayerConnectionListener {

    private final DuelsVelocityPlugin plugin;
    private final RedisManager redisManager;

    public PlayerConnectionListener(DuelsVelocityPlugin plugin) {
        this.plugin = plugin;
        this.redisManager = plugin.getRedisManager();
    }

    @Subscribe
    public void onPlayerServerConnect(ServerConnectedEvent event) {
        Player player = event.getPlayer();
        String serverName = event.getServer().getServerInfo().getName();

        // Notify all servers about player joining a specific server
        Map<String, Object> data = new HashMap<>();
        data.put("playerId", player.getUniqueId().toString());
        data.put("playerName", player.getUsername());
        data.put("serverName", serverName);
        data.put("timestamp", System.currentTimeMillis());

        redisManager.publish("player-join", data);

        if (plugin.getConfig().isDebugMode()) {
            plugin.getLogger().info("Player {} connected to server {}", 
                player.getUsername(), serverName);
        }
    }

    @Subscribe
    public void onPlayerDisconnect(DisconnectEvent event) {
        Player player = event.getPlayer();

        // Notify all servers about player leaving the network
        Map<String, Object> data = new HashMap<>();
        data.put("playerId", player.getUniqueId().toString());
        data.put("playerName", player.getUsername());
        data.put("timestamp", System.currentTimeMillis());

        redisManager.publish("player-leave", data);

        if (plugin.getConfig().isDebugMode()) {
            plugin.getLogger().info("Player {} disconnected from the network", 
                player.getUsername());
        }
    }
}