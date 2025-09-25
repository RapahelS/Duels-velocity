package com.meteordevelopments.duels.velocity.network;

import com.meteordevelopments.duels.velocity.DuelsVelocityPlugin;
import com.meteordevelopments.duels.velocity.data.RedisManager;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class PacketHandler {

    private final DuelsVelocityPlugin plugin;
    @Getter
    private final RedisManager redisManager;
    private final NetworkManager networkManager;

    public PacketHandler(DuelsVelocityPlugin plugin) {
        this.plugin = plugin;
        this.redisManager = plugin.getRedisManager();
        this.networkManager = plugin.getNetworkManager();
        
        setupPacketHandlers();
    }

    private void setupPacketHandlers() {
        // Handle arena data synchronization
        redisManager.subscribe("arena-update", this::handleArenaUpdate);
        redisManager.subscribe("kit-update", this::handleKitUpdate);
        redisManager.subscribe("queue-update", this::handleQueueUpdate);
        redisManager.subscribe("match-start", this::handleMatchStart);
        redisManager.subscribe("match-end", this::handleMatchEnd);
    }

    // Arena Management
    public CompletableFuture<Void> syncArenaData(String serverName, String arenaName, Map<String, Object> arenaData) {
        Map<String, Object> message = new HashMap<>();
        message.put("serverName", serverName);
        message.put("arenaName", arenaName);
        message.put("arenaData", arenaData);
        message.put("action", "update");
        message.put("timestamp", System.currentTimeMillis());

        redisManager.publish("arena-update", message);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> removeArena(String serverName, String arenaName) {
        Map<String, Object> message = new HashMap<>();
        message.put("serverName", serverName);
        message.put("arenaName", arenaName);
        message.put("action", "remove");
        message.put("timestamp", System.currentTimeMillis());

        redisManager.publish("arena-update", message);
        return CompletableFuture.completedFuture(null);
    }

    // Kit Management
    public CompletableFuture<Void> syncKitData(String kitName, Map<String, Object> kitData) {
        Map<String, Object> message = new HashMap<>();
        message.put("kitName", kitName);
        message.put("kitData", kitData);
        message.put("action", "update");
        message.put("timestamp", System.currentTimeMillis());

        redisManager.publish("kit-update", message);
        return CompletableFuture.completedFuture(null);
    }

    // Queue Management
    public CompletableFuture<Void> updateQueueStatus(UUID playerId, String kitName, String serverName, String action) {
        Map<String, Object> message = new HashMap<>();
        message.put("playerId", playerId.toString());
        message.put("kitName", kitName);
        message.put("serverName", serverName);
        message.put("action", action); // join, leave, match_found
        message.put("timestamp", System.currentTimeMillis());

        redisManager.publish("queue-update", message);
        return CompletableFuture.completedFuture(null);
    }

    // Match Management
    public CompletableFuture<Void> notifyMatchStart(String arenaName, String serverName, UUID[] players, String kitName) {
        Map<String, Object> message = new HashMap<>();
        message.put("arenaName", arenaName);
        message.put("serverName", serverName);
        message.put("players", java.util.Arrays.stream(players).map(UUID::toString).toArray());
        message.put("kitName", kitName);
        message.put("action", "start");
        message.put("timestamp", System.currentTimeMillis());

        redisManager.publish("match-start", message);
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> notifyMatchEnd(String arenaName, String serverName, UUID winner, UUID loser, String reason) {
        Map<String, Object> message = new HashMap<>();
        message.put("arenaName", arenaName);
        message.put("serverName", serverName);
        message.put("winner", winner != null ? winner.toString() : null);
        message.put("loser", loser != null ? loser.toString() : null);
        message.put("reason", reason);
        message.put("action", "end");
        message.put("timestamp", System.currentTimeMillis());

        redisManager.publish("match-end", message);
        return CompletableFuture.completedFuture(null);
    }

    // Player Transfer Request
    public CompletableFuture<Boolean> requestPlayerTransfer(UUID playerId, String targetServer, String reason) {
        Map<String, Object> message = new HashMap<>();
        message.put("playerId", playerId.toString());
        message.put("targetServer", targetServer);
        message.put("reason", reason);
        message.put("timestamp", System.currentTimeMillis());

        redisManager.publish("player-transfer", message);
        
        // The actual transfer is handled by NetworkManager
        return networkManager.transferPlayer(playerId, targetServer);
    }

    // Event Handlers
    private void handleArenaUpdate(String channel, Object message) {
        plugin.getLogger().debug("Arena update received: {}", message);
        // Arena updates are handled by individual server plugins
    }

    private void handleKitUpdate(String channel, Object message) {
        plugin.getLogger().debug("Kit update received: {}", message);
        // Kit updates are handled by individual server plugins
    }

    private void handleQueueUpdate(String channel, Object message) {
        plugin.getLogger().debug("Queue update received: {}", message);
        // Queue updates might trigger cross-server match making
        if (message instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) message;
            String action = (String) data.get("action");
            
            if ("join".equals(action)) {
                // Player joined queue - check for cross-server matches
                checkForCrossServerMatches();
            }
        }
    }

    private void handleMatchStart(String channel, Object message) {
        plugin.getLogger().debug("Match start received: {}", message);
        // Match start notifications for statistics/logging
    }

    private void handleMatchEnd(String channel, Object message) {
        plugin.getLogger().debug("Match end received: {}", message);
        // Match end notifications for statistics/logging
    }

    private void checkForCrossServerMatches() {
        // This is a placeholder for more sophisticated cross-server matchmaking
        // In a full implementation, this would query Redis for pending queue entries
        // and attempt to match players across different servers
        plugin.getLogger().debug("Checking for cross-server matches...");
    }
}