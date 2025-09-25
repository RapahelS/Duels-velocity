package com.meteordevelopments.duels.velocity.network;

import com.meteordevelopments.duels.velocity.DuelsVelocityPlugin;
import com.meteordevelopments.duels.velocity.data.RedisManager;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import lombok.Getter;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkManager {

    private final DuelsVelocityPlugin plugin;
    @Getter
    private final RedisManager redisManager;

    // Cache for online players across servers
    private final Map<UUID, String> playerServerMap = new ConcurrentHashMap<>();
    // Cache for server player counts
    private final Map<String, Integer> serverPlayerCounts = new ConcurrentHashMap<>();

    public NetworkManager(DuelsVelocityPlugin plugin) {
        this.plugin = plugin;
        this.redisManager = plugin.getRedisManager();
        
        // Subscribe to network events
        setupRedisSubscriptions();
        
        // Update player locations periodically
        startPlayerLocationUpdater();
    }

    private void setupRedisSubscriptions() {
        redisManager.subscribe("player-join", (channel, message) -> {
            if (message instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) message;
                UUID playerId = UUID.fromString((String) data.get("playerId"));
                String serverName = (String) data.get("serverName");
                playerServerMap.put(playerId, serverName);
            }
        });

        redisManager.subscribe("player-leave", (channel, message) -> {
            if (message instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) message;
                UUID playerId = UUID.fromString((String) data.get("playerId"));
                playerServerMap.remove(playerId);
            }
        });

        redisManager.subscribe("duel-request", this::handleDuelRequest);
        redisManager.subscribe("player-transfer", this::handlePlayerTransfer);
    }

    private void startPlayerLocationUpdater() {
        plugin.getServer().getScheduler()
            .buildTask(plugin, () -> updatePlayerLocations())
            .repeat(java.time.Duration.ofSeconds(30))
            .schedule();
    }

    private void updatePlayerLocations() {
        for (Player player : plugin.getServer().getAllPlayers()) {
            player.getCurrentServer().ifPresent(server -> {
                playerServerMap.put(player.getUniqueId(), server.getServerInfo().getName());
            });
        }

        // Update server player counts
        for (RegisteredServer server : plugin.getServer().getAllServers()) {
            serverPlayerCounts.put(server.getServerInfo().getName(), server.getPlayersConnected().size());
        }
    }

    public Optional<String> getPlayerServer(UUID playerId) {
        return Optional.ofNullable(playerServerMap.get(playerId));
    }

    public Optional<String> getPlayerServer(String playerName) {
        return plugin.getServer().getPlayer(playerName)
            .flatMap(player -> player.getCurrentServer())
            .map(server -> server.getServerInfo().getName());
    }

    public CompletableFuture<Boolean> transferPlayer(UUID playerId, String targetServer) {
        Optional<Player> playerOpt = plugin.getServer().getPlayer(playerId);
        if (playerOpt.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }

        Player player = playerOpt.get();
        Optional<RegisteredServer> serverOpt = plugin.getServer().getServer(targetServer);
        if (serverOpt.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }

        return player.createConnectionRequest(serverOpt.get()).connect()
            .thenApply(result -> {
                if (result.isSuccessful()) {
                    playerServerMap.put(playerId, targetServer);
                    plugin.getLogger().info("Successfully transferred player {} to server {}", 
                        player.getUsername(), targetServer);
                    return true;
                } else {
                    plugin.getLogger().warn("Failed to transfer player {} to server {}: {}", 
                        player.getUsername(), targetServer, result.getReasonComponent());
                    return false;
                }
            });
    }

    public String getBestDuelServer() {
        // Find the duel server with the least players
        return plugin.getConfig().getDuelServers().stream()
            .min(Comparator.comparingInt(server -> 
                serverPlayerCounts.getOrDefault(server, Integer.MAX_VALUE)))
            .orElse(plugin.getConfig().getDefaultDuelServer());
    }

    public CompletableFuture<Void> broadcastDuelRequest(UUID senderId, UUID targetId, String duelData) {
        Map<String, Object> message = new HashMap<>();
        message.put("senderId", senderId.toString());
        message.put("targetId", targetId.toString());
        message.put("duelData", duelData);
        message.put("timestamp", System.currentTimeMillis());

        redisManager.publish("duel-request", message);
        return CompletableFuture.completedFuture(null);
    }

    private void handleDuelRequest(String channel, Object message) {
        // This will be handled by the Spigot plugin side
        plugin.getLogger().debug("Received duel request: {}", message);
    }

    private void handlePlayerTransfer(String channel, Object message) {
        if (message instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) message;
            UUID playerId = UUID.fromString((String) data.get("playerId"));
            String targetServer = (String) data.get("targetServer");
            
            transferPlayer(playerId, targetServer);
        }
    }

    public List<String> getOnlinePlayersOnServer(String serverName) {
        return plugin.getServer().getServer(serverName)
            .map(server -> server.getPlayersConnected().stream()
                .map(Player::getUsername)
                .toList())
            .orElse(Collections.emptyList());
    }

    public boolean isPlayerOnline(UUID playerId) {
        return plugin.getServer().getPlayer(playerId).isPresent();
    }

    public boolean isPlayerOnline(String playerName) {
        return plugin.getServer().getPlayer(playerName).isPresent();
    }
}