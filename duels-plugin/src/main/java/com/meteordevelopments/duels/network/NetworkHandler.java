package com.meteordevelopments.duels.network;

import com.meteordevelopments.duels.DuelsPlugin;
import com.meteordevelopments.duels.arena.ArenaImpl;
import com.meteordevelopments.duels.common.network.NetworkArena;
import com.meteordevelopments.duels.common.network.NetworkQueue;
import com.meteordevelopments.duels.config.Config;
import com.meteordevelopments.duels.util.Loadable;
import lombok.Getter;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config as RedissonConfig;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles network communication between servers for cross-server duels functionality
 */
public class NetworkHandler implements Loadable {

    private final DuelsPlugin plugin;
    private final Config config;
    
    @Getter
    private RedissonClient redisson;
    
    @Getter
    private boolean networkEnabled = false;
    
    @Getter
    private String serverName;
    
    // Cache for network data
    private final Map<String, NetworkArena> networkArenas = new ConcurrentHashMap<>();
    private final Map<String, List<NetworkQueue>> networkQueues = new ConcurrentHashMap<>();
    
    public NetworkHandler(DuelsPlugin plugin) {
        this.plugin = plugin;
        this.config = plugin.getConfiguration();
    }

    @Override
    public void handleLoad() {
        // Load network configuration
        serverName = config.getNetworkServerName();
        
        if (config.isNetworkEnabled()) {
            connectToNetwork();
            setupNetworkListeners();
            networkEnabled = true;
            plugin.info("Network support enabled for server: " + serverName);
        } else {
            plugin.info("Network support disabled - running in standalone mode");
        }
    }

    @Override
    public void handleUnload() {
        if (redisson != null && !redisson.isShutdown()) {
            redisson.shutdown();
            networkEnabled = false;
            plugin.info("Disconnected from network");
        }
    }

    private void connectToNetwork() {
        try {
            RedissonConfig redisConfig = new RedissonConfig();
            
            String redisUrl = "redis://";
            if (!config.getNetworkRedisPassword().isEmpty()) {
                redisUrl += ":" + config.getNetworkRedisPassword() + "@";
            }
            redisUrl += config.getNetworkRedisHost() + ":" + config.getNetworkRedisPort();
            redisUrl += "/" + config.getNetworkRedisDatabase();
            
            redisConfig.useSingleServer()
                .setAddress(redisUrl)
                .setConnectionPoolSize(10)
                .setConnectionMinimumIdleSize(2);

            redisson = Redisson.create(redisConfig);
            plugin.info("Connected to Redis network at " + config.getNetworkRedisHost() + ":" + config.getNetworkRedisPort());
        } catch (Exception e) {
            plugin.warn("Failed to connect to network: " + e.getMessage());
            networkEnabled = false;
        }
    }

    private void setupNetworkListeners() {
        if (!networkEnabled) return;

        // Listen for cross-server duel requests
        redisson.getTopic("duels:duel-request").addListener(Map.class, (channel, message) -> {
            handleDuelRequest(message);
        });

        // Listen for player transfers
        redisson.getTopic("duels:player-transfer").addListener(Map.class, (channel, message) -> {
            handlePlayerTransfer(message);
        });

        // Listen for arena updates
        redisson.getTopic("duels:arena-update").addListener(Map.class, (channel, message) -> {
            handleArenaUpdate(message);
        });

        // Listen for queue updates
        redisson.getTopic("duels:queue-update").addListener(Map.class, (channel, message) -> {
            handleQueueUpdate(message);
        });
    }

    // Arena Management
    public void syncArena(ArenaImpl arena) {
        if (!networkEnabled) return;

        NetworkArena networkArena = new NetworkArena(arena.getName(), serverName);
        networkArena.setAvailable(arena.isAvailable());
        networkArena.setDisabled(arena.isDisabled());
        
        // Store arena data
        String key = "duels:arenas:" + serverName + ":" + arena.getName();
        redisson.getBucket(key).setAsync(networkArena);
        
        // Notify other servers
        Map<String, Object> message = new HashMap<>();
        message.put("serverName", serverName);
        message.put("arenaName", arena.getName());
        message.put("action", "update");
        message.put("data", networkArena);
        
        redisson.getTopic("duels:arena-update").publishAsync(message);
    }

    public CompletableFuture<List<NetworkArena>> getAvailableArenas(String kitName) {
        if (!networkEnabled) return CompletableFuture.completedFuture(new ArrayList<>());

        return CompletableFuture.supplyAsync(() -> {
            List<NetworkArena> availableArenas = new ArrayList<>();
            
            try {
                // Get all arena keys
                for (String key : redisson.getKeys().getKeysByPattern("duels:arenas:*")) {
                    NetworkArena arena = redisson.<NetworkArena>getBucket(key).get();
                    if (arena != null && arena.isAvailable() && 
                        (arena.isBoundless() || arena.isBound(kitName))) {
                        availableArenas.add(arena);
                    }
                }
            } catch (Exception e) {
                plugin.warn("Failed to get available arenas: " + e.getMessage());
            }
            
            return availableArenas;
        });
    }

    // Queue Management
    public void addToNetworkQueue(Player player, String kitName, int bet) {
        if (!networkEnabled) return;

        NetworkQueue queueEntry = new NetworkQueue(
            player.getUniqueId(), 
            player.getName(), 
            serverName, 
            kitName, 
            bet
        );

        String queueKey = "duels:queue:" + queueEntry.getQueueKey();
        redisson.getList(queueKey).addAsync(queueEntry);

        // Notify other servers
        Map<String, Object> message = new HashMap<>();
        message.put("action", "join");
        message.put("player", queueEntry);
        
        redisson.getTopic("duels:queue-update").publishAsync(message);
    }

    public void removeFromNetworkQueue(Player player, String kitName, int bet) {
        if (!networkEnabled) return;

        String queueKey = "duels:queue:" + kitName + ":" + bet;
        List<NetworkQueue> queue = redisson.getList(queueKey);
        
        // Remove player from queue
        queue.removeIf(entry -> entry.getPlayerId().equals(player.getUniqueId()));

        // Notify other servers
        Map<String, Object> message = new HashMap<>();
        message.put("action", "leave");
        message.put("playerId", player.getUniqueId().toString());
        message.put("kitName", kitName);
        message.put("bet", bet);
        
        redisson.getTopic("duels:queue-update").publishAsync(message);
    }

    // Cross-server player lookup
    public CompletableFuture<Optional<String>> findPlayerServer(String playerName) {
        if (!networkEnabled) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                String key = "duels:players:" + playerName.toLowerCase();
                String server = redisson.<String>getBucket(key).get();
                return Optional.ofNullable(server);
            } catch (Exception e) {
                plugin.warn("Failed to find player server: " + e.getMessage());
                return Optional.empty();
            }
        });
    }

    public void updatePlayerLocation(Player player) {
        if (!networkEnabled) return;

        String key = "duels:players:" + player.getName().toLowerCase();
        redisson.getBucket(key).setAsync(serverName, 30, java.util.concurrent.TimeUnit.SECONDS);
    }

    // Request player transfer to this server
    public CompletableFuture<Boolean> requestPlayerTransfer(String playerName, String reason) {
        if (!networkEnabled) return CompletableFuture.completedFuture(false);

        return findPlayerServer(playerName).thenCompose(serverOpt -> {
            if (serverOpt.isEmpty()) {
                return CompletableFuture.completedFuture(false);
            }

            Map<String, Object> message = new HashMap<>();
            message.put("playerName", playerName);
            message.put("targetServer", serverName);
            message.put("reason", reason);
            message.put("timestamp", System.currentTimeMillis());

            redisson.getTopic("duels:player-transfer").publishAsync(message);
            return CompletableFuture.completedFuture(true);
        });
    }

    // Event Handlers
    private void handleDuelRequest(Map<String, Object> message) {
        // Handle incoming duel requests from other servers
        plugin.doSync(() -> {
            String targetPlayerName = (String) message.get("targetPlayer");
            Player target = Bukkit.getPlayer(targetPlayerName);
            
            if (target != null) {
                // Process the duel request for local player
                plugin.getLogger().info("Received cross-server duel request for " + targetPlayerName);
            }
        });
    }

    private void handlePlayerTransfer(Map<String, Object> message) {
        // This is handled by the Velocity plugin, but we can log it
        plugin.getLogger().info("Player transfer request: " + message);
    }

    private void handleArenaUpdate(Map<String, Object> message) {
        String arenaServer = (String) message.get("serverName");
        if (!serverName.equals(arenaServer)) {
            // Update our cache of remote arenas
            NetworkArena arenaData = (NetworkArena) message.get("data");
            if (arenaData != null) {
                networkArenas.put(arenaData.getFullName(), arenaData);
            }
        }
    }

    private void handleQueueUpdate(Map<String, Object> message) {
        String action = (String) message.get("action");
        
        if ("join".equals(action)) {
            // Check if we can match this player with someone in our queue
            NetworkQueue playerQueue = (NetworkQueue) message.get("player");
            if (playerQueue != null && !playerQueue.getServerName().equals(serverName)) {
                checkForCrossServerMatch(playerQueue);
            }
        }
    }

    private void checkForCrossServerMatch(NetworkQueue remotePlayer) {
        // Check if any local players in queue can match with this remote player
        plugin.doSync(() -> {
            // This would integrate with the existing QueueManager to find matches
            plugin.getLogger().info("Checking for cross-server match with player from " + remotePlayer.getServerName());
        });
    }

    public boolean isNetworkEnabled() {
        return networkEnabled;
    }

    public String getServerName() {
        return serverName;
    }
}