package com.meteordevelopments.duels.velocity.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.meteordevelopments.duels.common.database.DatabaseManager;
import com.meteordevelopments.duels.common.database.NetworkEvent;
import com.meteordevelopments.duels.common.network.NetworkQueue;
import com.meteordevelopments.duels.velocity.DuelsVelocityPlugin;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import lombok.Getter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkManager {

    private static final String CHANNEL_PLAYER_JOIN = "duels:player-join";
    private static final String CHANNEL_PLAYER_LEAVE = "duels:player-leave";
    private static final String CHANNEL_PLAYER_TRANSFER = "duels:player-transfer";
    private static final String CHANNEL_DUEL_REQUEST = "duels:duel-request";
    private static final String CHANNEL_QUEUE_UPDATE = "duels:queue-update";

    private final DuelsVelocityPlugin plugin;
    @Getter
    private final DatabaseManager databaseManager;
    private final ObjectMapper mapper = new ObjectMapper();

    private final Map<UUID, String> playerServerMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> serverPlayerCounts = new ConcurrentHashMap<>();
    private final Map<UUID, Long> processedDuelRequests = new ConcurrentHashMap<>();
    private final Map<String, List<NetworkQueue>> queueEntries = new ConcurrentHashMap<>();
    private final Set<UUID> playersPendingMatch = ConcurrentHashMap.newKeySet();

    private volatile long lastPlayerJoinEventId = 0L;
    private volatile long lastPlayerLeaveEventId = 0L;
    private volatile long lastPlayerTransferEventId = 0L;
    private volatile long lastDuelRequestEventId = 0L;
    private volatile long lastQueueEventId = 0L;

    public NetworkManager(DuelsVelocityPlugin plugin) {
        this.plugin = plugin;
        this.databaseManager = plugin.getDatabaseManager();

        initializeCaches();
        startPlayerLocationUpdater();
        startEventPollers();
    }

    private void initializeCaches() {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.loadPlayerServerMap()
            .thenAccept(playerServerMap::putAll)
            .exceptionally(throwable -> {
                plugin.getLogger().error("Failed to load player cache from database", throwable);
                return null;
            });

        databaseManager.loadServerPlayerCounts()
            .thenAccept(serverPlayerCounts::putAll)
            .exceptionally(throwable -> {
                plugin.getLogger().error("Failed to load server counts from database", throwable);
                return null;
            });

        databaseManager.loadAllQueueEntries()
            .thenAccept(entries -> {
                entries.forEach(this::cacheQueueEntry);
                new HashSet<>(queueEntries.keySet()).forEach(this::attemptMatchmaking);
            })
            .exceptionally(throwable -> {
                plugin.getLogger().error("Failed to load queue cache from database", throwable);
                return null;
            });
    }

    private void startPlayerLocationUpdater() {
        plugin.getServer().getScheduler()
            .buildTask(plugin, this::updatePlayerLocations)
            .repeat(Duration.ofSeconds(30))
            .schedule();
    }

    private void startEventPollers() {
        plugin.getServer().getScheduler()
            .buildTask(plugin, this::pollPlayerEvents)
            .repeat(Duration.ofSeconds(2))
            .schedule();

        plugin.getServer().getScheduler()
            .buildTask(plugin, this::pollTransferEvents)
            .repeat(Duration.ofSeconds(2))
            .schedule();

        plugin.getServer().getScheduler()
            .buildTask(plugin, this::pollDuelRequests)
            .repeat(Duration.ofSeconds(5))
            .schedule();

        plugin.getServer().getScheduler()
            .buildTask(plugin, this::pollQueueEvents)
            .repeat(Duration.ofSeconds(2))
            .schedule();

        plugin.getServer().getScheduler()
            .buildTask(plugin, this::cleanupOldEvents)
            .repeat(Duration.ofMinutes(5))
            .schedule();
    }

    private void updatePlayerLocations() {
        for (Player player : plugin.getServer().getAllPlayers()) {
            player.getCurrentServer().ifPresent(server ->
                playerServerMap.put(player.getUniqueId(), server.getServerInfo().getName())
            );
        }

        for (RegisteredServer server : plugin.getServer().getAllServers()) {
            serverPlayerCounts.put(server.getServerInfo().getName(), server.getPlayersConnected().size());
        }
    }

    private void pollPlayerEvents() {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.fetchEvents(CHANNEL_PLAYER_JOIN, lastPlayerJoinEventId, 64)
            .thenAccept(this::handlePlayerJoinEvents)
            .exceptionally(throwable -> {
                plugin.getLogger().error("Failed to fetch player join events", throwable);
                return null;
            });

        databaseManager.fetchEvents(CHANNEL_PLAYER_LEAVE, lastPlayerLeaveEventId, 64)
            .thenAccept(this::handlePlayerLeaveEvents)
            .exceptionally(throwable -> {
                plugin.getLogger().error("Failed to fetch player leave events", throwable);
                return null;
            });
    }

    private void pollTransferEvents() {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.fetchEvents(CHANNEL_PLAYER_TRANSFER, lastPlayerTransferEventId, 32)
            .thenAccept(this::handlePlayerTransferEvents)
            .exceptionally(throwable -> {
                plugin.getLogger().error("Failed to fetch player transfer events", throwable);
                return null;
            });
    }

    private void pollDuelRequests() {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.fetchEvents(CHANNEL_DUEL_REQUEST, lastDuelRequestEventId, 32)
            .thenAccept(this::handleDuelRequestEvents)
            .exceptionally(throwable -> {
                plugin.getLogger().error("Failed to fetch duel request events", throwable);
                return null;
            });
    }

    private void pollQueueEvents() {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.fetchEvents(CHANNEL_QUEUE_UPDATE, lastQueueEventId, 64)
            .thenAccept(this::handleQueueEvents)
            .exceptionally(throwable -> {
                plugin.getLogger().error("Failed to fetch queue events", throwable);
                return null;
            });
    }

    private void cleanupOldEvents() {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.cleanupOldEvents(Duration.ofMinutes(10).toMillis())
            .exceptionally(throwable -> {
                plugin.getLogger().warn("Failed to prune old network events", throwable);
                return null;
            });

        long cutoff = System.currentTimeMillis() - Duration.ofMinutes(10).toMillis();
        processedDuelRequests.entrySet().removeIf(entry -> entry.getValue() < cutoff);
    }

    private void handlePlayerJoinEvents(Collection<NetworkEvent> events) {
        for (NetworkEvent event : events) {
            lastPlayerJoinEventId = Math.max(lastPlayerJoinEventId, event.getId());
            ObjectNode payload = event.payloadAsObjectNode(mapper);
            String playerIdRaw = payload.path("playerId").asText(null);
            String serverName = payload.path("serverName").asText(null);
            if (playerIdRaw == null || serverName == null) {
                continue;
            }
            try {
                UUID playerId = UUID.fromString(playerIdRaw);
                playerServerMap.put(playerId, serverName);
            } catch (IllegalArgumentException ignored) {
            }
        }
    }

    private void handlePlayerLeaveEvents(Collection<NetworkEvent> events) {
        for (NetworkEvent event : events) {
            lastPlayerLeaveEventId = Math.max(lastPlayerLeaveEventId, event.getId());
            ObjectNode payload = event.payloadAsObjectNode(mapper);
            String playerIdRaw = payload.path("playerId").asText(null);
            if (playerIdRaw == null) {
                continue;
            }
            try {
                UUID playerId = UUID.fromString(playerIdRaw);
                playerServerMap.remove(playerId);
            } catch (IllegalArgumentException ignored) {
            }
        }
    }

    private void handlePlayerTransferEvents(Collection<NetworkEvent> events) {
        for (NetworkEvent event : events) {
            lastPlayerTransferEventId = Math.max(lastPlayerTransferEventId, event.getId());
            ObjectNode payload = event.payloadAsObjectNode(mapper);
            handlePlayerTransfer(payload);
        }
    }

    private void handleDuelRequestEvents(Collection<NetworkEvent> events) {
        for (NetworkEvent event : events) {
            lastDuelRequestEventId = Math.max(lastDuelRequestEventId, event.getId());
            ObjectNode payload = event.payloadAsObjectNode(mapper);
            processDuelRequestPayload(payload);
        }
    }

    private void handleQueueEvents(Collection<NetworkEvent> events) {
        Set<String> dirtyKeys = new HashSet<>();

        for (NetworkEvent event : events) {
            lastQueueEventId = Math.max(lastQueueEventId, event.getId());
            ObjectNode payload = event.payloadAsObjectNode(mapper);
            String action = payload.path("action").asText("");

            if ("join".equalsIgnoreCase(action)) {
                NetworkQueue entry = deserializeQueueEntry(payload);
                if (entry == null) {
                    continue;
                }
                cacheQueueEntry(entry);
                dirtyKeys.add(buildQueueKey(entry.getKitName(), entry.getBet()));
            } else if ("leave".equalsIgnoreCase(action)) {
                UUID playerId = parseUuid(payload.path("playerId").asText(null));
                if (playerId != null) {
                    removeCachedQueueEntry(playerId);
                }
                dirtyKeys.add(buildQueueKey(payload.hasNonNull("kitName") ? payload.get("kitName").asText() : null,
                    payload.path("bet").asInt(0)));
            }
        }

        for (String key : dirtyKeys) {
            attemptMatchmaking(key);
        }
    }

    private boolean isDatabaseReady() {
        return databaseManager != null && databaseManager.isConnected();
    }

    private void processDuelRequestPayload(ObjectNode payload) {
        if (payload == null) {
            return;
        }

        String requestIdRaw = payload.path("requestId").asText(null);
        if (requestIdRaw == null) {
            if (plugin.getConfig().isDebugMode()) {
                plugin.getLogger().warn("Ignoring duel request without requestId: {}", payload);
            }
            return;
        }

        UUID requestId;
        try {
            requestId = UUID.fromString(requestIdRaw);
        } catch (IllegalArgumentException exception) {
            plugin.getLogger().warn("Invalid duel request id {}", requestIdRaw);
            return;
        }

        Long previous = processedDuelRequests.putIfAbsent(requestId, System.currentTimeMillis());
        if (previous != null) {
            return;
        }

        ObjectNode localNode = payload.has("local") && payload.get("local").isObject()
            ? (ObjectNode) payload.get("local") : null;
        ObjectNode remoteNode = payload.has("remote") && payload.get("remote").isObject()
            ? (ObjectNode) payload.get("remote") : null;

        if (localNode == null || remoteNode == null) {
            plugin.getLogger().warn("Incomplete duel request payload for {}: {}", requestId, payload);
            processedDuelRequests.remove(requestId);
            return;
        }

        UUID localId = parseUuid(localNode.path("playerId").asText(null));
        UUID remoteId = parseUuid(remoteNode.path("playerId").asText(null));
        if (localId == null || remoteId == null) {
            processedDuelRequests.remove(requestId);
            return;
        }

        String hostServer = payload.path("hostServer").asText(null);
        String remoteServer = payload.path("remoteServer").asText(null);
        if (hostServer == null || remoteServer == null) {
            processedDuelRequests.remove(requestId);
            return;
        }

        String localName = localNode.path("playerName").asText("Unknown");
        String remoteName = remoteNode.path("playerName").asText("Unknown");
        String kitName = payload.hasNonNull("kitName") ? payload.get("kitName").asText() : null;
        int bet = payload.path("bet").asInt(0);

        if (plugin.getConfig().isDebugMode()) {
            plugin.getLogger().info("Processing duel request {}: {} vs {} on {}",
                requestId, localName, remoteName, hostServer);
        }

        CompletableFuture<Boolean> localReady = ensurePlayerOnServer(localId, hostServer);
        CompletableFuture<Boolean> remoteReady = ensurePlayerOnServer(remoteId, hostServer);

        CompletableFuture.allOf(localReady, remoteReady)
            .thenCompose(ignored -> {
                boolean localOk = Boolean.TRUE.equals(localReady.join());
                boolean remoteOk = Boolean.TRUE.equals(remoteReady.join());

                if (!localOk || !remoteOk) {
                    if (!localOk) {
                        plugin.getLogger().warn("Local player {} unavailable for cross-server duel {}", localName, requestId);
                    }
                    if (!remoteOk) {
                        plugin.getLogger().warn("Remote player {} unavailable for cross-server duel {}", remoteName, requestId);
                    }
                    processedDuelRequests.remove(requestId);
                    return CompletableFuture.completedFuture(null);
                }

                List<NetworkQueue> teamA = new ArrayList<>();
                teamA.add(new NetworkQueue(localId, localName, hostServer, kitName, bet));

                List<NetworkQueue> teamB = new ArrayList<>();
                teamB.add(new NetworkQueue(remoteId, remoteName, remoteServer, kitName, bet));

                PacketHandler handler = plugin.getPacketHandler();
                if (handler == null) {
                    plugin.getLogger().error("Packet handler unavailable; cannot start duel {}", requestId);
                    processedDuelRequests.remove(requestId);
                    return CompletableFuture.completedFuture(null);
                }

                return handler.notifyMatchStart(requestId, hostServer, teamA, teamB, kitName, bet);
            })
            .exceptionally(throwable -> {
                plugin.getLogger().error("Failed to orchestrate duel request {}: {}", requestId, throwable.getMessage());
                processedDuelRequests.remove(requestId);
                return null;
            });
    }

    private UUID parseUuid(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private CompletableFuture<Boolean> ensurePlayerOnServer(UUID playerId, String targetServer) {
        if (playerId == null || targetServer == null) {
            return CompletableFuture.completedFuture(false);
        }

        Optional<Player> playerOpt = plugin.getServer().getPlayer(playerId);
        if (playerOpt.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }

        Player player = playerOpt.get();
        String current = player.getCurrentServer()
            .map(registeredServer -> registeredServer.getServerInfo().getName())
            .orElse(null);

        if (targetServer.equalsIgnoreCase(current)) {
            return CompletableFuture.completedFuture(true);
        }

        return transferPlayer(playerId, targetServer);
    }

    public Optional<String> getPlayerServer(UUID playerId) {
        Optional<String> cached = Optional.ofNullable(playerServerMap.get(playerId));
        if (cached.isPresent() || !isDatabaseReady()) {
            return cached;
        }

        databaseManager.getPlayerServer(playerId)
            .thenAccept(opt -> opt.ifPresent(server -> playerServerMap.put(playerId, server)));
        return cached;
    }

    public Optional<String> getPlayerServer(String playerName) {
        return plugin.getServer().getPlayer(playerName)
            .flatMap(Player::getCurrentServer)
            .map(server -> server.getServerInfo().getName());
    }

    public CompletableFuture<Boolean> transferPlayer(UUID playerId, String targetServer) {
        Optional<Player> playerOpt = plugin.getServer().getPlayer(playerId);
        if (playerOpt.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }

        Player player = playerOpt.get();

        String currentServer = player.getCurrentServer()
            .map(registeredServer -> registeredServer.getServerInfo().getName())
            .orElse(null);
        if (currentServer != null && currentServer.equalsIgnoreCase(targetServer)) {
            return CompletableFuture.completedFuture(true);
        }

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
                        player.getUsername(), targetServer, result.getReasonComponent().orElse(null));
                    return false;
                }
            });
    }

    public String getBestDuelServer() {
        return plugin.getConfig().getDuelServers().stream()
            .min(Comparator.comparingInt(server ->
                serverPlayerCounts.getOrDefault(server, Integer.MAX_VALUE)))
            .orElse(plugin.getConfig().getDefaultDuelServer());
    }

    public CompletableFuture<Void> broadcastDuelRequest(UUID senderId, UUID targetId, String duelData) {
        if (!isDatabaseReady()) {
            return CompletableFuture.completedFuture(null);
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("senderId", senderId.toString());
        payload.put("targetId", targetId.toString());
        payload.put("duelData", duelData);
        payload.put("timestamp", System.currentTimeMillis());

        return databaseManager.publishEvent(CHANNEL_DUEL_REQUEST, payload)
            .thenAccept(ignored -> {
            });
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

    private void handlePlayerTransfer(ObjectNode payload) {
        String playerIdRaw = payload.path("playerId").asText(null);
        String playerName = payload.path("playerName").asText(null);
        String targetServer = payload.path("targetServer").asText(null);

        if (targetServer == null || targetServer.isBlank()) {
            return;
        }

        CompletableFuture<Boolean> transferFuture;
        if (playerIdRaw != null && !playerIdRaw.isBlank()) {
            try {
                UUID playerId = UUID.fromString(playerIdRaw);
                transferFuture = transferPlayer(playerId, targetServer);
            } catch (IllegalArgumentException exception) {
                transferFuture = CompletableFuture.completedFuture(false);
            }
        } else if (playerName != null && !playerName.isBlank()) {
            transferFuture = plugin.getServer().getPlayer(playerName)
                .map(player -> transferPlayer(player.getUniqueId(), targetServer))
                .orElseGet(() -> CompletableFuture.completedFuture(false));
        } else {
            transferFuture = CompletableFuture.completedFuture(false);
        }

        transferFuture.thenAccept(success -> {
            if (!success && plugin.getConfig().isDebugMode()) {
                plugin.getLogger().warn("Failed to transfer player via event: {}", payload);
            }
        });
    }

    private void cacheQueueEntry(NetworkQueue entry) {
        if (entry == null || playersPendingMatch.contains(entry.getPlayerId())) {
            return;
        }

        if (!isPlayerOnline(entry.getPlayerId())) {
            return;
        }

        queueEntries.compute(buildQueueKey(entry.getKitName(), entry.getBet()), (key, list) -> {
            if (list == null) {
                list = new ArrayList<>();
            }

            list.removeIf(existing -> existing.getPlayerId().equals(entry.getPlayerId()));
            list.add(entry);
            list.sort(Comparator.comparingLong(NetworkQueue::getJoinedAt));
            return list;
        });
    }

    private void removeCachedQueueEntry(UUID playerId) {
        if (playerId == null) {
            return;
        }

        queueEntries.values().forEach(list -> list.removeIf(entry -> entry.getPlayerId().equals(playerId)));
        playersPendingMatch.remove(playerId);
    }

    private void attemptMatchmaking(String key) {
        if (key == null) {
            return;
        }

        List<NetworkQueue> entries = queueEntries.get(key);
        if (entries == null || entries.isEmpty()) {
            return;
        }

        synchronized (entries) {
            entries.removeIf(entry -> entry == null || playersPendingMatch.contains(entry.getPlayerId()) || !isPlayerOnline(entry.getPlayerId()));

            while (entries.size() >= 2) {
                NetworkQueue first = entries.remove(0);
                if (first == null || first.isInParty()) {
                    continue;
                }

                NetworkQueue second = null;
                Iterator<NetworkQueue> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    NetworkQueue candidate = iterator.next();
                    if (candidate == null || candidate.isInParty()) {
                        continue;
                    }

                    if (first.matches(candidate)) {
                        second = candidate;
                        iterator.remove();
                        break;
                    }
                }

                if (second == null) {
                    entries.add(0, first);
                    break;
                }

                playersPendingMatch.add(first.getPlayerId());
                playersPendingMatch.add(second.getPlayerId());

                startCrossServerDuel(first, second, key);
            }
        }
    }

    private void startCrossServerDuel(NetworkQueue first, NetworkQueue second, String key) {
        String hostServer = plugin.getConfig().getDuelServers().isEmpty()
            ? plugin.getConfig().getDefaultDuelServer()
            : getBestDuelServer();

        if (hostServer == null || hostServer.isBlank()) {
            plugin.getLogger().warn("No duel host server configured; unable to start cross-server match for {}", key);
            playersPendingMatch.remove(first.getPlayerId());
            playersPendingMatch.remove(second.getPlayerId());
            return;
        }

        PacketHandler handler = plugin.getPacketHandler();
        if (handler == null) {
            plugin.getLogger().error("Packet handler unavailable; cannot orchestrate cross-server duel");
            playersPendingMatch.remove(first.getPlayerId());
            playersPendingMatch.remove(second.getPlayerId());
            cacheQueueEntry(first);
            cacheQueueEntry(second);
            return;
        }

        CompletableFuture<Boolean> transferFirst = handler.requestPlayerTransfer(first.getPlayerId(), hostServer, "duels-match");
        CompletableFuture<Boolean> transferSecond = handler.requestPlayerTransfer(second.getPlayerId(), hostServer, "duels-match");

        CompletableFuture.allOf(transferFirst, transferSecond)
            .thenCompose(unused -> {
                boolean firstOk = Boolean.TRUE.equals(transferFirst.join());
                boolean secondOk = Boolean.TRUE.equals(transferSecond.join());

                if (!firstOk || !secondOk) {
                    plugin.getLogger().warn("Failed to transfer players for cross-server duel {} -> {}", key, hostServer);
                    playersPendingMatch.remove(first.getPlayerId());
                    playersPendingMatch.remove(second.getPlayerId());
                    cacheQueueEntry(first);
                    cacheQueueEntry(second);
                    return CompletableFuture.completedFuture(null);
                }

                UUID matchId = UUID.randomUUID();
                List<NetworkQueue> teamA = Collections.singletonList(first);
                List<NetworkQueue> teamB = Collections.singletonList(second);

                return handler.notifyMatchStart(matchId, hostServer, teamA, teamB,
                    first.getKitName() != null ? first.getKitName() : second.getKitName(),
                    first.getBet());
            })
            .whenComplete((unused, ex) -> {
                playersPendingMatch.remove(first.getPlayerId());
                playersPendingMatch.remove(second.getPlayerId());
                if (ex != null) {
                    plugin.getLogger().error("Error while starting cross-server duel {}: {}", key, ex.getMessage());
                    cacheQueueEntry(first);
                    cacheQueueEntry(second);
                }
            });
    }

    private NetworkQueue deserializeQueueEntry(ObjectNode payload) {
        if (payload == null) {
            return null;
        }

        UUID playerId = parseUuid(payload.path("playerId").asText(null));
        if (playerId == null) {
            return null;
        }

        String playerName = payload.path("playerName").asText(null);
        String serverName = payload.path("serverName").asText(null);
        if (playerName == null || serverName == null) {
            return null;
        }

        String kitName = payload.hasNonNull("kitName") ? payload.get("kitName").asText() : null;
        int bet = payload.path("bet").asInt(0);

        NetworkQueue entry = new NetworkQueue(playerId, playerName, serverName, kitName, bet);
        entry.setJoinedAt(payload.path("timestamp").asLong(System.currentTimeMillis()));

        if (payload.has("partyMembers") && payload.get("partyMembers").isArray()) {
            List<UUID> members = new ArrayList<>();
            payload.get("partyMembers").forEach(node -> {
                UUID member = parseUuid(node.asText(null));
                if (member != null) {
                    members.add(member);
                }
            });
            if (!members.isEmpty()) {
                entry.setPartyMembers(members);
            }
        }

        return entry;
    }

    private String buildQueueKey(String kitName, int bet) {
        return (kitName == null ? "__none__" : kitName) + ":" + bet;
    }
}