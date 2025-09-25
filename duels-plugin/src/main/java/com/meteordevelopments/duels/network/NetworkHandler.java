package com.meteordevelopments.duels.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.meteordevelopments.duels.DuelsPlugin;
import com.meteordevelopments.duels.arena.ArenaImpl;
import com.meteordevelopments.duels.common.database.DatabaseManager;
import com.meteordevelopments.duels.common.database.DatabaseSettings;
import com.meteordevelopments.duels.common.database.NetworkEvent;
import com.meteordevelopments.duels.common.network.NetworkArena;
import com.meteordevelopments.duels.common.network.NetworkQueue;
import com.meteordevelopments.duels.config.Config;
import com.meteordevelopments.duels.config.Lang;
import com.meteordevelopments.duels.database.BukkitDatabaseLogger;
import com.meteordevelopments.duels.duel.DuelManager;
import com.meteordevelopments.duels.kit.KitImpl;
import com.meteordevelopments.duels.kit.KitManagerImpl;
import com.meteordevelopments.duels.party.Party;
import com.meteordevelopments.duels.party.PartyManagerImpl;
import com.meteordevelopments.duels.party.PartyMember;
import com.meteordevelopments.duels.queue.Queue;
import com.meteordevelopments.duels.queue.QueueEntry;
import com.meteordevelopments.duels.queue.QueueManager;
import com.meteordevelopments.duels.setting.CachedInfo;
import com.meteordevelopments.duels.setting.Settings;
import com.meteordevelopments.duels.util.Loadable;
import lombok.Getter;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.entity.Player;
import space.arim.morepaperlib.scheduling.ScheduledTask;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Handles network communication between Bukkit servers and the Velocity proxy
 * using a shared SQL database.
 */
public class NetworkHandler implements Loadable {

    private static final long TICKS_PER_SECOND = 20L;

    private static final String CHANNEL_PLAYER_JOIN = "duels:player-join";
    private static final String CHANNEL_PLAYER_LEAVE = "duels:player-leave";
    private static final String CHANNEL_ARENA_UPDATE = "duels:arena-update";
    private static final String CHANNEL_QUEUE_UPDATE = "duels:queue-update";
    private static final String CHANNEL_PLAYER_TRANSFER = "duels:player-transfer";
    private static final String CHANNEL_DUEL_REQUEST = "duels:duel-request";
    private static final String CHANNEL_MATCH_START = "duels:match-start";
    private static final int CROSS_SERVER_MATCH_TIMEOUT_SECONDS = 30;

    private final DuelsPlugin plugin;
    private final Config config;
    private final Lang lang;
    private final ObjectMapper mapper = new ObjectMapper();

    private QueueManager queueManager;
    private DuelManager duelManager;
    private KitManagerImpl kitManager;
    private PartyManagerImpl partyManager;

    private DatabaseManager databaseManager;
    private final List<ScheduledTask> scheduledTasks = new ArrayList<>();

    private final Map<String, NetworkArena> networkArenas = new ConcurrentHashMap<>();
    private final Map<String, List<NetworkQueue>> networkQueues = new ConcurrentHashMap<>();
    private final Map<UUID, PendingLocalMatch> pendingLocalMatches = new ConcurrentHashMap<>();
    private final Map<UUID, PendingMatchStart> pendingMatchStarts = new ConcurrentHashMap<>();

    private volatile long lastArenaEventId = 0L;
    private volatile long lastQueueEventId = 0L;
    private volatile long lastPlayerTransferEventId = 0L;
    private volatile long lastDuelRequestEventId = 0L;
    private volatile long lastMatchStartEventId = 0L;

    @Getter
    private boolean networkEnabled = false;
    @Getter
    private String serverName;

    public NetworkHandler(DuelsPlugin plugin) {
        this.plugin = plugin;
        this.config = plugin.getConfiguration();
        this.lang = plugin.getLang();
    }

    @Override
    public void handleLoad() {
        refreshManagerReferences();
        serverName = config.getNetworkServerName();

        if (!config.isNetworkEnabled()) {
            plugin.info("Network support disabled - running in standalone mode");
            return;
        }

        DatabaseSettings settings = config.getNetworkDatabaseSettings();
        databaseManager = new DatabaseManager(settings, new BukkitDatabaseLogger(plugin));

        try {
            databaseManager.connect();
        } catch (Exception exception) {
            plugin.warn("Failed to connect to the shared database: " + exception.getMessage());
            plugin.getLogger().log(java.util.logging.Level.SEVERE,
                    "Unable to connect to duel network database", exception);
            databaseManager = null;
            return;
        }

        networkEnabled = true;
        plugin.info("Network support enabled for server: " + serverName);

        initializeCaches();
        registerOnlinePlayers();
        startSchedulers();
    }

    @Override
    public void handleUnload() {
        scheduledTasks.forEach(ScheduledTask::cancel);
        scheduledTasks.clear();

        networkArenas.clear();
        networkQueues.clear();
        pendingLocalMatches.clear();
        pendingMatchStarts.clear();

        networkEnabled = false;

        if (databaseManager != null) {
            databaseManager.disconnect();
            databaseManager = null;
        }
    }

    private void refreshManagerReferences() {
        if (queueManager == null) {
            queueManager = plugin.getQueueManager();
        }
        if (duelManager == null) {
            duelManager = plugin.getDuelManager();
        }
        if (kitManager == null) {
            kitManager = plugin.getKitManager();
        }
        if (partyManager == null) {
            partyManager = plugin.getPartyManager();
        }
    }

    private void initializeCaches() {
        refreshRemoteArenas();
        refreshQueueCache();
    }

    private void registerOnlinePlayers() {
        if (!isDatabaseReady()) {
            return;
        }

        Bukkit.getOnlinePlayers().forEach(this::updatePlayerLocation);
    }

    private void startSchedulers() {
        scheduledTasks.add(plugin.doAsyncRepeat(this::pollNetworkEvents,
                TICKS_PER_SECOND * 2, TICKS_PER_SECOND * 2));

        scheduledTasks.add(plugin.doAsyncRepeat(this::refreshCachesPeriodically,
                TICKS_PER_SECOND * 60, TICKS_PER_SECOND * 120));

        scheduledTasks.add(plugin.doAsyncRepeat(() -> cleanupOldEvents(Duration.ofMinutes(10)),
                TICKS_PER_SECOND * 300, TICKS_PER_SECOND * 300));

    scheduledTasks.add(plugin.doSyncRepeat(this::processPendingMatches,
        TICKS_PER_SECOND, TICKS_PER_SECOND));
    }

    private void pollNetworkEvents() {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.fetchEvents(CHANNEL_ARENA_UPDATE, lastArenaEventId, 64)
                .thenAccept(this::handleArenaEvents)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to poll arena events: " + throwable.getMessage());
                    return null;
                });

        databaseManager.fetchEvents(CHANNEL_QUEUE_UPDATE, lastQueueEventId, 64)
                .thenAccept(this::handleQueueEvents)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to poll queue events: " + throwable.getMessage());
                    return null;
                });

        databaseManager.fetchEvents(CHANNEL_PLAYER_TRANSFER, lastPlayerTransferEventId, 32)
                .thenAccept(this::handleTransferEvents)
                .exceptionally(throwable -> {
                    if (config.isNetworkDebugMode()) {
                        plugin.warn("Failed to poll player transfer events: " + throwable.getMessage());
                    }
                    return null;
                });

        databaseManager.fetchEvents(CHANNEL_DUEL_REQUEST, lastDuelRequestEventId, 32)
                .thenAccept(this::handleDuelRequestEvents)
                .exceptionally(throwable -> {
                    if (config.isNetworkDebugMode()) {
                        plugin.warn("Failed to poll duel request events: " + throwable.getMessage());
                    }
                    return null;
                });

        databaseManager.fetchEvents(CHANNEL_MATCH_START, lastMatchStartEventId, 32)
                .thenAccept(this::handleMatchStartEvents)
                .exceptionally(throwable -> {
                    if (config.isNetworkDebugMode()) {
                        plugin.warn("Failed to poll match start events: " + throwable.getMessage());
                    }
                    return null;
                });
    }

    private void refreshCachesPeriodically() {
        refreshRemoteArenas();
        refreshQueueCache();
    }

    private void cleanupOldEvents(Duration retention) {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.cleanupOldEvents(retention.toMillis())
                .exceptionally(throwable -> {
                    if (config.isNetworkDebugMode()) {
                        plugin.warn("Failed to prune old network events: " + throwable.getMessage());
                    }
                    return null;
                });
    }

    private void refreshRemoteArenas() {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.loadArenas()
                .thenAccept(arenas -> {
                    Map<String, NetworkArena> refreshed = new HashMap<>();
                    for (NetworkArena arena : arenas) {
                        if (serverName.equalsIgnoreCase(arena.getServerName())) {
                            continue;
                        }
                        refreshed.put(arena.getFullName(), arena);
                    }
                    networkArenas.clear();
                    networkArenas.putAll(refreshed);
                })
                .exceptionally(throwable -> {
                    plugin.warn("Failed to refresh remote arenas: " + throwable.getMessage());
                    return null;
                });
    }

    private void refreshQueueCache() {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.loadAllQueueEntries()
                .thenAccept(entries -> {
                    Map<String, List<NetworkQueue>> refreshed = new HashMap<>();
                    for (NetworkQueue entry : entries) {
                        if (serverName.equalsIgnoreCase(entry.getServerName())) {
                            continue;
                        }
                        String key = buildQueueKey(entry.getKitName(), entry.getBet());
                        refreshed.computeIfAbsent(key, unused -> new ArrayList<>()).add(entry);
                    }
                    networkQueues.clear();
                    networkQueues.putAll(refreshed);
                })
                .exceptionally(throwable -> {
                    plugin.warn("Failed to refresh network queue cache: " + throwable.getMessage());
                    return null;
                });
    }

    private void handleArenaEvents(Collection<NetworkEvent> events) {
        for (NetworkEvent event : events) {
            lastArenaEventId = Math.max(lastArenaEventId, event.getId());
            ObjectNode payload = event.payloadAsObjectNode(mapper);
            String sourceServer = payload.path("serverName").asText("");
            if (serverName.equalsIgnoreCase(sourceServer)) {
                continue;
            }

            String arenaName = payload.path("arenaName").asText("");
            String action = payload.path("action").asText("update");

            if ("remove".equalsIgnoreCase(action)) {
                networkArenas.remove(sourceServer + ":" + arenaName);
                continue;
            }

            JsonNode arenaDataNode = payload.get("arenaData");
            NetworkArena arena = new NetworkArena(arenaName, sourceServer);
            if (arenaDataNode != null && arenaDataNode.isObject()) {
                ObjectNode data = (ObjectNode) arenaDataNode;
                arena.setAvailable(data.path("available").asBoolean(true));
                arena.setDisabled(data.path("disabled").asBoolean(false));
                arena.setLastUpdate(data.path("lastUpdate").asLong(System.currentTimeMillis()));

                if (data.has("boundKits")) {
                    List<String> kits = new ArrayList<>();
                    data.path("boundKits").forEach(node -> kits.add(node.asText()));
                    arena.setBoundKits(kits);
                }

                if (data.has("positions")) {
                    Map<String, Object> positions = mapper.convertValue(data.get("positions"), Map.class);
                    arena.setPositions(positions);
                }
            }

            networkArenas.put(arena.getFullName(), arena);
        }
    }

    private void handleQueueEvents(Collection<NetworkEvent> events) {
        boolean refreshQueues = false;

        for (NetworkEvent event : events) {
            lastQueueEventId = Math.max(lastQueueEventId, event.getId());
            ObjectNode payload = event.payloadAsObjectNode(mapper);
            String sourceServer = payload.path("serverName").asText("");
            if (serverName.equalsIgnoreCase(sourceServer)) {
                continue;
            }

            refreshQueues = true;
        }

        if (refreshQueues) {
            refreshQueueCache();
        }
    }

    private void handleTransferEvents(Collection<NetworkEvent> events) {
        for (NetworkEvent event : events) {
            lastPlayerTransferEventId = Math.max(lastPlayerTransferEventId, event.getId());
            if (config.isNetworkDebugMode()) {
                plugin.getLogger().info("Received player transfer event: " + event.getPayload());
            }
        }
    }

    private void handleDuelRequestEvents(Collection<NetworkEvent> events) {
        for (NetworkEvent event : events) {
            lastDuelRequestEventId = Math.max(lastDuelRequestEventId, event.getId());
            if (config.isNetworkDebugMode()) {
                plugin.getLogger().info("Received duel request event: " + event.getPayload());
            }
        }
    }

    private void handleMatchStartEvents(Collection<NetworkEvent> events) {
        for (NetworkEvent event : events) {
            lastMatchStartEventId = Math.max(lastMatchStartEventId, event.getId());
            ObjectNode payload = event.payloadAsObjectNode(mapper);

            String targetServer = payload.path("serverName").asText("");
            if (!serverName.equalsIgnoreCase(targetServer)) {
                continue;
            }

            String matchIdRaw = payload.hasNonNull("matchId") ? payload.get("matchId").asText() : payload.path("requestId").asText(null);
            if (matchIdRaw == null) {
                if (config.isNetworkDebugMode()) {
                    plugin.warn("Received match-start payload without matchId: " + payload);
                }
                continue;
            }

            UUID matchId;
            try {
                matchId = UUID.fromString(matchIdRaw);
            } catch (IllegalArgumentException exception) {
                if (config.isNetworkDebugMode()) {
                    plugin.warn("Invalid matchId in match-start event: " + matchIdRaw);
                }
                continue;
            }

            PendingLocalMatch localMatch = pendingLocalMatches.remove(matchId);
            if (localMatch == null && config.isNetworkDebugMode()) {
                plugin.info("Received proxy-initiated match start " + matchId);
            }

            List<UUID> teamA = new ArrayList<>();
            List<UUID> teamB = new ArrayList<>();

            JsonNode playersNode = payload.get("players");
            if (playersNode != null && playersNode.isArray()) {
                for (JsonNode node : playersNode) {
                    String playerIdRaw = node.path("id").asText(null);
                    if (playerIdRaw == null) {
                        continue;
                    }

                    UUID playerId;
                    try {
                        playerId = UUID.fromString(playerIdRaw);
                    } catch (IllegalArgumentException ignored) {
                        continue;
                    }

                    String side = node.path("side").asText("A");
                    if ("B".equalsIgnoreCase(side) || node.path("side").asInt(1) == 2) {
                        teamB.add(playerId);
                    } else {
                        teamA.add(playerId);
                    }
                }
            }

            String kitName = payload.has("kitName") && !payload.get("kitName").isNull()
                    ? payload.get("kitName").asText() : null;
            int bet = payload.path("bet").asInt(localMatch.bet());

        PendingMatchStart pending = new PendingMatchStart(matchId, localMatch, teamA, teamB,
            kitName, bet, CROSS_SERVER_MATCH_TIMEOUT_SECONDS);

            pendingMatchStarts.put(matchId, pending);

            if (config.isNetworkDebugMode()) {
                plugin.info("Queued pending cross-server match start " + matchId + " for " + teamA + " vs " + teamB);
            }
        }
    }

    private Queue resolveQueue(String kitName, int bet) {
        refreshManagerReferences();
        if (queueManager == null) {
            return null;
        }

        KitImpl kit = null;
        if (kitName != null && kitManager != null) {
            kit = kitManager.get(kitName);
        }

        return queueManager.get(kit, bet);
    }

    private QueueEntry findEligibleLocalEntry(Queue queue, UUID remotePlayerId) {
        if (queue == null) {
            return null;
        }

        for (QueueEntry entry : new ArrayList<>(queue.getPlayers())) {
            Player player = entry.getPlayer();
            if (player == null || !player.isOnline()) {
                continue;
            }
            if (player.getUniqueId().equals(remotePlayerId)) {
                continue;
            }
            if (isPlayerPending(player.getUniqueId())) {
                continue;
            }
            return entry;
        }

        return null;
    }

    private boolean isPlayerPending(UUID playerId) {
        for (PendingLocalMatch match : pendingLocalMatches.values()) {
            if (playerId.equals(match.localPlayerId())) {
                return true;
            }
        }

        for (PendingMatchStart pending : pendingMatchStarts.values()) {
            if (pending.teamA().contains(playerId) || pending.teamB().contains(playerId)) {
                return true;
            }
        }

        return false;
    }

    private CachedInfo cloneCachedInfo(CachedInfo info) {
        if (info == null) {
            return null;
        }

        Location location = info.getLocation();
        return new CachedInfo(location != null ? location.clone() : null, info.getDuelzone());
    }

    private void restoreLocalPlayer(PendingLocalMatch match) {
        if (match == null) {
            return;
        }

        refreshManagerReferences();
        if (queueManager == null) {
            return;
        }

        Player player = match.localPlayer();
        Queue queue = match.queue();
        if (player == null || queue == null) {
            return;
        }

        queueManager.queue(player, queue);
    }

    private void processPendingMatches() {
        if (pendingMatchStarts.isEmpty()) {
            return;
        }

        List<UUID> completed = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (PendingMatchStart pending : pendingMatchStarts.values()) {
            if (now >= pending.expiresAt()) {
                restoreLocalPlayer(pending.localMatch());
                if (config.isNetworkDebugMode()) {
                    plugin.warn("Cross-server match " + pending.matchId() + " timed out waiting for players");
                }
                completed.add(pending.matchId());
                continue;
            }

            if (!arePlayersPresent(pending)) {
                continue;
            }

            if (startPendingMatch(pending)) {
                completed.add(pending.matchId());
            }
        }

        completed.forEach(pendingMatchStarts::remove);
    }

    private boolean arePlayersPresent(PendingMatchStart pending) {
        for (UUID playerId : pending.teamA()) {
            Player player = Bukkit.getPlayer(playerId);
            if (player == null || !player.isOnline()) {
                return false;
            }
        }

        for (UUID playerId : pending.teamB()) {
            Player player = Bukkit.getPlayer(playerId);
            if (player == null || !player.isOnline()) {
                return false;
            }
        }

        return true;
    }

    private boolean startPendingMatch(PendingMatchStart pending) {
        refreshManagerReferences();

        PendingLocalMatch local = pending.localMatch();
        if (duelManager == null) {
            return false;
        }

        Settings settings;
        if (local != null) {
            settings = local.copySettings();
        } else {
            settings = new Settings(plugin);
            settings.setBet(pending.bet());
            if (pending.kitName() != null && kitManager != null) {
                KitImpl kit = kitManager.get(pending.kitName());
                if (kit != null) {
                    settings.setKit(kit);
                } else {
                    settings.setOwnInventory(true);
                }
            } else {
                settings.setOwnInventory(true);
            }
        }

        if (settings.getKit() == null && pending.kitName() != null && kitManager != null) {
            KitImpl kit = kitManager.get(pending.kitName());
            if (kit != null) {
                settings.setKit(kit);
            } else {
                settings.setOwnInventory(true);
            }
        }

        List<Player> teamAPlayers = resolvePlayers(pending.teamA());
        List<Player> teamBPlayers = resolvePlayers(pending.teamB());

        if (teamAPlayers.isEmpty() || teamBPlayers.isEmpty()) {
            return false;
        }

        for (Player player : teamAPlayers) {
            settings.getCache().computeIfAbsent(player.getUniqueId(), uuid ->
                    new CachedInfo(player.getLocation() != null ? player.getLocation().clone() : null, null));
        }

        for (Player player : teamBPlayers) {
            settings.getCache().put(player.getUniqueId(),
                    new CachedInfo(player.getLocation() != null ? player.getLocation().clone() : null, null));
        }

        Queue matchQueue = local != null ? local.queue() : resolveQueue(pending.kitName(), pending.bet());

        boolean started = duelManager.startMatch(teamAPlayers, teamBPlayers, settings, null, matchQueue);
        if (!started && local != null) {
            restoreLocalPlayer(local);
        }

        return started;
    }

    private List<Player> resolvePlayers(Collection<UUID> playerIds) {
        List<Player> players = new ArrayList<>();
        for (UUID id : playerIds) {
            Player player = Bukkit.getPlayer(id);
            if (player != null && player.isOnline()) {
                players.add(player);
            }
        }
        return players;
    }

    private static class PendingLocalMatch {
        private final UUID requestId;
        private final Queue queue;
        private final Player localPlayer;
        private final CachedInfo cachedInfo;
        private final Settings baseSettings;
        private final String kitName;
        private final int bet;
        private final long createdAt;

        PendingLocalMatch(UUID requestId, Queue queue, Player localPlayer, CachedInfo cachedInfo,
                           Settings baseSettings, String kitName, int bet, long createdAt) {
            this.requestId = requestId;
            this.queue = queue;
            this.localPlayer = localPlayer;
            this.cachedInfo = cachedInfo;
            this.baseSettings = baseSettings;
            this.kitName = kitName;
            this.bet = bet;
            this.createdAt = createdAt;
        }

        UUID requestId() {
            return requestId;
        }

        Queue queue() {
            return queue;
        }

        Player localPlayer() {
            return localPlayer;
        }

        UUID localPlayerId() {
            return localPlayer != null ? localPlayer.getUniqueId() : null;
        }

        String kitName() {
            return kitName;
        }

        int bet() {
            return bet;
        }

        long createdAt() {
            return createdAt;
        }

        Settings copySettings() {
            Settings copy = baseSettings.lightCopy();
            if (cachedInfo != null && localPlayer != null) {
                Location location = cachedInfo.getLocation();
                CachedInfo infoCopy = new CachedInfo(location != null ? location.clone() : null, cachedInfo.getDuelzone());
                copy.getCache().put(localPlayer.getUniqueId(), infoCopy);
            }
            return copy;
        }
    }

    private static class PendingMatchStart {
        private final UUID matchId;
        private final PendingLocalMatch localMatch;
        private final List<UUID> teamA;
        private final List<UUID> teamB;
        private final String kitName;
        private final int bet;
        private final long createdAt;
        private final long expiresAt;

        PendingMatchStart(UUID matchId, PendingLocalMatch localMatch, List<UUID> teamA, List<UUID> teamB,
                          String kitName, int bet, int timeoutSeconds) {
            this.matchId = matchId;
            this.localMatch = localMatch;
            this.teamA = new ArrayList<>(teamA);
            this.teamB = new ArrayList<>(teamB);
            this.kitName = kitName;
            this.bet = bet;
            this.createdAt = System.currentTimeMillis();
            this.expiresAt = this.createdAt + Math.max(5, timeoutSeconds) * 1000L;
        }

        UUID matchId() {
            return matchId;
        }

        PendingLocalMatch localMatch() {
            return localMatch;
        }

        List<UUID> teamA() {
            return teamA;
        }

        List<UUID> teamB() {
            return teamB;
        }

        String kitName() {
            return kitName;
        }

        int bet() {
            return bet;
        }

        long createdAt() {
            return createdAt;
        }

        long expiresAt() {
            return expiresAt;
        }
    }

    private NetworkQueue deserializeQueueEntry(ObjectNode payload) {
        String playerIdRaw = payload.path("playerId").asText(null);
        String playerName = payload.path("playerName").asText(null);
        String kitName = payload.has("kitName") && !payload.get("kitName").isNull()
                ? payload.get("kitName").asText(null) : null;
        int bet = payload.path("bet").asInt(0);
        String sourceServer = payload.path("serverName").asText(null);

        if (playerIdRaw == null || playerName == null || sourceServer == null) {
            return null;
        }

        try {
            UUID playerId = UUID.fromString(playerIdRaw);
            NetworkQueue queue = new NetworkQueue(playerId, playerName, sourceServer, kitName, bet);
            queue.setJoinedAt(payload.path("timestamp").asLong(System.currentTimeMillis()));

            if (payload.has("partyMembers")) {
                List<UUID> members = new ArrayList<>();
                payload.get("partyMembers").forEach(node -> {
                    try {
                        members.add(UUID.fromString(node.asText()));
                    } catch (IllegalArgumentException ignored) {
                    }
                });
                queue.setPartyMembers(members);
            }

            return queue;
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private boolean isDatabaseReady() {
        return networkEnabled && databaseManager != null && databaseManager.isConnected();
    }

    public boolean isDatabaseConnected() {
        return isDatabaseReady();
    }

    public CompletableFuture<List<NetworkArena>> getAvailableArenas(String kitName) {
        if (!networkEnabled) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        List<NetworkArena> arenas = new ArrayList<>();
        for (NetworkArena arena : networkArenas.values()) {
            if (!arena.isAvailable()) {
                continue;
            }
            if (kitName == null || arena.isBoundless() || arena.isBound(kitName)) {
                arenas.add(arena);
            }
        }
        return CompletableFuture.completedFuture(arenas);
    }

    public void syncArena(ArenaImpl arena) {
        if (!isDatabaseReady()) {
            return;
        }

    NetworkArena networkArena = new NetworkArena(arena.getName(), serverName);
    networkArena.setAvailable(arena.isAvailable());
    networkArena.setDisabled(arena.isDisabled());

    List<String> boundKits = arena.getKits().stream()
        .filter(Objects::nonNull)
        .map(KitImpl::getName)
        .collect(Collectors.toList());
    networkArena.setBoundKits(boundKits);

    Map<String, Object> serializedPositions = serializeArenaPositions(arena);
    networkArena.setPositions(serializedPositions);
    networkArena.setLastUpdate(System.currentTimeMillis());

        databaseManager.saveArena(networkArena)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to save arena to network: " + throwable.getMessage());
                    return null;
                });

        ObjectNode payload = mapper.createObjectNode();
        payload.put("serverName", serverName);
        payload.put("arenaName", arena.getName());
        payload.put("action", "update");
        payload.put("timestamp", System.currentTimeMillis());

        ObjectNode arenaData = mapper.createObjectNode();
        arenaData.put("available", arena.isAvailable());
        arenaData.put("disabled", arena.isDisabled());
        arenaData.put("lastUpdate", System.currentTimeMillis());

        if (!boundKits.isEmpty()) {
            ArrayNode kits = mapper.createArrayNode();
            boundKits.forEach(kits::add);
            arenaData.set("boundKits", kits);
        }

        if (!serializedPositions.isEmpty()) {
            ObjectNode positionsNode = mapper.createObjectNode();
            serializedPositions.forEach((key, value) -> positionsNode.set(key, mapper.valueToTree(value)));
            arenaData.set("positions", positionsNode);
        }

        payload.set("arenaData", arenaData);

        databaseManager.publishEvent(CHANNEL_ARENA_UPDATE, payload)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to broadcast arena update: " + throwable.getMessage());
                    return -1L;
                });
    }

    public void addToNetworkQueue(Player player, String kitName, int bet) {
        if (!isDatabaseReady()) {
            return;
        }

        NetworkQueue entry = new NetworkQueue(player.getUniqueId(), player.getName(), serverName, kitName, bet);
        entry.setJoinedAt(System.currentTimeMillis());

        Party party = plugin.getPartyManager().get(player);
        if (party != null && party.size() > 1) {
            List<UUID> members = party.getMembers().stream()
                    .map(PartyMember::getUuid)
                    .collect(Collectors.toList());
            if (!members.isEmpty()) {
                entry.setPartyMembers(members);
            }
        }

        databaseManager.addQueueEntry(entry)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to add player to network queue: " + throwable.getMessage());
                    return null;
                });

        ObjectNode payload = mapper.createObjectNode();
        payload.put("action", "join");
        payload.put("playerId", player.getUniqueId().toString());
        payload.put("playerName", player.getName());
        if (kitName != null) {
            payload.put("kitName", kitName);
        } else {
            payload.putNull("kitName");
        }
        payload.put("bet", bet);
        payload.put("serverName", serverName);
        payload.put("timestamp", System.currentTimeMillis());

        if (entry.isInParty() && entry.getPartyMembers() != null) {
            ArrayNode partyMembers = mapper.createArrayNode();
            entry.getPartyMembers().forEach(member -> partyMembers.add(member.toString()));
            payload.set("partyMembers", partyMembers);
        }

        databaseManager.publishEvent(CHANNEL_QUEUE_UPDATE, payload)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to publish queue join event: " + throwable.getMessage());
                    return -1L;
                });
    }

    public void removeFromNetworkQueue(Player player, String kitName, int bet) {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.removeQueueEntry(player.getUniqueId(), kitName, bet)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to remove player from network queue: " + throwable.getMessage());
                    return null;
                });

        ObjectNode payload = mapper.createObjectNode();
        payload.put("action", "leave");
        payload.put("playerId", player.getUniqueId().toString());
        payload.put("playerName", player.getName());
        if (kitName != null) {
            payload.put("kitName", kitName);
        } else {
            payload.putNull("kitName");
        }
        payload.put("bet", bet);
        payload.put("serverName", serverName);
        payload.put("timestamp", System.currentTimeMillis());

        databaseManager.publishEvent(CHANNEL_QUEUE_UPDATE, payload)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to publish queue leave event: " + throwable.getMessage());
                    return -1L;
                });
    }

    public CompletableFuture<Optional<String>> findPlayerServer(String playerName) {
        if (!isDatabaseReady()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        return databaseManager.getPlayerServerByName(playerName)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to lookup player server: " + throwable.getMessage());
                    return Optional.empty();
                });
    }

    public void updatePlayerLocation(Player player) {
        if (!isDatabaseReady()) {
            return;
        }

        long timestamp = System.currentTimeMillis();
        databaseManager.upsertPlayer(player.getUniqueId(), player.getName(), serverName, timestamp)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to update player location: " + throwable.getMessage());
                    return null;
                });

        ObjectNode payload = mapper.createObjectNode();
        payload.put("playerId", player.getUniqueId().toString());
        payload.put("playerName", player.getName());
        payload.put("serverName", serverName);
        payload.put("timestamp", timestamp);

        databaseManager.publishEvent(CHANNEL_PLAYER_JOIN, payload)
                .exceptionally(throwable -> {
                    if (config.isNetworkDebugMode()) {
                        plugin.warn("Failed to publish player join event: " + throwable.getMessage());
                    }
                    return -1L;
                });
    }

    public void handlePlayerQuit(Player player) {
        if (!isDatabaseReady()) {
            return;
        }

        databaseManager.removePlayer(player.getUniqueId())
                .exceptionally(throwable -> {
                    plugin.warn("Failed to remove player from network tracking: " + throwable.getMessage());
                    return null;
                });

        ObjectNode payload = mapper.createObjectNode();
        payload.put("playerId", player.getUniqueId().toString());
        payload.put("playerName", player.getName());
        payload.put("serverName", serverName);
        payload.put("timestamp", System.currentTimeMillis());

        databaseManager.publishEvent(CHANNEL_PLAYER_LEAVE, payload)
                .exceptionally(throwable -> {
                    if (config.isNetworkDebugMode()) {
                        plugin.warn("Failed to publish player leave event: " + throwable.getMessage());
                    }
                    return -1L;
                });
    }

    public CompletableFuture<Boolean> requestPlayerTransfer(String playerName, String reason) {
        if (!isDatabaseReady()) {
            return CompletableFuture.completedFuture(false);
        }

        return findPlayerServer(playerName).thenCompose(serverOpt -> {
            if (serverOpt.isEmpty()) {
                return CompletableFuture.completedFuture(false);
            }

            ObjectNode payload = mapper.createObjectNode();
            payload.put("playerName", playerName);
            payload.put("targetServer", serverName);
            payload.put("reason", reason);
            payload.put("timestamp", System.currentTimeMillis());

            Player online = Bukkit.getPlayerExact(playerName);
            if (online != null) {
                payload.put("playerId", online.getUniqueId().toString());
            }

            return databaseManager.publishEvent(CHANNEL_PLAYER_TRANSFER, payload)
                    .thenApply(id -> id > 0)
                    .exceptionally(throwable -> {
                        plugin.warn("Failed to publish player transfer request: " + throwable.getMessage());
                        return false;
                    });
        });
    }

    private void checkForCrossServerMatch(NetworkQueue remotePlayer) {
        if (remotePlayer == null || !isDatabaseReady()) {
            return;
        }

        refreshManagerReferences();

        if (queueManager == null || duelManager == null) {
            return;
        }

        if (remotePlayer.isInParty()) {
            // TODO: Support party-based cross-server duels
            if (config.isNetworkDebugMode()) {
                plugin.warn("Skipping cross-server match for party member " + remotePlayer.getPlayerName());
            }
            return;
        }

        Queue queue = resolveQueue(remotePlayer.getKitName(), remotePlayer.getBet());
        if (queue == null) {
            return;
        }

        QueueEntry localEntry = findEligibleLocalEntry(queue, remotePlayer.getPlayerId());
        if (localEntry == null) {
            return;
        }

        Player localPlayer = localEntry.getPlayer();
        if (localPlayer == null || !localPlayer.isOnline()) {
            return;
        }

        Party localParty = partyManager != null ? partyManager.get(localPlayer) : null;
        if (localParty != null && localParty.size() > 1) {
            // Parties require symmetrical support which is not yet implemented
            if (config.isNetworkDebugMode()) {
                plugin.warn("Skipping cross-server match for queued party player " + localPlayer.getName());
            }
            return;
        }

        Settings baseSettings = new Settings(plugin);
        baseSettings.setBet(queue.getBet());

        KitImpl queueKit = queue.getKit() instanceof KitImpl kitImpl ? kitImpl : null;
        KitImpl targetKit = queueKit;
        if (targetKit == null && remotePlayer.getKitName() != null && kitManager != null) {
            targetKit = kitManager.get(remotePlayer.getKitName());
        }

        if (targetKit != null) {
            baseSettings.setKit(targetKit);
        } else {
            baseSettings.setOwnInventory(true);
        }

        if (localParty != null) {
            baseSettings.setSenderParty(localParty);
        }

        CachedInfo info = cloneCachedInfo(localEntry.getInfo());
        if (info != null) {
            baseSettings.getCache().put(localPlayer.getUniqueId(), info);
        }

        UUID requestId = UUID.randomUUID();

        if (!queue.removeEntrySilently(localEntry)) {
            return;
        }

        removeFromNetworkQueue(localPlayer, queueKit != null ? queueKit.getName() : null, queue.getBet());

        PendingLocalMatch localMatch = new PendingLocalMatch(requestId, queue, localPlayer, info,
                baseSettings, queueKit != null ? queueKit.getName() : null, queue.getBet(), System.currentTimeMillis());

        pendingLocalMatches.put(requestId, localMatch);

        ObjectNode payload = mapper.createObjectNode();
        payload.put("requestId", requestId.toString());
        payload.put("hostServer", serverName);
        payload.put("remoteServer", remotePlayer.getServerName());
        payload.put("bet", remotePlayer.getBet());
        payload.put("timestamp", System.currentTimeMillis());

        if (remotePlayer.getKitName() != null) {
            payload.put("kitName", remotePlayer.getKitName());
        } else {
            payload.putNull("kitName");
        }

        ObjectNode localNode = payload.putObject("local");
        localNode.put("playerId", localPlayer.getUniqueId().toString());
        localNode.put("playerName", localPlayer.getName());
        localNode.put("serverName", serverName);

        ObjectNode remoteNode = payload.putObject("remote");
        remoteNode.put("playerId", remotePlayer.getPlayerId().toString());
        remoteNode.put("playerName", remotePlayer.getPlayerName());
        remoteNode.put("serverName", remotePlayer.getServerName());

        String kitDisplay = queueKit != null ? queueKit.getName() : lang.getMessage("GENERAL.none");
        lang.sendMessage(localPlayer, "QUEUE.found-opponent",
                "name", remotePlayer.getPlayerName(),
                "kit", kitDisplay,
                "bet_amount", queue.getBet());

        databaseManager.publishEvent(CHANNEL_DUEL_REQUEST, payload)
                .thenAccept(id -> {
                    if (config.isNetworkDebugMode()) {
                        plugin.info("Published cross-server duel request " + requestId + " for " + localPlayer.getName());
                    }
                })
                .exceptionally(throwable -> {
                    plugin.warn("Failed to publish cross-server duel request: " + throwable.getMessage());
                    pendingLocalMatches.remove(requestId);
                    plugin.doSync(() -> restoreLocalPlayer(localMatch));
                    return null;
                });
    }

    public boolean isNetworkEnabled() {
        return networkEnabled;
    }

    private Map<String, Object> serializeArenaPositions(ArenaImpl arena) {
        Map<String, Object> serialized = new HashMap<>();
        arena.getPositions().forEach((index, location) -> {
            if (location != null) {
                serialized.put(String.valueOf(index), serializeLocation(location));
            }
        });
        return serialized;
    }

    private Map<String, Object> serializeLocation(Location location) {
        Map<String, Object> result = new HashMap<>();
        if (location.getWorld() != null) {
            result.put("world", location.getWorld().getName());
        }
        result.put("x", location.getX());
        result.put("y", location.getY());
        result.put("z", location.getZ());
        result.put("yaw", location.getYaw());
        result.put("pitch", location.getPitch());
        return result;
    }

    private String buildQueueKey(String kitName, int bet) {
        return (kitName == null ? "__none__" : kitName) + ":" + bet;
    }

    public Collection<NetworkQueue> getQueueEntries(String kitName, int bet) {
        String key = buildQueueKey(kitName, bet);
        return networkQueues.getOrDefault(key, Collections.emptyList());
    }
}