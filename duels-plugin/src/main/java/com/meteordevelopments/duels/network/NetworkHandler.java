package com.meteordevelopments.duels.network;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.meteordevelopments.duels.DuelsPlugin;
import com.meteordevelopments.duels.arena.ArenaImpl;
import com.meteordevelopments.duels.arena.ArenaManagerImpl;
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
import com.meteordevelopments.duels.util.TextBuilder;
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
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import net.md_5.bungee.api.chat.ClickEvent;
import net.md_5.bungee.api.chat.HoverEvent.Action;

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
    private ArenaManagerImpl arenaManager;

    private DatabaseManager databaseManager;
    private final List<ScheduledTask> scheduledTasks = new ArrayList<>();

    private final Map<String, NetworkArena> networkArenas = new ConcurrentHashMap<>();
    private final Map<String, List<NetworkQueue>> networkQueues = new ConcurrentHashMap<>();
    private final Map<UUID, PendingLocalMatch> pendingLocalMatches = new ConcurrentHashMap<>();
    private final Map<UUID, PendingMatchStart> pendingMatchStarts = new ConcurrentHashMap<>();
    private final Map<UUID, RemoteChallenge> incomingChallengesById = new ConcurrentHashMap<>();
    private final Map<String, Map<String, RemoteChallenge>> incomingChallengesByTarget = new ConcurrentHashMap<>();

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
    incomingChallengesById.clear();
    incomingChallengesByTarget.clear();

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
        if (arenaManager == null && plugin.getArenaManager() instanceof ArenaManagerImpl impl) {
            arenaManager = impl;
        }
    }

    private boolean hasLocalAvailableArena(String kitName) {
        refreshManagerReferences();
        if (arenaManager == null) {
            return false;
        }

        KitImpl kit = null;
        if (kitName != null && kitManager != null) {
            kit = kitManager.get(kitName);
        }

        for (ArenaImpl arena : arenaManager.getArenasImpl()) {
            if (arena == null || !arena.isAvailable()) {
                continue;
            }
            if (arenaManager.isSelectable(kit, arena)) {
                return true;
            }
        }

        return false;
    }

    private boolean hasRemoteAvailableArena(String server, String kitName) {
        if (server == null || server.isEmpty()) {
            return false;
        }

        return networkArenas.values().stream()
            .filter(arena -> server.equalsIgnoreCase(arena.getServerName()))
            .anyMatch(arena -> isRemoteArenaSuitable(arena, kitName));
    }

    private boolean isRemoteArenaSuitable(NetworkArena arena, String kitName) {
        if (arena == null || !arena.isAvailable()) {
            return false;
        }

        if (kitName == null || kitName.isEmpty()) {
            return true;
        }

        if (arena.isBoundless()) {
            return true;
        }

        return arena.isBound(kitName);
    }

    private String findAnyRemoteArenaServer(String kitName) {
        return networkArenas.values().stream()
            .filter(arena -> isRemoteArenaSuitable(arena, kitName))
            .map(NetworkArena::getServerName)
            .findFirst()
            .orElse(null);
    }

    private String selectHostServer(String preferredHost, String secondaryHost, String kitName) {
        if (preferredHost != null && !preferredHost.isBlank()) {
            if (preferredHost.equalsIgnoreCase(serverName)) {
                if (hasLocalAvailableArena(kitName)) {
                    return preferredHost;
                }
            } else if (hasRemoteAvailableArena(preferredHost, kitName)) {
                return preferredHost;
            }
        }

        if (secondaryHost != null && !secondaryHost.isBlank() && !secondaryHost.equalsIgnoreCase(preferredHost)) {
            if (secondaryHost.equalsIgnoreCase(serverName)) {
                if (hasLocalAvailableArena(kitName)) {
                    return secondaryHost;
                }
            } else if (hasRemoteAvailableArena(secondaryHost, kitName)) {
                return secondaryHost;
            }
        }

        String fallback = findAnyRemoteArenaServer(kitName);
        if (fallback != null) {
            return fallback;
        }

        return serverName;
    }

    private boolean hasAnyArenaAvailable(String kitName) {
        if (hasLocalAvailableArena(kitName)) {
            return true;
        }

        return networkArenas.values().stream()
            .anyMatch(arena -> isRemoteArenaSuitable(arena, kitName));
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

    scheduledTasks.add(plugin.doAsyncRepeat(this::cleanupExpiredChallenges,
        TICKS_PER_SECOND * 60, TICKS_PER_SECOND * 120));

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
            ObjectNode payload = event.payloadAsObjectNode(mapper);
            String action = payload.path("action").asText("");

            if ("challenge".equalsIgnoreCase(action)) {
                handleIncomingChallenge(payload);
                continue;
            }

            if ("challenge-accept".equalsIgnoreCase(action)) {
                handleChallengeAccept(payload);
                continue;
            }

            if ("challenge-deny".equalsIgnoreCase(action)) {
                handleChallengeDeny(payload);
                continue;
            }

            if ("challenge-failed".equalsIgnoreCase(action)) {
                handleChallengeFailed(payload);
                continue;
            }

            if (config.isNetworkDebugMode()) {
                plugin.getLogger().info("Received duel request event: " + payload);
            }
        }
    }

    private void handleIncomingChallenge(ObjectNode payload) {
        if (!networkEnabled) {
            return;
        }

        String targetServer = payload.path("targetServer").asText("");
        if (!serverName.equalsIgnoreCase(targetServer)) {
            return;
        }

        UUID requestId = parseUuid(payload.path("requestId").asText(null));
        if (requestId == null) {
            return;
        }

        String targetName = payload.path("targetName").asText(null);
        if (targetName == null || targetName.isEmpty()) {
            return;
        }

        String challengerName = payload.path("challengerName").asText("Unknown");
        UUID challengerId = parseUuid(payload.path("challengerId").asText(null));
        String hostServer = payload.path("hostServer").asText(serverName);
        String sourceServer = payload.path("sourceServer").asText(hostServer);
        String kitName = payload.hasNonNull("kitName") ? payload.get("kitName").asText() : null;
        String arenaName = payload.hasNonNull("arenaName") ? payload.get("arenaName").asText() : null;
        boolean ownInventory = payload.path("ownInventory").asBoolean(false);
        boolean itemBetting = payload.path("itemBetting").asBoolean(false);
        int bet = payload.path("bet").asInt(0);

        plugin.doSync(() -> {
            Player target = Bukkit.getPlayerExact(targetName);
            if (target == null || !target.isOnline()) {
                notifyChallengeFailure(requestId, hostServer, sourceServer, targetServer, "offline", challengerName, targetName);
                return;
            }

            RemoteChallenge challenge = new RemoteChallenge(requestId, target.getName(), challengerName, challengerId,
                    hostServer, sourceServer, targetServer, kitName, arenaName, ownInventory, itemBetting, bet,
                    System.currentTimeMillis());

            storeRemoteChallenge(challenge);
            sendRemoteChallengeMessages(target, challenge);
        });
    }

    private void handleChallengeAccept(ObjectNode payload) {
        if (!networkEnabled) {
            return;
        }

        String hostServer = payload.path("hostServer").asText("");
        if (!serverName.equalsIgnoreCase(hostServer)) {
            return;
        }

        UUID requestId = parseUuid(payload.path("requestId").asText(null));
        if (requestId == null) {
            return;
        }

        PendingLocalMatch localMatch = pendingLocalMatches.get(requestId);
        if (localMatch == null) {
            return;
        }

        plugin.doSync(() -> {
            Player challenger = localMatch.localPlayer();
            if (challenger == null || !challenger.isOnline()) {
                pendingLocalMatches.remove(requestId);
                notifyChallengeFailure(requestId, hostServer, payload.path("sourceServer").asText(null),
                        payload.path("targetServer").asText(null), "challenger-offline",
                        payload.path("challengerName").asText(null), payload.path("targetPlayerName").asText(null));
                return;
            }

            Settings settingsCopy = localMatch.copySettings();
            String kitDisplay = settingsCopy.getKit() != null ? settingsCopy.getKit().getName() : lang.getMessage("GENERAL.not-selected");
            String arenaDisplay = settingsCopy.getArena() != null ? settingsCopy.getArena().getName() : lang.getMessage("GENERAL.random");
            double betDisplay = settingsCopy.getBet();
            String itemBettingDisplay = settingsCopy.isItemBetting() ? lang.getMessage("GENERAL.enabled") : lang.getMessage("GENERAL.disabled");

            lang.sendMessage(challenger, "COMMAND.duel.request.accept.sender",
                    "name", payload.path("targetPlayerName").asText("Unknown"),
                    "kit", kitDisplay,
                    "arena", arenaDisplay,
                    "bet_amount", betDisplay,
                    "item_betting", itemBettingDisplay);

            forwardToProxy(localMatch, payload);
        });
    }

    private void handleChallengeDeny(ObjectNode payload) {
        if (!networkEnabled) {
            return;
        }

        String hostServer = payload.path("hostServer").asText("");
        if (!serverName.equalsIgnoreCase(hostServer)) {
            return;
        }

        UUID requestId = parseUuid(payload.path("requestId").asText(null));
        if (requestId == null) {
            return;
        }

        PendingLocalMatch localMatch = pendingLocalMatches.remove(requestId);
        if (localMatch == null) {
            return;
        }

        plugin.doSync(() -> {
            Player challenger = localMatch.localPlayer();
            if (challenger != null && challenger.isOnline()) {
                lang.sendMessage(challenger, "COMMAND.duel.request.deny.sender",
                        "name", payload.path("targetPlayerName").asText("Unknown"));
            }
            restoreLocalPlayer(localMatch);
        });
    }

    private void handleChallengeFailed(ObjectNode payload) {
        if (!networkEnabled) {
            return;
        }

        String hostServer = payload.path("hostServer").asText("");
        if (!serverName.equalsIgnoreCase(hostServer)) {
            return;
        }

        UUID requestId = parseUuid(payload.path("requestId").asText(null));
        if (requestId == null) {
            return;
        }

        PendingLocalMatch localMatch = pendingLocalMatches.remove(requestId);
        if (localMatch == null) {
            return;
        }

        String reason = payload.path("reason").asText("unknown");
        String targetName = payload.path("targetPlayerName").asText("Unknown");

        plugin.doSync(() -> {
            Player challenger = localMatch.localPlayer();
            if (challenger != null && challenger.isOnline()) {
                switch (reason.toLowerCase(Locale.ROOT)) {
                    case "offline" -> lang.sendMessage(challenger, "ERROR.player.no-longer-online", "name", targetName);
                    case "expired" -> lang.sendMessage(challenger, "ERROR.duel.cross-server-timeout");
                    case "denied" -> lang.sendMessage(challenger, "COMMAND.duel.request.deny.sender", "name", targetName);
                    default -> lang.sendMessage(challenger, "ERROR.duel.cross-server-request-failed");
                }
            }
            restoreLocalPlayer(localMatch);
        });
    }

    private void storeRemoteChallenge(RemoteChallenge challenge) {
        incomingChallengesById.put(challenge.requestId(), challenge);
        incomingChallengesByTarget
                .computeIfAbsent(challenge.targetKey(), key -> new ConcurrentHashMap<>())
                .put(challenge.challengerKey(), challenge);
    }

    private RemoteChallenge getRemoteChallenge(String targetName, String challengerName) {
        if (targetName == null || challengerName == null) {
            return null;
        }

        Map<String, RemoteChallenge> map = incomingChallengesByTarget.get(targetName.toLowerCase(Locale.ROOT));
        if (map == null) {
            return null;
        }

        RemoteChallenge challenge = map.get(challengerName.toLowerCase(Locale.ROOT));
        if (challenge == null) {
            return null;
        }

        if (!incomingChallengesById.containsKey(challenge.requestId())) {
            map.remove(challenge.challengerKey());
            return null;
        }

        return challenge;
    }

    private void removeRemoteChallenge(RemoteChallenge challenge) {
        incomingChallengesById.remove(challenge.requestId());

        Map<String, RemoteChallenge> map = incomingChallengesByTarget.get(challenge.targetKey());
        if (map != null) {
            map.remove(challenge.challengerKey());
            if (map.isEmpty()) {
                incomingChallengesByTarget.remove(challenge.targetKey());
            }
        }
    }

    private List<RemoteChallenge> removeChallengesForTarget(String targetName) {
        if (targetName == null) {
            return Collections.emptyList();
        }

        Map<String, RemoteChallenge> map = incomingChallengesByTarget.remove(targetName.toLowerCase(Locale.ROOT));
        if (map == null) {
            return Collections.emptyList();
        }

        List<RemoteChallenge> removed = new ArrayList<>(map.values());
        removed.forEach(challenge -> incomingChallengesById.remove(challenge.requestId()));
        return removed;
    }

    private void sendRemoteChallengeMessages(Player target, RemoteChallenge challenge) {
        String kit = challenge.kitName() != null ? challenge.kitName() : lang.getMessage("GENERAL.not-selected");
        String ownInventory = challenge.ownInventory() ? lang.getMessage("GENERAL.enabled") : lang.getMessage("GENERAL.disabled");
        String arena = challenge.arenaName() != null ? challenge.arenaName() : lang.getMessage("GENERAL.random");
        int bet = challenge.bet();
        String itemBetting = challenge.itemBetting() ? lang.getMessage("GENERAL.enabled") : lang.getMessage("GENERAL.disabled");

        lang.sendMessage(target, "COMMAND.duel.request.send.receiver",
                "name", challenge.challengerName(),
                "kit", kit,
                "own_inventory", ownInventory,
                "arena", arena,
                "bet_amount", bet,
                "item_betting", itemBetting);

        TextBuilder
                .of(lang.getMessage("COMMAND.duel.request.send.clickable-text.info.text"),
                        null, null,
                        Action.SHOW_TEXT, lang.getMessage("COMMAND.duel.request.send.clickable-text.info.hover-text"))
                .add(lang.getMessage("COMMAND.duel.request.send.clickable-text.accept.text"),
                        ClickEvent.Action.RUN_COMMAND, "/duel accept " + challenge.challengerName(),
                        Action.SHOW_TEXT, lang.getMessage("COMMAND.duel.request.send.clickable-text.accept.hover-text"))
                .add(lang.getMessage("COMMAND.duel.request.send.clickable-text.deny.text"),
                        ClickEvent.Action.RUN_COMMAND, "/duel deny " + challenge.challengerName(),
                        Action.SHOW_TEXT, lang.getMessage("COMMAND.duel.request.send.clickable-text.deny.hover-text"))
                .send(Collections.singleton(target));
    }

    private void forwardToProxy(PendingLocalMatch localMatch, ObjectNode responsePayload) {
        if (!isDatabaseReady()) {
            return;
        }

        UUID requestId = localMatch.requestId();
        Player challenger = localMatch.localPlayer();

        ObjectNode forward = mapper.createObjectNode();
        forward.put("requestId", requestId.toString());
        forward.put("hostServer", serverName);

        String remoteServer = responsePayload.path("targetServer").asText(serverName);
        forward.put("remoteServer", remoteServer);

        if (responsePayload.hasNonNull("kitName")) {
            forward.put("kitName", responsePayload.get("kitName").asText());
        } else if (localMatch.kitName() != null) {
            forward.put("kitName", localMatch.kitName());
        } else {
            forward.putNull("kitName");
        }

        forward.put("bet", responsePayload.path("bet").asInt(localMatch.bet()));
        forward.put("timestamp", System.currentTimeMillis());

        ObjectNode localNode = forward.putObject("local");
        if (challenger != null) {
            localNode.put("playerId", challenger.getUniqueId().toString());
            localNode.put("playerName", challenger.getName());
        } else if (localMatch.localPlayerId() != null) {
            localNode.put("playerId", localMatch.localPlayerId().toString());
            localNode.put("playerName", lang.getMessage("GENERAL.none"));
        }
        localNode.put("serverName", serverName);

        ObjectNode remoteNode = forward.putObject("remote");
        String remotePlayerId = responsePayload.path("targetPlayerId").asText(null);
        if (remotePlayerId != null) {
            remoteNode.put("playerId", remotePlayerId);
        }
        remoteNode.put("playerName", responsePayload.path("targetPlayerName").asText("Unknown"));
        remoteNode.put("serverName", remoteServer);

        databaseManager.publishEvent(CHANNEL_DUEL_REQUEST, forward)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to forward cross-server duel request: " + throwable.getMessage());
                    pendingLocalMatches.remove(requestId);
                    if (challenger != null && challenger.isOnline()) {
                        plugin.doSync(() -> lang.sendMessage(challenger, "ERROR.duel.cross-server-request-failed"));
                    }
                    return null;
                });
    }

    private void notifyChallengeFailure(UUID requestId, String hostServer, String sourceServer,
                                        String targetServer, String reason, String challengerName,
                                        String targetName) {
        if (!isDatabaseReady() || requestId == null) {
            return;
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("action", "challenge-failed");
        payload.put("requestId", requestId.toString());
        payload.put("hostServer", hostServer != null ? hostServer : serverName);
        if (sourceServer != null) {
            payload.put("sourceServer", sourceServer);
        }
        payload.put("targetServer", targetServer != null ? targetServer : serverName);
        payload.put("reason", reason);
        if (challengerName != null) {
            payload.put("challengerName", challengerName);
        }
        if (targetName != null) {
            payload.put("targetPlayerName", targetName);
        }
        payload.put("timestamp", System.currentTimeMillis());

        databaseManager.publishEvent(CHANNEL_DUEL_REQUEST, payload)
                .exceptionally(throwable -> {
                    if (config.isNetworkDebugMode()) {
                        plugin.warn("Failed to publish challenge failure: " + throwable.getMessage());
                    }
                    return null;
                });
    }

    private void notifyChallengeFailure(RemoteChallenge challenge, String reason) {
        notifyChallengeFailure(challenge.requestId(), challenge.hostServer(), challenge.sourceServer(),
                challenge.targetServer(), reason, challenge.challengerName(), challenge.targetName());
    }

    private void cleanupExpiredChallenges() {
        if (incomingChallengesById.isEmpty()) {
            return;
        }

        long expirationSeconds = Math.max(5, config.getExpiration());
        long expirationMillis = expirationSeconds * 1000L;
        long now = System.currentTimeMillis();

        List<RemoteChallenge> expired = new ArrayList<>();
        for (RemoteChallenge challenge : incomingChallengesById.values()) {
            if (now - challenge.createdAt() >= expirationMillis) {
                expired.add(challenge);
            }
        }

        if (expired.isEmpty()) {
            return;
        }

        expired.forEach(this::removeRemoteChallenge);
        expired.forEach(challenge -> notifyChallengeFailure(challenge, "expired"));
    }

    private void sendChallengeSentMessage(Player challenger, String targetName, Settings settings) {
        String kit = settings.getKit() != null ? settings.getKit().getName() : lang.getMessage("GENERAL.not-selected");
        String ownInventory = settings.isOwnInventory() ? lang.getMessage("GENERAL.enabled") : lang.getMessage("GENERAL.disabled");
        String arena = settings.getArena() != null ? settings.getArena().getName() : lang.getMessage("GENERAL.random");
        double bet = settings.getBet();
        String itemBetting = settings.isItemBetting() ? lang.getMessage("GENERAL.enabled") : lang.getMessage("GENERAL.disabled");

        lang.sendMessage(challenger, "COMMAND.duel.request.send.sender",
                "name", targetName,
                "kit", kit,
                "own_inventory", ownInventory,
                "arena", arena,
                "bet_amount", bet,
                "item_betting", itemBetting);
    }

    private void initiateCrossServerChallenge(Player challenger, String targetName, String targetServer, Settings settings) {
        if (!challenger.isOnline()) {
            return;
        }

    Settings baseSettings = settings.lightCopy();
    baseSettings.setRemoteTargetName(targetName);
    baseSettings.getCache().computeIfAbsent(challenger.getUniqueId(), uuid ->
        new CachedInfo(challenger.getLocation() != null ? challenger.getLocation().clone() : null, null));

    String kitName = baseSettings.getKit() != null ? baseSettings.getKit().getName() : null;
    if (!hasAnyArenaAvailable(kitName)) {
        lang.sendMessage(challenger, "ERROR.queue.no-arena-available");
        return;
    }

    String hostServer = selectHostServer(serverName, targetServer, kitName);

    CachedInfo cachedInfo = cloneCachedInfo(baseSettings.getCache().get(challenger.getUniqueId()));
    UUID requestId = UUID.randomUUID();

    PendingLocalMatch localMatch = new PendingLocalMatch(requestId, null, challenger, cachedInfo,
        baseSettings, kitName, baseSettings.getBet(), System.currentTimeMillis(), hostServer,
        null, null);

    pendingLocalMatches.put(requestId, localMatch);

        ObjectNode payload = mapper.createObjectNode();
        payload.put("action", "challenge");
        payload.put("requestId", requestId.toString());
    payload.put("hostServer", hostServer);
        payload.put("sourceServer", serverName);
        payload.put("targetServer", targetServer);
        payload.put("challengerId", challenger.getUniqueId().toString());
        payload.put("challengerName", challenger.getName());
        payload.put("targetName", targetName);
        payload.put("bet", baseSettings.getBet());
        payload.put("ownInventory", baseSettings.isOwnInventory());
        payload.put("itemBetting", baseSettings.isItemBetting());
        if (baseSettings.getKit() != null) {
            payload.put("kitName", baseSettings.getKit().getName());
        } else {
            payload.putNull("kitName");
        }
        if (baseSettings.getArena() != null) {
            payload.put("arenaName", baseSettings.getArena().getName());
        } else {
            payload.putNull("arenaName");
        }
        payload.put("timestamp", System.currentTimeMillis());

        databaseManager.publishEvent(CHANNEL_DUEL_REQUEST, payload)
                .thenAccept(id -> {
                    if (id > 0) {
                        sendChallengeSentMessage(challenger, targetName, baseSettings);
                    } else {
                        pendingLocalMatches.remove(requestId);
                        plugin.doSync(() -> lang.sendMessage(challenger, "ERROR.duel.cross-server-request-failed"));
                    }
                })
                .exceptionally(throwable -> {
                    pendingLocalMatches.remove(requestId);
                    plugin.warn("Failed to publish cross-server duel request: " + throwable.getMessage());
                    plugin.doSync(() -> lang.sendMessage(challenger, "ERROR.duel.cross-server-request-failed"));
                    return null;
                });
    }

    public boolean sendCrossServerChallenge(Player challenger, String targetName, Settings settings) {
        if (!isDatabaseReady()) {
            lang.sendMessage(challenger, "ERROR.network.unavailable");
            return false;
        }

        if (settings.isPartyDuel()) {
            lang.sendMessage(challenger, "ERROR.duel.cross-server-party");
            return false;
        }

        if (settings.isItemBetting()) {
            lang.sendMessage(challenger, "ERROR.setting.disabled-option", "option", lang.getMessage("GENERAL.item-betting"));
            return false;
        }

        findPlayerServer(targetName).thenAccept(serverOpt -> {
            if (serverOpt.isEmpty()) {
                plugin.doSync(() -> lang.sendMessage(challenger, "ERROR.player.not-found", "name", targetName));
                return;
            }

            String targetServer = serverOpt.get();
            if (serverName.equalsIgnoreCase(targetServer)) {
                plugin.doSync(() -> lang.sendMessage(challenger, "ERROR.player.not-found", "name", targetName));
                return;
            }

            plugin.doSync(() -> initiateCrossServerChallenge(challenger, targetName, targetServer, settings));
        });

        return true;
    }

    public boolean acceptRemoteChallenge(Player player, String challengerName) {
        if (!networkEnabled) {
            return false;
        }

        RemoteChallenge challenge = getRemoteChallenge(player.getName(), challengerName);
        if (challenge == null) {
            return false;
        }

        removeRemoteChallenge(challenge);

        String kit = challenge.kitName() != null ? challenge.kitName() : lang.getMessage("GENERAL.not-selected");
        String arena = challenge.arenaName() != null ? challenge.arenaName() : lang.getMessage("GENERAL.random");
        double bet = challenge.bet();
        String itemBetting = challenge.itemBetting() ? lang.getMessage("GENERAL.enabled") : lang.getMessage("GENERAL.disabled");

        lang.sendMessage(player, "COMMAND.duel.request.accept.receiver",
                "name", challenge.challengerName(),
                "kit", kit,
                "arena", arena,
                "bet_amount", bet,
                "item_betting", itemBetting);

        ObjectNode payload = mapper.createObjectNode();
        payload.put("action", "challenge-accept");
        payload.put("requestId", challenge.requestId().toString());
        payload.put("hostServer", challenge.hostServer());
        payload.put("sourceServer", serverName);
        payload.put("targetServer", serverName);
        if (challenge.challengerId() != null) {
            payload.put("challengerId", challenge.challengerId().toString());
        }
        payload.put("challengerName", challenge.challengerName());
        payload.put("targetPlayerId", player.getUniqueId().toString());
        payload.put("targetPlayerName", player.getName());
        if (challenge.kitName() != null) {
            payload.put("kitName", challenge.kitName());
        } else {
            payload.putNull("kitName");
        }
        if (challenge.arenaName() != null) {
            payload.put("arenaName", challenge.arenaName());
        } else {
            payload.putNull("arenaName");
        }
        payload.put("ownInventory", challenge.ownInventory());
        payload.put("itemBetting", challenge.itemBetting());
        payload.put("bet", challenge.bet());
        payload.put("timestamp", System.currentTimeMillis());

        databaseManager.publishEvent(CHANNEL_DUEL_REQUEST, payload)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to publish challenge acceptance: " + throwable.getMessage());
                    plugin.doSync(() -> lang.sendMessage(player, "ERROR.duel.cross-server-request-failed"));
                    return null;
                });

        return true;
    }

    public boolean denyRemoteChallenge(Player player, String challengerName) {
        if (!networkEnabled) {
            return false;
        }

        RemoteChallenge challenge = getRemoteChallenge(player.getName(), challengerName);
        if (challenge == null) {
            return false;
        }

        removeRemoteChallenge(challenge);

        lang.sendMessage(player, "COMMAND.duel.request.deny.receiver",
                "name", challenge.challengerName());

        ObjectNode payload = mapper.createObjectNode();
        payload.put("action", "challenge-deny");
        payload.put("requestId", challenge.requestId().toString());
        payload.put("hostServer", challenge.hostServer());
        payload.put("sourceServer", serverName);
        payload.put("targetServer", serverName);
        payload.put("targetPlayerName", player.getName());
        payload.put("challengerName", challenge.challengerName());
        payload.put("timestamp", System.currentTimeMillis());

        databaseManager.publishEvent(CHANNEL_DUEL_REQUEST, payload)
                .exceptionally(throwable -> {
                    plugin.warn("Failed to publish challenge denial: " + throwable.getMessage());
                    return null;
                });

        return true;
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

            UUID requestId = parseUuid(payload.path("requestId").asText(null));
            if (requestId == null) {
                requestId = matchId;
            }

            PendingLocalMatch localMatch = null;
            if (requestId != null) {
                localMatch = pendingLocalMatches.remove(requestId);
            }
            if (localMatch == null && !Objects.equals(requestId, matchId)) {
                localMatch = pendingLocalMatches.remove(matchId);
            }
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
            ? payload.get("kitName").asText() : (localMatch != null ? localMatch.kitName() : null);
        int bet = payload.path("bet").asInt(localMatch != null ? localMatch.bet() : 0);

        String arenaName = payload.has("arenaName") && !payload.get("arenaName").isNull()
            ? payload.get("arenaName").asText()
            : (localMatch != null ? localMatch.selectedArenaName() : null);
        String arenaServerRaw = payload.path("arenaServer").asText(
            localMatch != null ? localMatch.selectedArenaServer() : null);
        String arenaServer = arenaServerRaw != null && !arenaServerRaw.isBlank() ? arenaServerRaw : null;

        PendingMatchStart pending = new PendingMatchStart(matchId, requestId, localMatch, teamA, teamB,
            kitName, bet, CROSS_SERVER_MATCH_TIMEOUT_SECONDS, arenaName, arenaServer);

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

        if (settings.getArena() == null && pending.arenaServer() != null && pending.arenaName() != null) {
            settings.selectRemoteArena(pending.arenaServer(), pending.arenaName());
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
        private final String hostServer;
        private final String selectedArenaName;
        private final String selectedArenaServer;

        PendingLocalMatch(UUID requestId, Queue queue, Player localPlayer, CachedInfo cachedInfo,
                           Settings baseSettings, String kitName, int bet, long createdAt,
                           String hostServer, String selectedArenaName, String selectedArenaServer) {
            this.requestId = requestId;
            this.queue = queue;
            this.localPlayer = localPlayer;
            this.cachedInfo = cachedInfo;
            this.baseSettings = baseSettings;
            this.kitName = kitName;
            this.bet = bet;
            this.createdAt = createdAt;
            this.hostServer = hostServer;
            this.selectedArenaName = selectedArenaName;
            this.selectedArenaServer = selectedArenaServer;
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

        String hostServer() {
            return hostServer;
        }

        String selectedArenaName() {
            return selectedArenaName;
        }

        String selectedArenaServer() {
            return selectedArenaServer;
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
        private final UUID requestId;
        private final PendingLocalMatch localMatch;
        private final List<UUID> teamA;
        private final List<UUID> teamB;
        private final String kitName;
        private final int bet;
        private final long createdAt;
        private final long expiresAt;
        private final String arenaName;
        private final String arenaServer;

        PendingMatchStart(UUID matchId, UUID requestId, PendingLocalMatch localMatch, List<UUID> teamA, List<UUID> teamB,
                          String kitName, int bet, int timeoutSeconds, String arenaName, String arenaServer) {
            this.matchId = matchId;
            this.requestId = requestId;
            this.localMatch = localMatch;
            this.teamA = new ArrayList<>(teamA);
            this.teamB = new ArrayList<>(teamB);
            this.kitName = kitName;
            this.bet = bet;
            this.createdAt = System.currentTimeMillis();
            this.expiresAt = this.createdAt + Math.max(5, timeoutSeconds) * 1000L;
            this.arenaName = arenaName;
            this.arenaServer = arenaServer;
        }

        UUID matchId() {
            return matchId;
        }

        UUID requestId() {
            return requestId;
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

        String arenaName() {
            return arenaName;
        }

        String arenaServer() {
            return arenaServer;
        }
    }

    private static class RemoteChallenge {
        private final UUID requestId;
        private final String targetName;
        private final String challengerName;
        private final UUID challengerId;
        private final String hostServer;
        private final String sourceServer;
        private final String targetServer;
        private final String kitName;
        private final String arenaName;
        private final boolean ownInventory;
        private final boolean itemBetting;
        private final int bet;
        private final long createdAt;

        RemoteChallenge(UUID requestId, String targetName, String challengerName, UUID challengerId,
                        String hostServer, String sourceServer, String targetServer, String kitName,
                        String arenaName, boolean ownInventory, boolean itemBetting, int bet, long createdAt) {
            this.requestId = requestId;
            this.targetName = targetName;
            this.challengerName = challengerName;
            this.challengerId = challengerId;
            this.hostServer = hostServer;
            this.sourceServer = sourceServer;
            this.targetServer = targetServer;
            this.kitName = kitName;
            this.arenaName = arenaName;
            this.ownInventory = ownInventory;
            this.itemBetting = itemBetting;
            this.bet = bet;
            this.createdAt = createdAt;
        }

        UUID requestId() {
            return requestId;
        }

        String targetName() {
            return targetName;
        }

        String challengerName() {
            return challengerName;
        }

        UUID challengerId() {
            return challengerId;
        }

        String hostServer() {
            return hostServer;
        }

        String sourceServer() {
            return sourceServer;
        }

        String targetServer() {
            return targetServer;
        }

        String kitName() {
            return kitName;
        }

        String arenaName() {
            return arenaName;
        }

        boolean ownInventory() {
            return ownInventory;
        }

        boolean itemBetting() {
            return itemBetting;
        }

        int bet() {
            return bet;
        }

        long createdAt() {
            return createdAt;
        }

        String targetKey() {
            return targetName.toLowerCase(Locale.ROOT);
        }

        String challengerKey() {
            return challengerName.toLowerCase(Locale.ROOT);
        }
    }

    private UUID parseUuid(String raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }

        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException ignored) {
            return null;
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
                    return null;
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
                    return null;
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
                    return null;
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
                    return null;
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
                    return null;
                });

        if (!incomingChallengesByTarget.isEmpty()) {
            removeChallengesForTarget(player.getName()).forEach(challenge -> notifyChallengeFailure(challenge, "offline"));
        }
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

        String resolvedKitName = targetKit != null ? targetKit.getName() : remotePlayer.getKitName();

        if (!hasAnyArenaAvailable(resolvedKitName)) {
            lang.sendMessage(localPlayer, "ERROR.queue.no-arena-available");
            return;
        }

        String hostServer = selectHostServer(serverName, remotePlayer.getServerName(), resolvedKitName);

        UUID requestId = UUID.randomUUID();

        if (!queue.removeEntrySilently(localEntry)) {
            return;
        }

        removeFromNetworkQueue(localPlayer, queueKit != null ? queueKit.getName() : null, queue.getBet());

        PendingLocalMatch localMatch = new PendingLocalMatch(requestId, queue, localPlayer, info,
                baseSettings, queueKit != null ? queueKit.getName() : null, queue.getBet(), System.currentTimeMillis(),
                hostServer, null, null);

        pendingLocalMatches.put(requestId, localMatch);

        ObjectNode payload = mapper.createObjectNode();
        payload.put("requestId", requestId.toString());
        payload.put("hostServer", hostServer);
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

    public List<NetworkArena> getRemoteArenasSnapshot() {
        return new ArrayList<>(networkArenas.values());
    }

    public Map<String, List<NetworkQueue>> getNetworkQueuesSnapshot() {
        Map<String, List<NetworkQueue>> snapshot = new HashMap<>();
        networkQueues.forEach((key, value) -> snapshot.put(key, new ArrayList<>(value)));
        return snapshot;
    }
}