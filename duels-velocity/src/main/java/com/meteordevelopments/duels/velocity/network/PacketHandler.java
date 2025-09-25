package com.meteordevelopments.duels.velocity.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.meteordevelopments.duels.common.database.DatabaseManager;
import com.meteordevelopments.duels.velocity.DuelsVelocityPlugin;
import com.velocitypowered.api.proxy.Player;
import com.meteordevelopments.duels.common.network.NetworkQueue;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class PacketHandler {

    private static final String CHANNEL_ARENA_UPDATE = "duels:arena-update";
    private static final String CHANNEL_KIT_UPDATE = "duels:kit-update";
    private static final String CHANNEL_QUEUE_UPDATE = "duels:queue-update";
    private static final String CHANNEL_MATCH_START = "duels:match-start";
    private static final String CHANNEL_MATCH_END = "duels:match-end";
    private static final String CHANNEL_PLAYER_TRANSFER = "duels:player-transfer";

    private final DuelsVelocityPlugin plugin;
    @Getter
    private final DatabaseManager databaseManager;
    private final NetworkManager networkManager;
    private final ObjectMapper mapper = new ObjectMapper();

    public PacketHandler(DuelsVelocityPlugin plugin) {
        this.plugin = plugin;
        this.databaseManager = plugin.getDatabaseManager();
        this.networkManager = plugin.getNetworkManager();
    }

    /* ----------------------------- Arena Management ----------------------------- */

    public CompletableFuture<Void> syncArenaData(String serverName, String arenaName, Map<String, Object> arenaData) {
        if (!isDatabaseReady()) {
            return CompletableFuture.completedFuture(null);
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("serverName", serverName);
        payload.put("arenaName", arenaName);
        payload.put("action", "update");
        payload.put("timestamp", System.currentTimeMillis());
        payload.set("arenaData", mapper.valueToTree(arenaData));

        return databaseManager.publishEvent(CHANNEL_ARENA_UPDATE, payload)
            .thenAccept(ignored -> {
            });
    }

    public CompletableFuture<Void> removeArena(String serverName, String arenaName) {
        if (!isDatabaseReady()) {
            return CompletableFuture.completedFuture(null);
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("serverName", serverName);
        payload.put("arenaName", arenaName);
        payload.put("action", "remove");
        payload.put("timestamp", System.currentTimeMillis());

        return databaseManager.publishEvent(CHANNEL_ARENA_UPDATE, payload)
            .thenAccept(ignored -> {
            });
    }

    /* ----------------------------- Kit Management ----------------------------- */

    public CompletableFuture<Void> syncKitData(String kitName, Map<String, Object> kitData) {
        if (!isDatabaseReady()) {
            return CompletableFuture.completedFuture(null);
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("kitName", kitName);
        payload.put("action", "update");
        payload.put("timestamp", System.currentTimeMillis());
        payload.set("kitData", mapper.valueToTree(kitData));

        return databaseManager.publishEvent(CHANNEL_KIT_UPDATE, payload)
            .thenAccept(ignored -> {
            });
    }

    /* ----------------------------- Queue Management ----------------------------- */

    public CompletableFuture<Void> updateQueueStatus(UUID playerId, String kitName, String serverName, String action) {
        if (!isDatabaseReady()) {
            return CompletableFuture.completedFuture(null);
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("playerId", playerId.toString());
        if (kitName != null) {
            payload.put("kitName", kitName);
        } else {
            payload.putNull("kitName");
        }
        payload.put("serverName", serverName);
        payload.put("action", action);
        payload.put("timestamp", System.currentTimeMillis());

        return databaseManager.publishEvent(CHANNEL_QUEUE_UPDATE, payload)
            .thenAccept(ignored -> {
            });
    }

    /* ----------------------------- Match Management ----------------------------- */

    public CompletableFuture<Void> notifyMatchStart(UUID matchId, String serverName,
                                                    List<NetworkQueue> teamA, List<NetworkQueue> teamB,
                                                    String kitName, int bet) {
        if (!isDatabaseReady()) {
            return CompletableFuture.completedFuture(null);
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("serverName", serverName);
        payload.put("action", "start");
        if (matchId != null) {
            payload.put("matchId", matchId.toString());
        }
        if (kitName != null) {
            payload.put("kitName", kitName);
        } else {
            payload.putNull("kitName");
        }
        payload.put("bet", bet);
        payload.put("action", "start");
        payload.put("timestamp", System.currentTimeMillis());

        ArrayNode array = payload.putArray("players");
        appendPlayers(array, teamA, "A");
        appendPlayers(array, teamB, "B");

        return databaseManager.publishEvent(CHANNEL_MATCH_START, payload)
            .thenAccept(ignored -> {
            });
    }

    private void appendPlayers(ArrayNode array, List<NetworkQueue> players, String side) {
        if (players == null) {
            return;
        }

        for (NetworkQueue participant : players) {
            ObjectNode playerNode = mapper.createObjectNode();
            playerNode.put("id", participant.getPlayerId().toString());
            playerNode.put("name", participant.getPlayerName());
            playerNode.put("origin", participant.getServerName());
            playerNode.put("side", side);
            if (participant.getKitName() != null) {
                playerNode.put("kit", participant.getKitName());
            }
            playerNode.put("bet", participant.getBet());
            array.add(playerNode);
        }
    }

    public CompletableFuture<Void> notifyMatchEnd(String arenaName, String serverName, UUID winner, UUID loser, String reason) {
        if (!isDatabaseReady()) {
            return CompletableFuture.completedFuture(null);
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("arenaName", arenaName);
        payload.put("serverName", serverName);
        payload.put("action", "end");
        payload.put("timestamp", System.currentTimeMillis());
        payload.put("reason", reason);

        if (winner != null) {
            payload.put("winner", winner.toString());
        }
        if (loser != null) {
            payload.put("loser", loser.toString());
        }

        return databaseManager.publishEvent(CHANNEL_MATCH_END, payload)
            .thenAccept(ignored -> {
            });
    }

    /* ----------------------------- Player Transfers ----------------------------- */

    public CompletableFuture<Boolean> requestPlayerTransfer(UUID playerId, String targetServer, String reason) {
        if (!isDatabaseReady()) {
            return CompletableFuture.completedFuture(false);
        }

        ObjectNode payload = mapper.createObjectNode();
        payload.put("playerId", playerId.toString());
        networkManager.getPlayerServer(playerId).ifPresent(server -> payload.put("playerServer", server));
        payload.put("targetServer", targetServer);
        payload.put("reason", reason);
        payload.put("timestamp", System.currentTimeMillis());

        plugin.getServer().getPlayer(playerId)
            .map(Player::getUsername)
            .ifPresent(name -> payload.put("playerName", name));

        return databaseManager.publishEvent(CHANNEL_PLAYER_TRANSFER, payload)
            .thenApply(id -> id > 0)
            .thenCompose(result -> {
                if (!result) {
                    return CompletableFuture.completedFuture(false);
                }
                return networkManager.transferPlayer(playerId, targetServer);
            });
    }

    private boolean isDatabaseReady() {
        return databaseManager != null && databaseManager.isConnected();
    }
}