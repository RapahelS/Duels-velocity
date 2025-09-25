package com.meteordevelopments.duels.common.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.meteordevelopments.duels.common.network.NetworkArena;
import com.meteordevelopments.duels.common.network.NetworkQueue;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Shared JDBC helper backed by HikariCP. Provides asynchronous operations used
 * by both the Bukkit and Velocity plugins for cross-server communication.
 */
public class DatabaseManager {

    private static final String TABLE_PLAYERS = "duels_network_players";
    private static final String TABLE_ARENAS = "duels_network_arenas";
    private static final String TABLE_QUEUE = "duels_network_queue";
    private static final String TABLE_EVENTS = "duels_network_events";

    private final DatabaseSettings settings;
    private final DatabaseLogger logger;
    private final ObjectMapper mapper = new ObjectMapper();

    private HikariDataSource dataSource;
    private ExecutorService executor;

    public DatabaseManager(DatabaseSettings settings, DatabaseLogger logger) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    public synchronized void connect() {
        if (dataSource != null && !dataSource.isClosed()) {
            return;
        }

        HikariConfig config = new HikariConfig();
        String jdbcUrl = String.format(
            "jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf8&useSSL=%s&allowPublicKeyRetrieval=true",
            settings.getHost(),
            settings.getPort(),
            settings.getDatabase(),
            settings.isUseSsl()
        );
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException exception) {
            throw new IllegalStateException("MySQL JDBC driver not found in classpath", exception);
        }
        config.setJdbcUrl(jdbcUrl);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setUsername(settings.getUsername());
        config.setPassword(settings.getPassword());
        config.setMaximumPoolSize(Math.max(2, settings.getMaximumPoolSize()));
        config.setMinimumIdle(Math.max(1, settings.getMinimumIdle()));
        config.setConnectionTimeout(Math.max(5_000L, settings.getConnectionTimeoutMs()));
        config.setIdleTimeout(Math.max(60_000L, settings.getIdleTimeoutMs()));
        config.setPoolName("DuelsNetworkHikari");

        dataSource = new HikariDataSource(config);
        executor = Executors.newFixedThreadPool(Math.max(2, config.getMaximumPoolSize() / 2));

        ensureSchema();
        logger.info("Connected to MySQL database at " + settings.getHost() + ":" + settings.getPort());
    }

    private void ensureSchema() {
        runSync("CREATE TABLE IF NOT EXISTS " + TABLE_PLAYERS + " (" +
            "player_uuid CHAR(36) PRIMARY KEY," +
            "player_name VARCHAR(16) NOT NULL," +
            "server_name VARCHAR(64) NOT NULL," +
            "last_seen BIGINT NOT NULL" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

        runSync("CREATE TABLE IF NOT EXISTS " + TABLE_ARENAS + " (" +
            "server_name VARCHAR(64) NOT NULL," +
            "arena_name VARCHAR(64) NOT NULL," +
            "data LONGTEXT NOT NULL," +
            "last_update BIGINT NOT NULL," +
            "PRIMARY KEY (server_name, arena_name)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

        runSync("CREATE TABLE IF NOT EXISTS " + TABLE_QUEUE + " (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY," +
            "player_uuid CHAR(36) NOT NULL UNIQUE," +
            "player_name VARCHAR(16) NOT NULL," +
            "server_name VARCHAR(64) NOT NULL," +
            "kit_name VARCHAR(64)," +
            "bet INT NOT NULL," +
            "joined_at BIGINT NOT NULL," +
            "party_members LONGTEXT" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

        runSync("CREATE TABLE IF NOT EXISTS " + TABLE_EVENTS + " (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY," +
            "channel VARCHAR(64) NOT NULL," +
            "payload LONGTEXT NOT NULL," +
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
    }

    public synchronized void disconnect() {
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            executor = null;
        }

        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    public boolean isConnected() {
        return dataSource != null && !dataSource.isClosed();
    }

    private void runSync(String sql) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException exception) {
            logger.error("Failed to execute schema statement", exception);
        }
    }

    private <T> CompletableFuture<T> supplyAsync(Function<Connection, T> supplier) {
        if (!isConnected()) {
            CompletableFuture<T> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Database not connected"));
            return future;
        }

        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = dataSource.getConnection()) {
                return supplier.apply(connection);
            } catch (SQLException exception) {
                logger.error("Database operation failed", exception);
                throw new RuntimeException(exception);
            }
        }, executor);
    }

    /* ----------------------------- Player methods ----------------------------- */

    public CompletableFuture<Void> upsertPlayer(UUID playerId, String playerName, String serverName, long lastSeen) {
        String sql = "INSERT INTO " + TABLE_PLAYERS +
            " (player_uuid, player_name, server_name, last_seen) VALUES (?,?,?,?) " +
            "ON DUPLICATE KEY UPDATE player_name=VALUES(player_name), server_name=VALUES(server_name), last_seen=VALUES(last_seen)";

        return supplyAsync(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, playerId.toString());
                statement.setString(2, playerName);
                statement.setString(3, serverName);
                statement.setLong(4, lastSeen);
                statement.executeUpdate();
            } catch (SQLException exception) {
                logger.error("Failed to upsert player", exception);
            }
            return null;
        });
    }

    public CompletableFuture<Void> removePlayer(UUID playerId) {
        String sql = "DELETE FROM " + TABLE_PLAYERS + " WHERE player_uuid=?";
        return supplyAsync(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, playerId.toString());
                statement.executeUpdate();
            } catch (SQLException exception) {
                logger.error("Failed to remove player", exception);
            }
            return null;
        });
    }

    public CompletableFuture<Optional<String>> getPlayerServer(UUID playerId) {
        String sql = "SELECT server_name FROM " + TABLE_PLAYERS + " WHERE player_uuid=?";
        return supplyAsync(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, playerId.toString());
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return Optional.ofNullable(rs.getString("server_name"));
                    }
                }
            } catch (SQLException exception) {
                logger.error("Failed to query player server", exception);
            }
            return Optional.empty();
        });
    }

    public CompletableFuture<Optional<String>> getPlayerServerByName(String playerName) {
        String sql = "SELECT server_name FROM " + TABLE_PLAYERS + " WHERE LOWER(player_name)=LOWER(?)";
        return supplyAsync(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, playerName);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return Optional.ofNullable(rs.getString("server_name"));
                    }
                }
            } catch (SQLException exception) {
                logger.error("Failed to query player server by name", exception);
            }
            return Optional.empty();
        });
    }

    public CompletableFuture<Map<UUID, String>> loadPlayerServerMap() {
        String sql = "SELECT player_uuid, server_name FROM " + TABLE_PLAYERS;
        return supplyAsync(connection -> {
            Map<UUID, String> result = new HashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    result.put(UUID.fromString(rs.getString("player_uuid")), rs.getString("server_name"));
                }
            } catch (SQLException exception) {
                logger.error("Failed to load player map", exception);
            }
            return result;
        });
    }

    public CompletableFuture<Map<String, Integer>> loadServerPlayerCounts() {
        String sql = "SELECT server_name, COUNT(*) as player_count FROM " + TABLE_PLAYERS + " GROUP BY server_name";
        return supplyAsync(connection -> {
            Map<String, Integer> result = new HashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getString("server_name"), rs.getInt("player_count"));
                }
            } catch (SQLException exception) {
                logger.error("Failed to load player counts", exception);
            }
            return result;
        });
    }

    /* ----------------------------- Arena methods ----------------------------- */

    public CompletableFuture<Void> saveArena(NetworkArena arena) {
        String sql = "INSERT INTO " + TABLE_ARENAS +
            " (server_name, arena_name, data, last_update) VALUES (?,?,?,?) " +
            "ON DUPLICATE KEY UPDATE data=VALUES(data), last_update=VALUES(last_update)";

        return supplyAsync(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, arena.getServerName());
                statement.setString(2, arena.getName());

                ObjectNode data = mapper.createObjectNode();
                data.put("available", arena.isAvailable());
                data.put("disabled", arena.isDisabled());
                data.put("lastUpdate", arena.getLastUpdate());

                ArrayNode kits = mapper.createArrayNode();
                if (arena.getBoundKits() != null) {
                    arena.getBoundKits().forEach(kits::add);
                }
                data.set("boundKits", kits);

                ObjectNode positions = mapper.createObjectNode();
                if (arena.getPositions() != null) {
                    arena.getPositions().forEach((key, value) -> positions.putPOJO(key, value));
                }
                data.set("positions", positions);

                statement.setString(3, data.toString());
                statement.setLong(4, Instant.now().toEpochMilli());
                statement.executeUpdate();
            } catch (SQLException exception) {
                logger.error("Failed to save arena", exception);
            }
            return null;
        });
    }

    public CompletableFuture<Void> deleteArena(String serverName, String arenaName) {
        String sql = "DELETE FROM " + TABLE_ARENAS + " WHERE server_name=? AND arena_name=?";
        return supplyAsync(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, serverName);
                statement.setString(2, arenaName);
                statement.executeUpdate();
            } catch (SQLException exception) {
                logger.error("Failed to delete arena", exception);
            }
            return null;
        });
    }

    public CompletableFuture<List<NetworkArena>> loadArenas() {
        String sql = "SELECT server_name, arena_name, data, last_update FROM " + TABLE_ARENAS;
        return supplyAsync(connection -> {
            List<NetworkArena> arenas = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String serverName = rs.getString("server_name");
                    String arenaName = rs.getString("arena_name");
                    String dataJson = rs.getString("data");

                    NetworkArena arena = new NetworkArena(arenaName, serverName);
                    try {
                        ObjectNode node = (ObjectNode) mapper.readTree(dataJson);
                        arena.setAvailable(node.path("available").asBoolean(true));
                        arena.setDisabled(node.path("disabled").asBoolean(false));
                        arena.setLastUpdate(node.path("lastUpdate").asLong(System.currentTimeMillis()));

                        if (node.has("boundKits")) {
                            List<String> kits = new ArrayList<>();
                            node.path("boundKits").forEach(jsonNode -> kits.add(jsonNode.asText()));
                            arena.setBoundKits(kits);
                        }

                        if (node.has("positions")) {
                            Map<String, Object> positions = mapper.convertValue(node.get("positions"), Map.class);
                            arena.setPositions(positions);
                        }
                    } catch (Exception ignored) {
                    }
                    arenas.add(arena);
                }
            } catch (SQLException exception) {
                logger.error("Failed to load arenas", exception);
            }
            return arenas;
        });
    }

    /* ----------------------------- Queue methods ----------------------------- */

    public CompletableFuture<Void> addQueueEntry(NetworkQueue entry) {
        String sql = "INSERT INTO " + TABLE_QUEUE +
            " (player_uuid, player_name, server_name, kit_name, bet, joined_at, party_members) " +
            "VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE " +
            "player_name=VALUES(player_name), server_name=VALUES(server_name), kit_name=VALUES(kit_name), " +
            "bet=VALUES(bet), joined_at=VALUES(joined_at), party_members=VALUES(party_members)";

        return supplyAsync(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, entry.getPlayerId().toString());
                statement.setString(2, entry.getPlayerName());
                statement.setString(3, entry.getServerName());
                statement.setString(4, entry.getKitName());
                statement.setInt(5, entry.getBet());
                statement.setLong(6, entry.getJoinedAt());
                statement.setString(7, serializePartyMembers(entry));
                statement.executeUpdate();
            } catch (SQLException exception) {
                logger.error("Failed to add queue entry", exception);
            }
            return null;
        });
    }

    public CompletableFuture<Void> removeQueueEntry(UUID playerId, String kitName, int bet) {
        String sql = "DELETE FROM " + TABLE_QUEUE + " WHERE player_uuid=?";
        return supplyAsync(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, playerId.toString());
                statement.executeUpdate();
            } catch (SQLException exception) {
                logger.error("Failed to remove queue entry", exception);
            }
            return null;
        });
    }

    public CompletableFuture<List<NetworkQueue>> loadQueueEntries(String kitName, int bet) {
        String sql = "SELECT player_uuid, player_name, server_name, kit_name, bet, joined_at, party_members " +
            "FROM " + TABLE_QUEUE + " WHERE kit_name=? AND bet=?";
        return supplyAsync(connection -> mapQueueEntries(connection, sql, preparedStatement -> {
            preparedStatement.setString(1, kitName);
            preparedStatement.setInt(2, bet);
        }));
    }

    public CompletableFuture<List<NetworkQueue>> loadAllQueueEntries() {
        String sql = "SELECT player_uuid, player_name, server_name, kit_name, bet, joined_at, party_members FROM " + TABLE_QUEUE;
        return supplyAsync(connection -> mapQueueEntries(connection, sql, null));
    }

    private List<NetworkQueue> mapQueueEntries(Connection connection, String sql, SqlConfigurer configurer) {
        List<NetworkQueue> entries = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            if (configurer != null) {
                configurer.configure(statement);
            }
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    UUID playerId = UUID.fromString(rs.getString("player_uuid"));
                    String playerName = rs.getString("player_name");
                    String serverName = rs.getString("server_name");
                    String kitName = rs.getString("kit_name");
                    int bet = rs.getInt("bet");
                    long joinedAt = rs.getLong("joined_at");
                    String partyMembers = rs.getString("party_members");

                    NetworkQueue entry = new NetworkQueue(playerId, playerName, serverName, kitName, bet);
                    entry.setJoinedAt(joinedAt);
                    if (partyMembers != null && !partyMembers.isEmpty()) {
                        String[] parts = partyMembers.split(",");
                        List<UUID> members = new ArrayList<>();
                        for (String raw : parts) {
                            try {
                                members.add(UUID.fromString(raw));
                            } catch (IllegalArgumentException ignored) {
                            }
                        }
                        entry.setPartyMembers(members);
                    }
                    entries.add(entry);
                }
            }
        } catch (SQLException exception) {
            logger.error("Failed to load queue entries", exception);
        }
        return entries;
    }

    private String serializePartyMembers(NetworkQueue entry) {
        if (!entry.isInParty() || entry.getPartyMembers() == null || entry.getPartyMembers().isEmpty()) {
            return null;
        }
        return String.join(",", entry.getPartyMembers().stream().map(UUID::toString).toList());
    }

    /* ----------------------------- Event methods ----------------------------- */

    public CompletableFuture<Long> publishEvent(String channel, ObjectNode payload) {
        String sql = "INSERT INTO " + TABLE_EVENTS + " (channel, payload) VALUES (?,?)";
        return supplyAsync(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                statement.setString(1, channel);
                statement.setString(2, payload.toString());
                statement.executeUpdate();
                try (ResultSet rs = statement.getGeneratedKeys()) {
                    if (rs.next()) {
                        return rs.getLong(1);
                    }
                }
            } catch (SQLException exception) {
                logger.error("Failed to publish event", exception);
            }
            return -1L;
        });
    }

    public CompletableFuture<Collection<NetworkEvent>> fetchEvents(String channel, long afterId, int limit) {
        String sql = "SELECT id, channel, payload, UNIX_TIMESTAMP(created_at) * 1000 AS created_at_ms " +
            "FROM " + TABLE_EVENTS + " WHERE channel=? AND id>? ORDER BY id ASC LIMIT ?";
        return supplyAsync(connection -> {
            List<NetworkEvent> events = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, channel);
                statement.setLong(2, afterId);
                statement.setInt(3, Math.max(1, limit));
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        String payload = rs.getString("payload");
                        long createdAt = rs.getLong("created_at_ms");
                        events.add(new NetworkEvent(id, channel, payload, createdAt));
                    }
                }
            } catch (SQLException exception) {
                logger.error("Failed to fetch events", exception);
            }
            return events;
        });
    }

    public CompletableFuture<Void> cleanupOldEvents(long olderThanMs) {
        String sql = "DELETE FROM " + TABLE_EVENTS + " WHERE created_at < (NOW(6) - INTERVAL ? SECOND)";
        long seconds = Math.max(1, olderThanMs / 1000L);
        return supplyAsync(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, seconds);
                statement.executeUpdate();
            } catch (SQLException exception) {
                logger.error("Failed to prune old events", exception);
            }
            return null;
        });
    }

    /* ----------------------------- Functional helpers ----------------------------- */

    @FunctionalInterface
    private interface SqlConfigurer {
        void configure(PreparedStatement statement) throws SQLException;
    }
}
