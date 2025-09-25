package com.meteordevelopments.duels.velocity.data;

import com.meteordevelopments.duels.velocity.DuelsVelocityPlugin;
import com.meteordevelopments.duels.velocity.config.VelocityConfig;
import lombok.Getter;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.CompletableFuture;

public class RedisManager {

    private final DuelsVelocityPlugin plugin;
    private final VelocityConfig config;
    
    @Getter
    private RedissonClient redisson;
    
    @Getter
    private boolean connected = false;

    public RedisManager(DuelsVelocityPlugin plugin) {
        this.plugin = plugin;
        this.config = plugin.getConfig();
    }

    public void connect() {
        try {
            Config redisConfig = new Config();
            
            String redisUrl = "redis://";
            if (!config.getRedisPassword().isEmpty()) {
                redisUrl += ":" + config.getRedisPassword() + "@";
            }
            redisUrl += config.getRedisHost() + ":" + config.getRedisPort();
            redisUrl += "/" + config.getRedisDatabase();
            
            redisConfig.useSingleServer()
                .setAddress(redisUrl)
                .setConnectionPoolSize(10)
                .setConnectionMinimumIdleSize(2)
                .setRetryAttempts(3)
                .setRetryInterval(1500);

            redisson = Redisson.create(redisConfig);
            connected = true;

            plugin.getLogger().info("Connected to Redis at {}:{}", config.getRedisHost(), config.getRedisPort());
        } catch (Exception e) {
            plugin.getLogger().error("Failed to connect to Redis", e);
            connected = false;
        }
    }

    public void disconnect() {
        if (redisson != null && !redisson.isShutdown()) {
            redisson.shutdown();
            connected = false;
            plugin.getLogger().info("Disconnected from Redis");
        }
    }

    public CompletableFuture<Void> setAsync(String key, Object value) {
        if (!connected) return CompletableFuture.completedFuture(null);
        return redisson.getBucket(config.getRedisPrefix() + key).setAsync(value).toCompletableFuture();
    }

    public <T> CompletableFuture<T> getAsync(String key, Class<T> type) {
        if (!connected) return CompletableFuture.completedFuture(null);
        return redisson.<T>getBucket(config.getRedisPrefix() + key).getAsync();
    }

    public CompletableFuture<Boolean> deleteAsync(String key) {
        if (!connected) return CompletableFuture.completedFuture(false);
        return redisson.getBucket(config.getRedisPrefix() + key).deleteAsync();
    }

    public CompletableFuture<Boolean> existsAsync(String key) {
        if (!connected) return CompletableFuture.completedFuture(false);
        return redisson.getBucket(config.getRedisPrefix() + key).isExistsAsync();
    }

    public void publish(String channel, Object message) {
        if (!connected) return;
        redisson.getTopic(config.getRedisPrefix() + channel).publishAsync(message);
    }

    public void subscribe(String channel, RedisMessageListener listener) {
        if (!connected) return;
        redisson.getTopic(config.getRedisPrefix() + channel).addListener(Object.class, (charSequence, message) -> {
            listener.onMessage(channel, message);
        });
    }

    @FunctionalInterface
    public interface RedisMessageListener {
        void onMessage(String channel, Object message);
    }
}