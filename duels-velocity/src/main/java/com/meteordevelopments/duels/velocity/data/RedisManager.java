package com.meteordevelopments.duels.velocity.data;

import com.meteordevelopments.duels.velocity.DuelsVelocityPlugin;

/**
 * Legacy stub retained for backwards compatibility. The Velocity module now
 * uses a SQL-backed {@code DatabaseManager} instead of Redis, so this class is
 * intentionally empty. It remains only to avoid ClassNotFound errors for any
 * shading or reflection-based integrations that may still reference the old
 * type.
 */
@Deprecated(forRemoval = true, since = "4.4")
public final class RedisManager {

    public RedisManager(DuelsVelocityPlugin plugin) {
        // No-op
    }

    public void connect() {
        // No-op
    }

    public void disconnect() {
        // No-op
    }
}