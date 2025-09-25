package com.meteordevelopments.duels.common.network;

import java.util.List;
import java.util.Map;

/**
 * Represents an arena that can be shared across servers in the network
 */
public class NetworkArena {
    private String name;
    private String serverName;
    private boolean available;
    private boolean disabled;
    private List<String> boundKits;
    private Map<String, Object> positions;
    private long lastUpdate;

    public NetworkArena(String name, String serverName) {
        this.name = name;
        this.serverName = serverName;
        this.available = true;
        this.disabled = false;
        this.lastUpdate = System.currentTimeMillis();
    }

    // Getters and Setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getServerName() { return serverName; }
    public void setServerName(String serverName) { this.serverName = serverName; }

    public boolean isAvailable() { return available && !disabled; }
    public void setAvailable(boolean available) { this.available = available; }

    public boolean isDisabled() { return disabled; }
    public void setDisabled(boolean disabled) { this.disabled = disabled; }

    public List<String> getBoundKits() { return boundKits; }
    public void setBoundKits(List<String> boundKits) { this.boundKits = boundKits; }

    public Map<String, Object> getPositions() { return positions; }
    public void setPositions(Map<String, Object> positions) { this.positions = positions; }

    public long getLastUpdate() { return lastUpdate; }
    public void setLastUpdate(long lastUpdate) { this.lastUpdate = lastUpdate; }

    public boolean isBoundless() {
        return boundKits == null || boundKits.isEmpty();
    }

    public boolean isBound(String kitName) {
        return boundKits != null && boundKits.contains(kitName);
    }

    public String getFullName() {
        return serverName + ":" + name;
    }
}