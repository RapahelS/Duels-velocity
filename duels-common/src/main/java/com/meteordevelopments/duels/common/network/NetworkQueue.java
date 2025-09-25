package com.meteordevelopments.duels.common.network;

import java.util.List;
import java.util.UUID;

/**
 * Represents a queue entry that can exist across multiple servers
 */
public class NetworkQueue {
    private UUID playerId;
    private String playerName;
    private String serverName;
    private String kitName;
    private int bet;
    private long joinedAt;
    private boolean inParty;
    private List<UUID> partyMembers;

    public NetworkQueue(UUID playerId, String playerName, String serverName, String kitName, int bet) {
        this.playerId = playerId;
        this.playerName = playerName;
        this.serverName = serverName;
        this.kitName = kitName;
        this.bet = bet;
        this.joinedAt = System.currentTimeMillis();
        this.inParty = false;
    }

    // Getters and Setters
    public UUID getPlayerId() { return playerId; }
    public void setPlayerId(UUID playerId) { this.playerId = playerId; }

    public String getPlayerName() { return playerName; }
    public void setPlayerName(String playerName) { this.playerName = playerName; }

    public String getServerName() { return serverName; }
    public void setServerName(String serverName) { this.serverName = serverName; }

    public String getKitName() { return kitName; }
    public void setKitName(String kitName) { this.kitName = kitName; }

    public int getBet() { return bet; }
    public void setBet(int bet) { this.bet = bet; }

    public long getJoinedAt() { return joinedAt; }
    public void setJoinedAt(long joinedAt) { this.joinedAt = joinedAt; }

    public boolean isInParty() { return inParty; }
    public void setInParty(boolean inParty) { this.inParty = inParty; }

    public List<UUID> getPartyMembers() { return partyMembers; }
    public void setPartyMembers(List<UUID> partyMembers) { 
        this.partyMembers = partyMembers;
        this.inParty = partyMembers != null && !partyMembers.isEmpty();
    }

    public long getWaitTime() {
        return System.currentTimeMillis() - joinedAt;
    }

    public boolean matches(NetworkQueue other) {
        if (other == null) return false;

        // Basic matching criteria
        boolean kitMatches = (kitName == null && other.kitName == null) || 
                           (kitName != null && kitName.equals(other.kitName));
        boolean betMatches = bet == other.bet;
        boolean notSamePlayer = !playerId.equals(other.playerId);

        return kitMatches && betMatches && notSamePlayer;
    }

    public String getQueueKey() {
        return (kitName == null ? "__none__" : kitName) + ":" + bet;
    }
}