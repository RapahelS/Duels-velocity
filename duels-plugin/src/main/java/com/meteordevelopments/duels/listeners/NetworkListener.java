package com.meteordevelopments.duels.listeners;

import com.meteordevelopments.duels.DuelsPlugin;
import com.meteordevelopments.duels.network.NetworkHandler;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

/**
 * Handles player connection events for network synchronization
 */
public class NetworkListener implements Listener {

    private final DuelsPlugin plugin;
    private final NetworkHandler networkHandler;

    public NetworkListener(DuelsPlugin plugin) {
        this.plugin = plugin;
        this.networkHandler = null; // Will be set later when network handler is loaded
        
        // Always register the listener, but check network status in event handlers
        org.bukkit.Bukkit.getPluginManager().registerEvents(this, plugin);
    }

    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent event) {
        // Get network handler at runtime to avoid loading order issues
        NetworkHandler handler = plugin.getNetworkHandler();
        if (handler != null && handler.isNetworkEnabled()) {
            // Update player location in network cache
            handler.updatePlayerLocation(event.getPlayer());
        }
    }

    @EventHandler
    public void onPlayerQuit(PlayerQuitEvent event) {
        // Get network handler at runtime to avoid loading order issues  
        NetworkHandler handler = plugin.getNetworkHandler();
        if (handler != null && handler.isNetworkEnabled()) {
            handler.handlePlayerQuit(event.getPlayer());
        }
    }
}