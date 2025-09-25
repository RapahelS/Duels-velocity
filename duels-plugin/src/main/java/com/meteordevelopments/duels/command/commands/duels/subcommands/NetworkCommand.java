package com.meteordevelopments.duels.command.commands.duels.subcommands;

import com.meteordevelopments.duels.DuelsPlugin;
import com.meteordevelopments.duels.Permissions;
import com.meteordevelopments.duels.command.BaseCommand;
import com.meteordevelopments.duels.network.NetworkHandler;
import org.bukkit.command.CommandSender;

public class NetworkCommand extends BaseCommand {

    private final NetworkHandler networkHandler;

    public NetworkCommand(final DuelsPlugin plugin) {
        super(plugin, "network", "network [status|reload]", "Network management commands.", Permissions.ADMIN, 1, false);
        this.networkHandler = plugin.getNetworkHandler();
    }

    @Override
    protected void execute(final CommandSender sender, final String label, final String[] args) {
        if (networkHandler == null) {
            lang.sendMessage(sender, "COMMAND.duels.network.not-available");
            return;
        }

        if (args.length < 2) {
            lang.sendMessage(sender, "COMMAND.duels.network.usage");
            return;
        }

        final String subCommand = args[1].toLowerCase();

        switch (subCommand) {
            case "status":
                showNetworkStatus(sender);
                break;
            case "reload":
                reloadNetwork(sender);
                break;
            default:
                lang.sendMessage(sender, "COMMAND.duels.network.usage");
                break;
        }
    }

    private void showNetworkStatus(final CommandSender sender) {
        lang.sendMessage(sender, "COMMAND.duels.network.status.header");
        
        if (networkHandler.isNetworkEnabled()) {
            lang.sendMessage(sender, "COMMAND.duels.network.status.enabled", "server", networkHandler.getServerName());
            
            if (networkHandler.getRedisson() != null && !networkHandler.getRedisson().isShutdown()) {
                lang.sendMessage(sender, "COMMAND.duels.network.status.redis-connected");
            } else {
                lang.sendMessage(sender, "COMMAND.duels.network.status.redis-disconnected");
            }
        } else {
            lang.sendMessage(sender, "COMMAND.duels.network.status.disabled");
        }
        
        lang.sendMessage(sender, "COMMAND.duels.network.status.footer");
    }

    private void reloadNetwork(final CommandSender sender) {
        lang.sendMessage(sender, "COMMAND.duels.network.reload.start");
        
        try {
            // Unload and reload network handler
            networkHandler.handleUnload();
            networkHandler.handleLoad();
            
            lang.sendMessage(sender, "COMMAND.duels.network.reload.success");
        } catch (Exception e) {
            plugin.warn("Failed to reload network: " + e.getMessage());
            lang.sendMessage(sender, "COMMAND.duels.network.reload.failure", "error", e.getMessage());
        }
    }
}