package com.meteordevelopments.duels.command.commands.duels.subcommands;

import com.meteordevelopments.duels.DuelsPlugin;
import com.meteordevelopments.duels.arena.ArenaImpl;
import com.meteordevelopments.duels.common.network.NetworkArena;
import com.meteordevelopments.duels.command.BaseCommand;
import com.meteordevelopments.duels.kit.KitImpl;
import com.meteordevelopments.duels.network.NetworkHandler;
import com.meteordevelopments.duels.util.StringUtil;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import java.util.List;
import java.util.stream.Collectors;

public class InfoCommand extends BaseCommand {

    public InfoCommand(final DuelsPlugin plugin) {
        super(plugin, "info", "info [name]", "Displays information about the selected arena.", 2, false);
    }

    @Override
    protected void execute(final CommandSender sender, final String label, final String[] args) {
        final String name = StringUtil.join(args, " ", 1, args.length).replace("-", " ");
        final ArenaImpl arena = arenaManager.get(name);
    final NetworkHandler networkHandler = plugin.getNetworkHandler();

        if (arena == null) {
            if (networkHandler != null && networkHandler.isNetworkEnabled()) {
                NetworkArena remoteArena = findRemoteArena(name);
                if (remoteArena != null) {
                    final String inUse = lang.getMessage("GENERAL.unknown");
                    final String disabled = remoteArena.isDisabled() ? lang.getMessage("GENERAL.true") : lang.getMessage("GENERAL.false");
                    final String kits = remoteArena.getBoundKits() != null && !remoteArena.getBoundKits().isEmpty()
                            ? StringUtil.join(remoteArena.getBoundKits(), ", ")
                            : lang.getMessage("GENERAL.none");
                    final String positions = remoteArena.getPositions() != null && !remoteArena.getPositions().isEmpty()
                            ? remoteArena.getPositions().entrySet().stream()
                            .map(entry -> entry.getKey() + ":" + entry.getValue())
                            .collect(Collectors.joining(", "))
                            : lang.getMessage("GENERAL.none");
                    final String players = lang.getMessage("GENERAL.none");
                    lang.sendMessage(sender, "COMMAND.duels.info",
                            "name", remoteArena.getFullName(),
                            "in_use", inUse,
                            "disabled", disabled,
                            "kits", kits,
                            "positions", positions,
                            "players", players);
                    return;
                }
            }

            lang.sendMessage(sender, "ERROR.arena.not-found", "name", name);
            return;
        }

        final String inUse = arena.isUsed() ? lang.getMessage("GENERAL.true") : lang.getMessage("GENERAL.false");
        final String disabled = arena.isDisabled() ? lang.getMessage("GENERAL.true") : lang.getMessage("GENERAL.false");
        final String kits = StringUtil.join(arena.getKits().stream().map(KitImpl::getName).collect(Collectors.toList()), ", ");
        final String positions = StringUtil.join(arena.getPositions().values().stream().map(StringUtil::parse).collect(Collectors.toList()), ", ");
        final String players = StringUtil.join(arena.getPlayers().stream().map(Player::getName).collect(Collectors.toList()), ", ");
        lang.sendMessage(sender, "COMMAND.duels.info", "name", name, "in_use", inUse, "disabled", disabled, "kits",
                !kits.isEmpty() ? kits : lang.getMessage("GENERAL.none"), "positions", !positions.isEmpty() ? positions : lang.getMessage("GENERAL.none"), "players",
                !players.isEmpty() ? players : lang.getMessage("GENERAL.none"));
    }

    @Override
    public List<String> onTabComplete(final CommandSender sender, final Command command, final String alias, final String[] args) {
        if (args.length == 2) {
            return handleTabCompletion(args[1], arenaManager.getNames());
        }

        return null;
    }

    private NetworkArena findRemoteArena(String query) {
    NetworkHandler networkHandler = plugin.getNetworkHandler();
        if (networkHandler == null || !networkHandler.isNetworkEnabled()) {
            return null;
        }

        List<NetworkArena> arenas = networkHandler.getRemoteArenasSnapshot();

        for (NetworkArena arena : arenas) {
            if (arena.getFullName().equalsIgnoreCase(query)) {
                return arena;
            }
        }

        List<NetworkArena> matches = arenas.stream()
                .filter(arena -> arena.getName().equalsIgnoreCase(query))
                .collect(Collectors.toList());

        return matches.size() == 1 ? matches.get(0) : null;
    }
}
