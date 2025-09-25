package com.meteordevelopments.duels.command.commands.duels.subcommands;

import com.meteordevelopments.duels.DuelsPlugin;
import com.meteordevelopments.duels.api.kit.Kit;
import com.meteordevelopments.duels.api.queue.DQueue;
import com.meteordevelopments.duels.arena.ArenaImpl;
import com.meteordevelopments.duels.common.network.NetworkArena;
import com.meteordevelopments.duels.common.network.NetworkQueue;
import com.meteordevelopments.duels.network.NetworkHandler;
import com.meteordevelopments.duels.command.BaseCommand;
import com.meteordevelopments.duels.queue.sign.QueueSignImpl;
import com.meteordevelopments.duels.util.StringUtil;
import org.bukkit.command.CommandSender;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ListCommand extends BaseCommand {

    public ListCommand(final DuelsPlugin plugin) {
        super(plugin, "list", null, null, 1, false, "ls");
    }

    @Override
    protected void execute(final CommandSender sender, final String label, final String[] args) {
        final List<String> arenas = new ArrayList<>();
        arenaManager.getArenasImpl().forEach(arena -> arenas.add("&" + getColor(arena) + arena.getName()));
        final String kits = StringUtil.join(kitManager.getKits().stream().map(Kit::getName).collect(Collectors.toList()), ", ");
        final String localQueues = StringUtil.join(queueManager.getQueues().stream().map(DQueue::toString).collect(Collectors.toList()), ", ");
        final String signs = StringUtil.join(queueSignManager.getSigns().stream().map(QueueSignImpl::toString).collect(Collectors.toList()), ", ");
    final NetworkHandler networkHandler = plugin.getNetworkHandler();

        if (networkHandler != null && networkHandler.isNetworkEnabled()) {
            for (NetworkArena arena : networkHandler.getRemoteArenasSnapshot()) {
                final String color = arena.isDisabled() ? "4" : (arena.isAvailable() ? "a" : "c");
                arenas.add("&" + color + arena.getFullName());
            }

            final Map<String, List<NetworkQueue>> networkQueues = networkHandler.getNetworkQueuesSnapshot();
            final List<String> remoteQueues = new ArrayList<>();

            networkQueues.forEach((key, entries) -> {
                if (entries.isEmpty()) {
                    return;
                }

                final String[] parts = key.split(":", 2);
                final String kit = parts.length > 0 && !"__none__".equalsIgnoreCase(parts[0]) ? parts[0] : lang.getMessage("GENERAL.none");
                final String bet = parts.length == 2 ? parts[1] : "0";
        final String queueLabel = kit + " ($" + bet + ")";

                final String servers = entries.stream()
                        .collect(Collectors.groupingBy(NetworkQueue::getServerName, Collectors.counting()))
                        .entrySet()
                        .stream()
                        .map(entry -> entry.getKey() + ":" + entry.getValue())
                        .collect(Collectors.joining(", "));

        remoteQueues.add(queueLabel + " [" + servers + "]");
            });

            final String remoteQueuesJoined = StringUtil.join(remoteQueues, ", ");
            final String combinedQueues;
            if (!remoteQueuesJoined.isEmpty() && !localQueues.isEmpty()) {
                combinedQueues = localQueues + ", " + remoteQueuesJoined;
            } else if (!remoteQueuesJoined.isEmpty()) {
                combinedQueues = remoteQueuesJoined;
            } else {
                combinedQueues = localQueues;
            }

            lang.sendMessage(sender, "COMMAND.duels.list",
                    "arenas", !arenas.isEmpty() ? StringUtil.join(arenas, "&r, &r") : lang.getMessage("GENERAL.none"),
                    "kits", !kits.isEmpty() ? kits : lang.getMessage("GENERAL.none"),
                    "queues", !combinedQueues.isEmpty() ? combinedQueues : lang.getMessage("GENERAL.none"),
                    "queue_signs", !signs.isEmpty() ? signs : lang.getMessage("GENERAL.none"),
                    "lobby", StringUtil.parse(playerManager.getLobby()));
            return;
        }

        lang.sendMessage(sender, "COMMAND.duels.list",
                "arenas", !arenas.isEmpty() ? StringUtil.join(arenas, "&r, &r") : lang.getMessage("GENERAL.none"),
                "kits", !kits.isEmpty() ? kits : lang.getMessage("GENERAL.none"),
                "queues", !localQueues.isEmpty() ? localQueues : lang.getMessage("GENERAL.none"),
                "queue_signs", !signs.isEmpty() ? signs : lang.getMessage("GENERAL.none"),
                "lobby", StringUtil.parse(playerManager.getLobby()));
    }

    private String getColor(final ArenaImpl arena) {
        return arena.isDisabled() ? "4" : (arena.getPositions().size() < 2 ? "9" : arena.isUsed() ? "c" : "a");
    }
}