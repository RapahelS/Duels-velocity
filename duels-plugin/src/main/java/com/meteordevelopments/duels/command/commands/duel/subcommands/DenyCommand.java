package com.meteordevelopments.duels.command.commands.duel.subcommands;

import com.meteordevelopments.duels.DuelsPlugin;
import com.meteordevelopments.duels.api.event.request.RequestDenyEvent;
import com.meteordevelopments.duels.command.BaseCommand;
import com.meteordevelopments.duels.network.NetworkHandler;
import com.meteordevelopments.duels.request.RequestImpl;
import com.meteordevelopments.duels.util.function.Pair;
import com.meteordevelopments.duels.util.validator.ValidatorUtil;
import org.bukkit.Bukkit;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import java.util.Collection;
import java.util.Collections;

public class DenyCommand extends BaseCommand {
    private final NetworkHandler networkHandler;

    public DenyCommand(final DuelsPlugin plugin) {
        super(plugin, "deny", "deny [player]", "Declines a duel request.", 2, true);
        this.networkHandler = plugin.getNetworkHandler();
    }

    @Override
    protected void execute(final CommandSender sender, final String label, final String[] args) {
        final Player player = (Player) sender;
        final Player target = Bukkit.getPlayerExact(args[1]);

        if (target == null || !player.canSee(target)) {
            if (target != null) {
                lang.sendMessage(sender, "ERROR.player.not-found", "name", args[1]);
                return;
            }

            if (networkHandler != null && networkHandler.isNetworkEnabled()) {
                if (networkHandler.denyRemoteChallenge(player, args[1])) {
                    return;
                }
                lang.sendMessage(sender, "ERROR.duel.no-request", "name", args[1]);
                return;
            }

            lang.sendMessage(sender, "ERROR.player.not-found", "name", args[1]);
            return;
        }

        if (!ValidatorUtil.validate(validatorManager.getDuelDenyTargetValidators(), new Pair<>(player, target), partyManager.get(target), Collections.emptyList())) {
            return;
        }

        final RequestImpl request = requestManager.remove(target, player);
        final RequestDenyEvent event = new RequestDenyEvent(player, target, request);
        Bukkit.getPluginManager().callEvent(event);

        if (request.isPartyDuel()) {
            final Player targetPartyLeader = request.getTargetParty().getOwner().getPlayer();
            final Player senderPartyLeader = request.getSenderParty().getOwner().getPlayer();
            lang.sendMessage(Collections.singleton(senderPartyLeader), "COMMAND.duel.party-request.deny.receiver-party", "owner", player.getName(), "name", target.getName());
            lang.sendMessage(targetPartyLeader, "COMMAND.duel.party-request.deny.sender-party", "owner", target.getName(), "name", player.getName());
        } else {
            lang.sendMessage(player, "COMMAND.duel.request.deny.receiver", "name", target.getName());
            lang.sendMessage(target, "COMMAND.duel.request.deny.sender", "name", player.getName());
        }
    }
}
