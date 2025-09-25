package com.meteordevelopments.duels.gui.settings.buttons;

import com.meteordevelopments.duels.DuelsPlugin;
import com.meteordevelopments.duels.gui.BaseButton;
import com.meteordevelopments.duels.setting.Settings;
import com.meteordevelopments.duels.util.compat.Items;
import com.meteordevelopments.duels.util.inventory.ItemBuilder;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

public class RequestDetailsButton extends BaseButton {

    public RequestDetailsButton(final DuelsPlugin plugin) {
        super(plugin, ItemBuilder.of(Items.SIGN).name(plugin.getLang().getMessage("GUI.settings.buttons.details.name")).build());
    }

    @Override
    public void update(final Player player) {
        final Settings settings = settingManager.getSafely(player);
        Player target = settings.getTarget() != null ? Bukkit.getPlayer(settings.getTarget()) : null;
        final String opponentName = target != null ? target.getName() : settings.getTargetName();

        if (opponentName == null) {
            settings.reset();
            player.closeInventory();
            lang.sendMessage(player, "ERROR.player.no-longer-online");
            return;
        }

        if (target == null && settings.getTarget() != null) {
            // Local target went offline mid-selection
            settings.reset();
            player.closeInventory();
            lang.sendMessage(player, "ERROR.player.no-longer-online");
            return;
        }

        final String lore = lang.getMessage("GUI.settings.buttons.details.lore",
                "opponent", opponentName,
                "kit", settings.getKit() != null ? settings.getKit().getName() : lang.getMessage("GENERAL.not-selected"),
                "own_inventory", settings.isOwnInventory() ? lang.getMessage("GENERAL.enabled") : lang.getMessage("GENERAL.disabled"),
                "arena", settings.getArena() != null ? settings.getArena().getName() : lang.getMessage("GENERAL.random"),
                "item_betting", settings.isItemBetting() ? lang.getMessage("GENERAL.enabled") : lang.getMessage("GENERAL.disabled"),
                "bet_amount", settings.getBet()
        );
        setLore(lore.split("\n"));
    }
}
