package com.meteordevelopments.duels.util.inventory;

import org.bukkit.inventory.ItemStack;
import org.bukkit.util.io.BukkitObjectInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;

public final class ItemUtil {

    private ItemUtil() {
    }

    public static ItemStack itemFrom64(final String data) {
        try {
            final byte[] decoded = Base64.getMimeDecoder().decode(data);
            final ByteArrayInputStream inputStream = new ByteArrayInputStream(decoded);
            try (final BukkitObjectInputStream dataInput = new BukkitObjectInputStream(inputStream)) {
                return (ItemStack) dataInput.readObject();
            }
        } catch (ClassNotFoundException | IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }
}
