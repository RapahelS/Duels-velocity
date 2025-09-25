package com.meteordevelopments.duels.common.database;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;

/**
 * Represents a lightweight pub/sub message that has been persisted in the
 * shared SQL database. Consumers keep track of the latest processed id and
 * regularly poll for newer events.
 */
@Getter
public class NetworkEvent {
    private final long id;
    private final String channel;
    private final String payload;
    private final long createdAt;

    public NetworkEvent(long id, String channel, String payload, long createdAt) {
        this.id = id;
        this.channel = channel;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    public ObjectNode payloadAsObjectNode(ObjectMapper mapper) {
        try {
            JsonNode node = mapper.readTree(payload);
            if (node.isObject()) {
                return (ObjectNode) node;
            }
        } catch (Exception ignored) {
        }
        return mapper.createObjectNode();
    }
}
