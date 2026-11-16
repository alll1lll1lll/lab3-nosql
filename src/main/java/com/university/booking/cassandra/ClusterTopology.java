package com.university.booking.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public final class ClusterTopology {

    private static final Map<String, String> NODE_BY_PORT = Map.of(
            "9042", "cassandra-1",
            "9043", "cassandra-2",
            "9044", "cassandra-3");

    private ClusterTopology() {
    }

    public static String nodeName(Node node) {
        String endpoint = node.getEndPoint().toString();
        String port = endpoint.substring(endpoint.lastIndexOf(':') + 1);
        return NODE_BY_PORT.getOrDefault(port, endpoint) + "(" + node.getRack() + ")";
    }

    public static List<String> nodeStates(CqlSession session) {
        return session.getMetadata().getNodes().values().stream()
                .map(n -> nodeName(n) + "=" + n.getState())
                .sorted()
                .toList();
    }

    public static List<String> replicas(CqlSession session, String keyspace, BoundStatement partitionKey) {
        TokenMap tokens = session.getMetadata().getTokenMap().orElse(null);
        ByteBuffer routingKey = partitionKey.getRoutingKey();
        if (tokens == null || routingKey == null) {
            return List.of();
        }
        return tokens.getReplicas(CqlIdentifier.fromInternal(keyspace), routingKey).stream()
                .map(ClusterTopology::nodeName)
                .sorted()
                .toList();
    }
}
