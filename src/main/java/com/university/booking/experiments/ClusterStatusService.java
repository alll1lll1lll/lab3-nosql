package com.university.booking.experiments;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.university.booking.cassandra.CassandraConnector;
import com.university.booking.cassandra.ClusterTopology;
import com.university.booking.config.CassandraProperties;
import com.university.booking.dto.cassandra.ClusterStatus;
import com.university.booking.dto.cassandra.NodeStatus;
import com.university.booking.events.EventRecorder;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ClusterStatusService {

    private final CassandraConnector cassandra;
    private final EventRecorder recorder;

    public ClusterStatus status() {
        CqlSession session = cassandra.require().session();
        CassandraProperties props = cassandra.properties();
        List<NodeStatus> nodes = session.getMetadata().getNodes().values().stream()
                .map(ClusterStatusService::toStatus)
                .toList();
        return new ClusterStatus(
                session.getMetadata().getClusterName().orElse(null),
                nodes,
                session.getMetadata().getKeyspace(props.keyspace()).map(KeyspaceMetadata::getReplication).orElse(null),
                props.consistency(),
                props.bucketSize(),
                recorder.stats());
    }

    private static NodeStatus toStatus(Node n) {
        return new NodeStatus(
                ClusterTopology.nodeName(n),
                n.getEndPoint().toString(),
                n.getBroadcastAddress().map(Object::toString).orElse(null),
                n.getDatacenter(),
                n.getRack(),
                n.getState().name(),
                n.getOpenConnections(),
                String.valueOf(n.getCassandraVersion()),
                String.valueOf(n.getHostId()));
    }
}
