package com.university.booking.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.university.booking.config.CassandraProperties;
import com.university.booking.exception.ServiceUnavailableException;
import jakarta.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CassandraConnector {

    private static final int DEFAULT_CQL_PORT = 9042;
    private static final int DEFAULT_PAGE_SIZE = 100;
    private static final String SERIAL_CONSISTENCY = "LOCAL_SERIAL";
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration METADATA_TIMEOUT = Duration.ofSeconds(30);

    private final CassandraProperties props;
    private final AtomicReference<Connection> connection = new AtomicReference<>();

    public record Connection(CqlSession session, CassandraStatements statements) {
    }

    @EventListener(ApplicationReadyEvent.class)
    public void connectOnStartup() {
        tryConnect();
    }

    @Scheduled(fixedDelayString = "${app.cassandra.reconnect-interval:10s}",
            initialDelayString = "${app.cassandra.reconnect-interval:10s}")
    public void reconnectIfNeeded() {
        if (connection.get() == null) {
            tryConnect();
        }
    }

    public boolean isConnected() {
        return connection.get() != null;
    }

    public Connection require() {
        Connection c = connection.get();
        if (c == null) {
            throw new ServiceUnavailableException("Cassandra недоступна: нет подключения к кластеру");
        }
        return c;
    }

    public CassandraProperties properties() {
        return props;
    }

    @PreDestroy
    public void close() {
        Connection c = connection.getAndSet(null);
        if (c != null) {
            c.session().close();
        }
    }

    private synchronized void tryConnect() {
        if (connection.get() != null) {
            return;
        }
        CqlSession session = null;
        try {
            session = CqlSession.builder()
                    .addContactPoints(contactPoints())
                    .withLocalDatacenter(props.localDatacenter())
                    .withConfigLoader(driverConfig())
                    .build();
            CassandraSchema.create(session, props.keyspace(), props.localDatacenter(), props.replicationFactor());
            connection.set(new Connection(session, CassandraStatements.prepare(session, props.keyspace())));
            log.atInfo()
                    .addKeyValue("event", "cassandra_connected")
                    .addKeyValue("nodes", session.getMetadata().getNodes().size())
                    .log("connected to Cassandra cluster");
        } catch (RuntimeException e) {
            if (session != null) {
                session.closeAsync();
            }
            log.atWarn()
                    .addKeyValue("event", "cassandra_unavailable")
                    .log("Cassandra is not available, will retry: {}", e.getMessage());
        }
    }

    private DriverConfigLoader driverConfig() {
        return DriverConfigLoader.programmaticBuilder()
                .withString(DefaultDriverOption.REQUEST_CONSISTENCY, props.consistency())
                .withString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, SERIAL_CONSISTENCY)
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, props.requestTimeout())
                .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, DEFAULT_PAGE_SIZE)
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, CONNECT_TIMEOUT)
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, CONNECT_TIMEOUT)
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, METADATA_TIMEOUT)
                .withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, METADATA_TIMEOUT)
                .build();
    }

    private List<InetSocketAddress> contactPoints() {
        return props.contactPoints().stream()
                .map(String::trim)
                .map(CassandraConnector::toAddress)
                .toList();
    }

    private static InetSocketAddress toAddress(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        return colon < 0
                ? new InetSocketAddress(hostPort, DEFAULT_CQL_PORT)
                : new InetSocketAddress(hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)));
    }
}
