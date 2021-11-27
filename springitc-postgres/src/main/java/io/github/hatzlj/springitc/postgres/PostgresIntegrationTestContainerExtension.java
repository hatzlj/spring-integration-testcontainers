package io.github.hatzlj.springitc.postgres;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

/**
 * A simple JUnit5 test extension to spin up a RabbitMQ testcontainer before spring context.
 * <p>To use the test containers in an integration test with spring make sure to specify {@code @ExtendWith
 * (IntegrationTestContainerExtension.class)} <strong>BEFORE</strong> the {@code @ExtendWith(SpringExtension.class)}
 * and {@code @SpringBootTest} annotations to be sure the containers are up and config properties are exposed before
 * spring context is created.
 * <p>
 * The containers expose ports, username, etc. via system properties that are injected during
 * {@link #beforeAll(ExtensionContext)} so that they are available to other extensions (e.g.
 * the <code>org.springframework.test.context.junit.jupiter.SpringExtension</code>).
 * <p>
 * The Postgres container exposes the {@value #TC_DB_URL}, {@value #TC_DB_USER}, {@value #TC_DB_PASSWORD}
 * as system properties to be used in your spring application config (e.g. test.application.properties) to configure
 * your database connection.
 */
public class PostgresIntegrationTestContainerExtension implements BeforeAllCallback, AfterAllCallback {

    private static final Logger log = LoggerFactory.getLogger(PostgresIntegrationTestContainerExtension.class);

    private static final ExtensionContext.Namespace INTEGRATION_CONTAINERS_NAMESPACE =
        ExtensionContext.Namespace.create(PostgresIntegrationTestContainerExtension.class);

    private static final String POSTGRES_IMAGE_VERSION = "postgres:10.3";
    private static final String POSTGRES_CONTAINER_INSTANCE = "POSTGRES_CONTAINER_INSTANCE";
    public static final String TC_DB_URL = "tc.db.url";
    public static final String TC_DB_USER = "tc.db.user";
    public static final String TC_DB_PASSWORD = "tc.db.password";

    /**
     * The singleton instance.
     */
    private static PostgreSQLContainer postgresContainer;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        final Class<?> testClass = context.getRequiredTestClass();
        log.info("setting up PostgreSQL integration test container for {}", testClass.getSimpleName());
        final ExtensionContext.Store store = context.getStore(INTEGRATION_CONTAINERS_NAMESPACE);
        store.put(POSTGRES_CONTAINER_INSTANCE,
            createContainer(Optional.ofNullable(testClass.getAnnotation(PostgresIntegrationConfig.class))));
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        final ExtensionContext.Store store = context.getStore(INTEGRATION_CONTAINERS_NAMESPACE);
        final Class<?> testClass = context.getRequiredTestClass();

        final Optional<PostgresIntegrationConfig> postgresConfig =
            Optional.ofNullable(testClass.getAnnotation(PostgresIntegrationConfig.class));
        final Boolean sharedPostgresInstance =
            postgresConfig.map(PostgresIntegrationConfig::sharedInstance).orElse(true);
        if (!sharedPostgresInstance) {
            log.info("shut down individual PostgreSQL integration test container for {}",
                testClass.getSimpleName());
            Optional.ofNullable(store.get(POSTGRES_CONTAINER_INSTANCE, GenericContainer.class))
                .ifPresent(GenericContainer::stop);
        }
    }

    private PostgreSQLContainer createContainer(Optional<PostgresIntegrationConfig> cfg) throws IOException,
        InterruptedException {
        final Boolean sharedInstance = cfg.map(PostgresIntegrationConfig::sharedInstance).orElse(true);
        final String initWithDump = cfg.map(PostgresIntegrationConfig::initWithDump).orElse(null);
        final String user = cfg.map(PostgresIntegrationConfig::user).orElse("test");
        PostgreSQLContainer container;
        if (sharedInstance && postgresContainer != null) {
            log.info("reusing existing shared PostgreSQL integration test container");
            container = postgresContainer;
        } else {
            container = getPostgreSQLContainer(sharedInstance, user);
            if (initWithDump != null) {
                initDatabase(initWithDump, user, container);
            }
            if (sharedInstance) {
                postgresContainer = container;
            }
        }
        System.setProperty(TC_DB_URL, container.getJdbcUrl());
        System.setProperty(TC_DB_USER, container.getUsername());
        System.setProperty(TC_DB_PASSWORD, container.getPassword());
        return container;
    }

    private PostgreSQLContainer getPostgreSQLContainer(Boolean sharedInstance, String user) {
        log.info("creating new {} PostgreSQL integration test container", sharedInstance ? "shared" : "individual");
        final String dataPath = "/var/lib/postgresql/data/" + UUID.randomUUID();
        PostgreSQLContainer container;
        container = (PostgreSQLContainer) new PostgreSQLContainer(POSTGRES_IMAGE_VERSION)
            .withUsername(user)
            .withDatabaseName(user)
            .withEnv("PGDATA", dataPath);
        container.start();
        return container;
    }

    private void initDatabase(String initWithDump, String user, PostgreSQLContainer container) throws IOException,
        InterruptedException {
        final String dumpTarget = "/initData.dump";
        container.copyFileToContainer(MountableFile.forHostPath(Path.of(initWithDump)), dumpTarget);
        final Container.ExecResult execResult =
            container.execInContainer("bash", "-c",
                "pg_restore -C --clean -d postgres -U " + user + " " + dumpTarget + "");
        if (execResult.getExitCode() != 0) {
            log.error(execResult.getStdout());
            throw new RuntimeException("failed to import init data for PostgreSQL integration test container");
        }
    }

}
