package io.github.hatzlj.springitc.rabbit;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;

import java.util.Optional;

/**
 * A simple JUnit5 test extension to spin up a Postgres testcontainer before spring context.
 * <p>To use the test containers in an integration test with spring make sure to specify {@code @ExtendWith
 * (IntegrationTestContainerExtension.class)} <strong>BEFORE</strong> the {@code @ExtendWith(SpringExtension.class)}
 * and {@code @SpringBootTest} annotations to be sure the containers are up and config properties are exposed before
 * spring context is created.
 * <p>
 * The containers expose ports, username, etc. via system properties that are injected during
 * {@link #beforeAll(ExtensionContext)} so that they are available to other extensions (e.g.
 * the <code>org.springframework.test.context.junit.jupiter.SpringExtension</code>).
 * <p>
 * The RabbitMQ container exposes the {@value #TC_RABBITMQ_ADDRESS}, {@value #TC_RABBITMQ_USER},
 * {@value #TC_RABBITMQ_PASSWORD} as system properties to be used in your spring application config
 * (e.g. test.application.properties) to configure your rabbitmq connection.
 */
public class RabbitIntegrationTestContainerExtension implements BeforeAllCallback, AfterAllCallback {

    private static final Logger log = LoggerFactory.getLogger(RabbitIntegrationTestContainerExtension.class);

    private static final ExtensionContext.Namespace INTEGRATION_CONTAINERS_NAMESPACE =
        ExtensionContext.Namespace.create(RabbitIntegrationTestContainerExtension.class);

    private static final String RABBIT_IMAGE_VERSION = "rabbitmq:3.8.9-management";
    private static final String RABBIT_CONTAINER_INSTANCE = "RABBIT_CONTAINER_INSTANCE";
    public static final String TC_RABBITMQ_ADDRESS = "tc.rabbitmq.address";
    public static final String TC_RABBITMQ_USER = "tc.rabbitmq.user";
    public static final String TC_RABBITMQ_PASSWORD = "tc.rabbitmq.password";

    /**
     * The singleton instance.
     */
    private static RabbitMQContainer rabbitContainer;


    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        final Class<?> testClass = context.getRequiredTestClass();
        log.info("setting up RabbitMQ integration test containers for {}", testClass.getSimpleName());
        final ExtensionContext.Store store = context.getStore(INTEGRATION_CONTAINERS_NAMESPACE);
        store.put(RABBIT_CONTAINER_INSTANCE,
            createRabbitContainer(Optional.ofNullable(testClass.getAnnotation(RabbitIntegrationConfig.class))));
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        final ExtensionContext.Store store = context.getStore(INTEGRATION_CONTAINERS_NAMESPACE);
        final Class<?> testClass = context.getRequiredTestClass();

        final Optional<RabbitIntegrationConfig> rabbitConfig =
            Optional.ofNullable(testClass.getAnnotation(RabbitIntegrationConfig.class));
        final Boolean sharedRabbitInstance =
            rabbitConfig.map(RabbitIntegrationConfig::sharedInstance).orElse(true);
        if (!sharedRabbitInstance) {
            log.info("shut down individual RabbitMQ integration test container for {}",
                testClass.getSimpleName());
            Optional.ofNullable(store.get(RABBIT_CONTAINER_INSTANCE, GenericContainer.class))
                .ifPresent(GenericContainer::stop);
        }
    }

    private RabbitMQContainer createRabbitContainer(Optional<RabbitIntegrationConfig> cfg) {
        final Boolean sharedInstance = cfg.map(RabbitIntegrationConfig::sharedInstance).orElse(true);
        RabbitMQContainer container;
        if (sharedInstance && rabbitContainer != null) {
            log.info("reusing existing shared RabbitMQ integration test container");
            container = rabbitContainer;
        } else {
            log.info("creating new {} RabbitMQ integration test container", sharedInstance ? "shared" : "individual");
            container = new RabbitMQContainer(RABBIT_IMAGE_VERSION);
            container.start();
            if (sharedInstance) {
                rabbitContainer = container;
            }
        }
        System.setProperty(TC_RABBITMQ_ADDRESS, container.getAmqpUrl());
        System.setProperty(TC_RABBITMQ_USER, container.getAdminUsername());
        System.setProperty(TC_RABBITMQ_PASSWORD, container.getAdminPassword());
        return container;
    }

}
