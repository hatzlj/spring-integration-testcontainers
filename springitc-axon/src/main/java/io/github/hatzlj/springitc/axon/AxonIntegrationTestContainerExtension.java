package io.github.hatzlj.springitc.axon;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A simple JUnit5 test extension to spin up an Axon testcontainer before spring context.
 * <p>To use the test containers in an integration test with spring make sure to specify {@code @ExtendWith
 * (IntegrationTestContainerExtension.class)} <strong>BEFORE</strong> the {@code @ExtendWith(SpringExtension.class)}
 * and {@code @SpringBootTest} annotations to be sure the containers are up and config properties are exposed before
 * spring context is created.
 * <p>
 * The containers expose ports, username, etc. via system properties that are injected during
 * {@link #beforeAll(ExtensionContext)} so that they are available to other extensions (e.g.
 * the <code>org.springframework.test.context.junit.jupiter.SpringExtension</code>).
 * <p>
 * The Axon Server container exposes the {@value #TC_AXON_HOST}, {@value #TC_AXON_GRPC_PORT}, {@value #TC_AXON_TOKEN}
 * as system properties to be used in your spring application config (e.g. test.application.properties) to configure
 * your axon connection.
 */
public class AxonIntegrationTestContainerExtension implements BeforeAllCallback, AfterAllCallback {

    private static final Logger log = LoggerFactory.getLogger(AxonIntegrationTestContainerExtension.class);

    private static final String AXON_IMAGE_VERSION = "axoniq/axonserver:4.4.12";
    private static final String AXON_CONTAINER_INSTANCE = "AXON_CONTAINER_INSTANCE";
    private static final String AXON_INIT_CONTROL_DATA_PATH = "AXON_INIT_CONTROL_DATA_PATH";
    private static final String AXON_INIT_EVENT_DATA_PATH = "AXON_INIT_EVENT_DATA_PATH";
    public static final String TC_AXON_HOST = "tc.axon.host";
    public static final String TC_AXON_GRPC_PORT = "tc.axon.grpc.port";
    public static final String TC_AXON_HTTP_PORT = "tc.axon.http.port";
    public static final String TC_AXON_TOKEN = "tc.axon.token";

    /**
     * The singleton instance.
     */
    private static GenericContainer axonContainer;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        final Class<?> testClass = context.getRequiredTestClass();
        final ExtensionContext.Store store = context.getStore(getNamespace(testClass));

        log.info("setting up axon integration test container for {}", testClass.getSimpleName());
        createAxonContainer(Optional.ofNullable(testClass.getAnnotation(AxonIntegrationConfig.class)), store);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        final Class<?> testClass = context.getRequiredTestClass();
        final ExtensionContext.Store store = context.getStore(getNamespace(testClass));

        final GenericContainer container = store.get(AXON_CONTAINER_INSTANCE, GenericContainer.class);
        if (container != null) {
            log.info("shut down individual axon integration test container for {}", testClass.getSimpleName());
            Optional.ofNullable(container).ifPresent(GenericContainer::stop);
        }
        final Path initControlDataPath = store.get(AXON_INIT_CONTROL_DATA_PATH, Path.class);
        if (initControlDataPath != null) {
            clearInitData(initControlDataPath);
        }
        final Path initEventDataPath = store.get(AXON_INIT_EVENT_DATA_PATH, Path.class);
        if (initEventDataPath != null) {
            clearInitData(initEventDataPath);
        }

    }

    private void createAxonContainer(Optional<AxonIntegrationConfig> cfg, ExtensionContext.Store store) throws IOException {
        final Boolean sharedInstance = cfg.map(AxonIntegrationConfig::sharedInstance).orElse(true);
        final String initControlData = cfg.map(AxonIntegrationConfig::initControlData).orElse(null);
        final String initEventData = cfg.map(AxonIntegrationConfig::initEventData).orElse(null);

        GenericContainer container;
        if (sharedInstance && axonContainer != null) {
            log.info("reusing existing shared axon integration test container");
            container = axonContainer;
        } else {
            log.info("creating new {} axon integration test container", sharedInstance ? "shared" : "individual");
            final String basePath = "/home/data/UUID.randomUUID()/";
            final String controlPath = basePath + "control/";
            final String eventPath = basePath + "events/";
            container = new GenericContainer(AXON_IMAGE_VERSION)
                    .withExposedPorts(8024, 8124)
                    .withEnv("AXONIQ_AXONSERVER_ACCESSCONTROL_TOKEN", UUID.randomUUID().toString())
                    .withEnv("AXONIQ_AXONSERVER_DEVMODE_ENABLED", "true")
                    .withEnv("AXONIQ_AXONSERVER_CONTROLDB_PATH", controlPath)
                    .withEnv("AXONIQ_AXONSERVER_EVENT_STORAGE", eventPath)
                    .withEnv("JAVA_OPTS", "-Daxoniq.axonserver.default-command-timeout=900000")
                    // the container is ready when the below log message is posted
                    .waitingFor(Wait.forLogMessage(".*Started AxonServer.*\\n", 1))
                    .withStartupTimeout(Duration.ofSeconds(60));

            if (sharedInstance) {
                axonContainer = container;
            } else {
                store.put(AXON_CONTAINER_INSTANCE, container);
            }

            if (initControlData != null) {
                initControlData(store, sharedInstance, initControlData, container, controlPath);
            }
            if (initEventData != null) {
                initEventData(store, sharedInstance, initEventData, container, eventPath);
            }
            container.start();
        }
        System.setProperty(TC_AXON_HOST, container.getHost());
        System.setProperty(TC_AXON_GRPC_PORT, Integer.toString(container.getMappedPort(8124)));
        System.setProperty(TC_AXON_HTTP_PORT, Integer.toString(container.getMappedPort(8024)));
        System.setProperty(TC_AXON_TOKEN, (String) container.getEnvMap()
                .get("AXONIQ_AXONSERVER_ACCESSCONTROL_TOKEN"));
    }

    private void initEventData(ExtensionContext.Store store, Boolean sharedInstance, String initEventData,
                               GenericContainer container, String eventPath) throws IOException {
        if (sharedInstance) {
            throw new RuntimeException("init event data not supported for shared instances");
        }
        final Path initEventDataPath = Path.of(initEventData);
        if (!(Files.isRegularFile(initEventDataPath)
                && initEventDataPath.getFileName().toString().endsWith(".zip"))) {
            throw new RuntimeException("init event data must be zipped");
        }
        final String eventDataPrefix = getClass().getSimpleName() + "-event";
        final Path eventDataTmpHostPath = Files.createTempDirectory(eventDataPrefix);
        unzip(initEventDataPath, eventDataTmpHostPath);
        container.withFileSystemBind(eventDataTmpHostPath.toString(), eventPath);
        store.put(AXON_INIT_EVENT_DATA_PATH, eventDataTmpHostPath);
    }

    private void initControlData(ExtensionContext.Store store, Boolean sharedInstance, String initControlData,
                                 GenericContainer container, String controlPath) throws IOException {
        if (sharedInstance) {
            throw new RuntimeException("init control data not supported for shared instances");
        }
        final Path initControlDataPath = Path.of(initControlData);
        if (!(Files.isRegularFile(initControlDataPath)
                && initControlDataPath.getFileName().toString().endsWith(".zip"))) {
            throw new RuntimeException("init control data must be zipped");
        }
        final String controlDataPrefix = getClass().getSimpleName() + "-control";
        final Path controlDataTmpHostPath = Files.createTempDirectory(controlDataPrefix);
        unzip(initControlDataPath, controlDataTmpHostPath);
        container.withFileSystemBind(controlDataTmpHostPath.toString(), controlPath);
        store.put(AXON_INIT_CONTROL_DATA_PATH, controlDataTmpHostPath);
    }

    private static ExtensionContext.Namespace getNamespace(Class<?> testClass) {
        return ExtensionContext.Namespace.create(AxonIntegrationTestContainerExtension.class, testClass);
    }

    private static void unzip(Path src, Path dest) throws IOException {
        byte[] buffer = new byte[1024];
        ZipInputStream zis = new ZipInputStream(new FileInputStream(src.toFile()));
        ZipEntry zipEntry = zis.getNextEntry();
        while (zipEntry != null) {
            File newFile = newFile(dest.toFile(), zipEntry);
            if (zipEntry.isDirectory()) {
                if (!newFile.isDirectory() && !newFile.mkdirs()) {
                    throw new IOException("Failed to create directory " + newFile);
                }
            } else {
                // fix for Windows-created archives
                File parent = newFile.getParentFile();
                if (!parent.isDirectory() && !parent.mkdirs()) {
                    throw new IOException("Failed to create directory " + parent);
                }

                // write file content
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
            }
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();
    }

    private static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
        File destFile = new File(destinationDir, zipEntry.getName());

        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();

        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destFile;
    }

    private static void clearInitData(Path targetBasePath) {
        log.info("cleanup axon testcontainer init data from '{}'", targetBasePath.toAbsolutePath());
        if (Files.exists(targetBasePath)) {
            try {
                Files.walk(targetBasePath)
                        .sorted(Comparator.reverseOrder())
                        .peek(file -> log.debug("\t{}", file.toAbsolutePath()))
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                log.error("failed to cleanup '{}', : {}", path.toAbsolutePath(), e.getMessage(), e);
                            }
                        });
            } catch (IOException e) {
                throw new RuntimeException("failed to cleanup test resources", e);
            }
        }
    }

}
