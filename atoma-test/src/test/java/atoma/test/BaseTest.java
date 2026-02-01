package atoma.test;

import atoma.api.coordination.CoordinationStore;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Base test class for mutex lock tests using TestContainers. Provides common setup and teardown for
 * MongoDB container. Uses singleton container pattern for efficient test execution.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers
public abstract class BaseTest {

  // Singleton container instance - reused across all tests
  private static final MongoDBContainer mongoDBContainer;

  // Static resources shared across all test classes
  protected static volatile MongoClient mongoClient;
  @Deprecated protected static volatile CoordinationStore coordinationStore;
  @Deprecated protected static volatile AtomaClient atomaClient;

  static {
    mongoDBContainer =
        new MongoDBContainer("mongo:7.0")
            .withExposedPorts(27017)
            .withReuse(true) // Enable container reuse
            .withCreateContainerCmdModifier(
                cmd -> {
                  // Set fixed name and labels for reuse
                  cmd.withName("atoma-test-mongodb");
                  cmd.getLabels().put("testcontainers.reuse", "true");
                  cmd.getLabels().put("project", "atoma-test");
                });
    mongoDBContainer.start();
    String connectionString = mongoDBContainer.getConnectionString();
    mongoClient = MongoClients.create(connectionString);
    coordinationStore = new MongoCoordinationStore(mongoClient, "atoma_test");
    atomaClient = new AtomaClient(coordinationStore);

  }

  public MongoCoordinationStore newMongoCoordinationStore() {
    return new MongoCoordinationStore(mongoClient, "atoma_test");
  }

  public void crashAtomaClient(
      CoordinationStore coordinationStore, ScheduledExecutorService executorService)
      throws Exception {
    executorService.shutdownNow();
    coordinationStore.close();
  }

  public ScheduledExecutorService newScheduledExecutorService() {
    return Executors.newScheduledThreadPool(
        2, new ThreadFactoryBuilder().setNameFormat("atoma-ttl-worker-%d").build());
  }

  /**
   * No-op method for backward compatibility. Resources are now initialized in static block and
   * never cleaned up to ensure they remain available for all test classes.
   */
  protected static synchronized void setupSharedResources() {
    // Resources are already initialized in static block
  }

  /**
   * No-op cleanup method. Resources are shared across all test classes and should not be cleaned up
   * to avoid issues with test execution order.
   */
  protected static synchronized void cleanupSharedResources() throws Exception {
    // Do not clean up resources to ensure they remain available
    // for subsequent test classes. TestContainers reuse will handle cleanup.
    // mongoClient.close();
  }

  /**
   * Global setup method that runs once before all tests. This method is inherited by all
   * subclasses.
   */
  @BeforeAll
  static void globalSetup() {
    // Resources are already initialized in static block
    // This method ensures setup happens for all test classes
    // Initialize MongoClient immediately in static block

  }

  /**
   * Global cleanup method that runs once after all tests. This method is inherited by all
   * subclasses.
   */
  @AfterAll
  static void globalCleanup() throws Exception {
    // Do not clean up shared resources
    // TestContainers reuse will handle cleanup
  }

  /**
   * Cleanup method that can be called after all tests are completed. This is optional since
   * containers with reuse=true will be kept running.
   */
  public void cleanupContainer() {
    if (mongoDBContainer.isRunning()) {
      // Container will be kept running due to reuse=true
      // This method is here for future extensibility
    }
  }
}
