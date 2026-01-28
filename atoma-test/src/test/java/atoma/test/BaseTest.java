package atoma.test;

import atoma.api.coordination.CoordinationStore;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base test class for mutex lock tests using TestContainers. Provides common setup and teardown for
 * MongoDB container. Uses singleton container pattern for efficient test execution.
 */
@Testcontainers
public abstract class BaseTest {

  // Singleton container instance - reused across all tests
  private static final MongoDBContainer mongoDBContainer;

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
  }

  protected MongoClient mongoClient;
  protected CoordinationStore coordinationStore;
  protected AtomaClient atomaClient;

  public MongoCoordinationStore newMongoCoordinationStore() {
    return new MongoCoordinationStore(mongoClient, "atoma_test");
  }

  @BeforeEach
  void setUp() {
    String connectionString = mongoDBContainer.getConnectionString();
    mongoClient = MongoClients.create(connectionString);
    coordinationStore = newMongoCoordinationStore();
    atomaClient = new AtomaClient(coordinationStore);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (atomaClient != null) {
      atomaClient.close();
    }
    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  /**
   * Cleanup method that can be called after all tests are completed. This is optional since
   * containers with reuse=true will be kept running.
   */
  public static void cleanupContainer() {
    if (mongoDBContainer != null && mongoDBContainer.isRunning()) {
      // Container will be kept running due to reuse=true
      // This method is here for future extensibility
    }
  }
}
