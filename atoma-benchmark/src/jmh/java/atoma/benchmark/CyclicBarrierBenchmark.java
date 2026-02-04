package atoma.benchmark;

import atoma.api.Lease;
import atoma.api.synchronizer.CyclicBarrier;
import atoma.core.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.time.Duration;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(
    value = 1,
    jvmArgs = {"-Xms2G", "-Xmx2G"})
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@State(Scope.Group)
public class CyclicBarrierBenchmark {

  private MongoClient mongoClient;
  private AtomaClient atomaClient;
  private Lease lease;
  private CyclicBarrier barrier;

  private static final int PARTIES = 32;

  @Setup(Level.Trial)
  public void setup() {
    mongoClient =
            MongoClients.create("mongodb://127.0.0.1:32768/atoma_benchmark?replicaSet=docker-rs");
    MongoCoordinationStore mongoCoordinationStore =
        new MongoCoordinationStore(mongoClient, "atoma_benchmark");
    atomaClient = new AtomaClient(mongoCoordinationStore);
    lease = atomaClient.grantLease(Duration.ofMinutes(1));
    // The barrier is cyclic, so it only needs to be created once for the trial.
    barrier = lease.getCyclicBarrier("benchmark-barrier", PARTIES);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    if (barrier != null) barrier.close();
    if (lease != null) lease.close();
    if (atomaClient != null) atomaClient.close();
    if (mongoClient != null) mongoClient.close();
  }

  @Benchmark
  @Group("BarrierAwait")
  @GroupThreads(PARTIES)
  public void awaitBarrier() throws InterruptedException, BrokenBarrierException {
    barrier.await();
  }
}
