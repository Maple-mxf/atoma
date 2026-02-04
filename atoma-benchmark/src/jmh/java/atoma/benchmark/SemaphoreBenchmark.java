package atoma.benchmark;

import atoma.api.Lease;
import atoma.api.synchronizer.Semaphore;
import atoma.core.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgs = {"-Xms2G", "-Xmx2G"})
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
public class SemaphoreBenchmark {

  private MongoClient mongoClient;
  private AtomaClient atomaClient;
  private Lease lease;
  private Semaphore semaphore;

  private static final int PERMITS = 16;

  @Setup
  public void setup() {
    mongoClient =
            MongoClients.create("mongodb://127.0.0.1:32768/atoma_benchmark?replicaSet=docker-rs");
    MongoCoordinationStore mongoCoordinationStore =
        new MongoCoordinationStore(mongoClient, "atoma_benchmark");
    atomaClient = new AtomaClient(mongoCoordinationStore);
    lease = atomaClient.grantLease(Duration.ofMinutes(1));
    semaphore = lease.getSemaphore("benchmark-semaphore", PERMITS);
  }

  @TearDown
  public void tearDown() throws Exception {
    if (semaphore != null) semaphore.close();
    if (lease != null) lease.close();
    if (atomaClient != null) atomaClient.close();
    if (mongoClient != null) mongoClient.close();
  }

  @Benchmark
  @Threads(1)
  public void acquireAndRelease_1_noContention(Blackhole blackhole) throws InterruptedException {
    semaphore.acquire(1);
    try {
      blackhole.consume(0);
    } finally {
      semaphore.release(1);
    }
  }

  @Benchmark
  @Threads(32)
  public void acquireAndRelease_1_withContention(Blackhole blackhole) throws InterruptedException {
    semaphore.acquire(1);
    try {
      blackhole.consume(0);
    } finally {
      semaphore.release(1);
    }
  }

  @Benchmark
  @Threads(1)
  public void acquireAndRelease_all_noContention(Blackhole blackhole) throws InterruptedException {
    semaphore.acquire(PERMITS);
    try {
      blackhole.consume(0);
    } finally {
      semaphore.release(PERMITS);
    }
  }
}
