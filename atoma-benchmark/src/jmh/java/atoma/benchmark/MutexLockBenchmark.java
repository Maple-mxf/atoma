package atoma.benchmark;

import atoma.api.Lease;
import atoma.core.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.openjdk.jmh.annotations.*;
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
public class MutexLockBenchmark {
  private MongoClient mongoClient;
  private AtomaClient atomaClient;
  private atoma.api.lock.Lock mutexLock;

  private Lease lease;

  @Setup
  public void setup() {
    mongoClient =
        MongoClients.create("mongodb://127.0.0.1:32768/atoma_benchmark?replicaSet=docker-rs");
    MongoCoordinationStore mongoCoordinationStore =
        new MongoCoordinationStore(mongoClient, "atoma_benchmark");

    atomaClient = new AtomaClient(mongoCoordinationStore);
    lease = atomaClient.grantLease(Duration.ofMinutes(1));

    mutexLock = lease.getLock("test-lock");
  }

  @TearDown
  public void tearDown() throws Exception {
    mutexLock.close();
    lease.close();
    atomaClient.close();
    mongoClient.close();
  }

  @Benchmark
  @Threads(1)
  public void lockAndUnlock_NoContention(Blackhole blackhole) throws InterruptedException {
    mutexLock.lock();
    try {
      // Simulate some work
      blackhole.consume(0);
    } finally {
      mutexLock.unlock();
    }
  }

  @Benchmark
  @Threads(32)
  public void lockAndUnlock_WithContention(Blackhole blackhole) throws InterruptedException {
    mutexLock.lock();
    try {
      // Simulate some work
      blackhole.consume(0);
    } finally {
      mutexLock.unlock();
    }
  }
}
