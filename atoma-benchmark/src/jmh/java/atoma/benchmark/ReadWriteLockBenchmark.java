package atoma.benchmark;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
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
public class ReadWriteLockBenchmark {

  private MongoClient mongoClient;
  private AtomaClient atomaClient;
  private Lease lease;
  private ReadWriteLock rwLock;
  private Lock readLock;
  private Lock writeLock;

  @Setup
  public void setup() {
    mongoClient =
            MongoClients.create("mongodb://127.0.0.1:32768/atoma_benchmark?replicaSet=docker-rs");
    MongoCoordinationStore mongoCoordinationStore =
        new MongoCoordinationStore(mongoClient, "atoma_benchmark");
    atomaClient = new AtomaClient(mongoCoordinationStore);
    lease = atomaClient.grantLease(Duration.ofMinutes(1));
    rwLock = lease.getReadWriteLock("benchmark-rwlock");
    readLock = rwLock.readLock();
    writeLock = rwLock.writeLock();
  }

  @TearDown
  public void tearDown() throws Exception {
    if (rwLock != null) rwLock.close();
    if (lease != null) lease.close();
    if (atomaClient != null) atomaClient.close();
    if (mongoClient != null) mongoClient.close();
  }

  @Benchmark
  @Threads(1)
  public void writeLock_noContention(Blackhole blackhole) throws InterruptedException {
    writeLock.lock();
    try {
      blackhole.consume(0);
    } finally {
      writeLock.unlock();
    }
  }

  @Benchmark
  @Threads(32)
  public void writeLock_withContention(Blackhole blackhole) throws InterruptedException {
    writeLock.lock();
    try {
      blackhole.consume(0);
    } finally {
      writeLock.unlock();
    }
  }

  @Benchmark
  @Threads(1)
  public void readLock_noContention(Blackhole blackhole) throws InterruptedException {
    readLock.lock();
    try {
      blackhole.consume(0);
    } finally {
      readLock.unlock();
    }
  }

  @Benchmark
  @Threads(32)
  public void readLock_withContention(Blackhole blackhole) throws InterruptedException {
    readLock.lock();
    try {
      blackhole.consume(0);
    } finally {
      readLock.unlock();
    }
  }
}
