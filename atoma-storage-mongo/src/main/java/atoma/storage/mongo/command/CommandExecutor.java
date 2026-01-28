package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.OperationTimeoutException;
import atoma.api.Result;
import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import dev.failsafe.CircuitBreaker;
import dev.failsafe.CircuitBreakerOpenException;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.Policy;
import dev.failsafe.RetryPolicy;
import dev.failsafe.RetryPolicyBuilder;
import dev.failsafe.Timeout;
import dev.failsafe.TimeoutExceededException;
import dev.failsafe.event.EventListener;
import dev.failsafe.event.ExecutionAttemptedEvent;
import dev.failsafe.event.ExecutionCompletedEvent;
import dev.failsafe.function.CheckedPredicate;
import dev.failsafe.function.CheckedSupplier;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @see FailsafeException
 * @see TimeoutExceededException
 * @param <R>
 */
public class CommandExecutor<R> {

  // Requests acknowledgement that the "calculated majority" of data-bearing voting members
  // have durably written the change to their local oplog
  // TODO. Atoma doesn't setup value for write timeout. The {@code wtimeout} opton to specify a time
  // TODO. limit to prevent write operations from blocking indefinitely
  public static final WriteConcern WRITE_CONCERN = new WriteConcern("majority").withJournal(true);
  public static final ReadConcern READ_CONCERN = new ReadConcern(ReadConcernLevel.MAJORITY);
  public static final TransactionOptions TRANSACTION_OPTIONS =
      TransactionOptions.builder().writeConcern(WRITE_CONCERN).readConcern(READ_CONCERN).build();

  private static final ClientSessionOptions CLIENT_SESSION_OPTIONS =
      ClientSessionOptions.builder()
          // causally consistent client sessions can only guarantee causal consistency for:
          // 1. Read operations with "majority" read concern; in other words, the read operations
          // that return
          // data that has been acknowledged by a majority of the replica set member is durable.
          // 2. Write operations with "majority" write concern; in other words, the write operations
          // that request
          // acknowledgment that operation has been applied to a majority of the replica set's
          // data-bearing voting member.
          .causallyConsistent(true)
          .defaultTransactionOptions(TRANSACTION_OPTIONS)
          .build();

  private final MongoClient client;
  private final List<Policy<Object>> policies = new ArrayList<>(4);
  private final List<RetryPolicyBuilder<Object>> retryPolicyBuilderList = new ArrayList<>(4);

  private boolean txn = true;

  public CommandExecutor(MongoClient client) {
    this.client = client;
    this.policies.add(
        RetryPolicy.builder()
            .handleIf(
                (CheckedPredicate<MongoException>)
                    dbError ->
                        dbError.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)
                            || dbError.hasErrorLabel(
                                MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL))
            .build());

    this.policies.add(
        RetryPolicy.builder()
            .handleIf(
                (CheckedPredicate<MongoException>)
                    dbError -> {
                      int code = dbError.getCode();
                      return code == MongoErrorCode.LOCK_TIMEOUT.getCode()
                          || code == MongoErrorCode.LOCK_BUSY.getCode()
                          || code == MongoErrorCode.WRITE_CONFLICT.getCode();
                    })
            .build());

    this.policies.add(
        CircuitBreaker.builder()
            .handle(
                List.of(
                    MongoConfigurationException.class,
                    MongoSecurityException.class,
                    MongoSocketException.class,
                    MongoConnectionPoolClearedException.class,
                    MongoCursorNotFoundException.class,
                    MongoInternalException.class,
                    MongoIncompatibleDriverException.class))
            .build());
  }

  public CommandExecutor<R> withTimeout(Duration timeout) {
    if (timeout.isNegative()) return this;
    this.policies.add(Timeout.of(timeout));
    return this;
  }

  public CommandExecutor<R> withTxn() {
    this.txn = true;
    return this;
  }

  public CommandExecutor<R> withoutTxn() {
    this.txn = false;
    return this;
  }

  public CommandExecutor<R> retryOnCode(MongoErrorCode code) {
    this.retryPolicyBuilderList.add(
        RetryPolicy.builder()
            .withMaxRetries(-1)
            .handleIf(
                throwable ->
                    throwable instanceof MongoException dbError
                        && dbError.getCode() == code.getCode()));
    return this;
  }

  public CommandExecutor<R> retryOnResult(Predicate<R> resultPredicate) {
    this.retryPolicyBuilderList.add(
        RetryPolicy.builder()
            .withMaxRetries(-1)
            .handleResultIf(
                o -> {
                  R result = (R) o;
                  return resultPredicate.test(result);
                }));
    return this;
  }

  public CommandExecutor<R> retryOnException(Predicate<Throwable> exceptionPredicate) {
    this.retryPolicyBuilderList.add(RetryPolicy.builder().handleIf(exceptionPredicate::test));
    return this;
  }

  public final CommandExecutor<R> retryOnException(Class<? extends Throwable> interestedException) {
    this.retryPolicyBuilderList.add(
        RetryPolicy.builder()
            .withMaxRetries(-1)
            .handleIf(e -> e.getClass().equals(interestedException)));
    return this;
  }

  public Result<R> execute(Function<ClientSession, R> command) {

    for (RetryPolicyBuilder<Object> policyBuilder : this.retryPolicyBuilderList) {
      RetryPolicy<Object> policy =
          policyBuilder.onFailedAttempt(event -> {}).onFailure(event -> {}).build();
      this.policies.add(policy);
    }

    CheckedSupplier<R> block =
        () -> {
          if (txn) {
            try (ClientSession session = client.startSession(CLIENT_SESSION_OPTIONS)) {
              return session.withTransaction(() -> command.apply(session), TRANSACTION_OPTIONS);
            }
          }
          return command.apply(null);
        };
    try {
      return new Result.Success<>(Failsafe.with(policies).get(block));
    } catch (FailsafeException e) {
      if (e instanceof TimeoutExceededException timeout) {
        return new Result.Failure<>(new OperationTimeoutException(timeout));
      } else if (e instanceof CircuitBreakerOpenException circuitBreak) {
        return new Result.Failure<>(new AtomaStateException(circuitBreak.getCause()));
      }
      return new Result.Failure<>(e);
    } catch (Throwable e) {
      return new Result.Failure<>(e);
    }
  }
}
