package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.OperationTimeoutException;
import atoma.api.Result;
import atoma.api.coordination.command.Command;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.CommandHandlerContext;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
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
import dev.failsafe.function.CheckedPredicate;
import dev.failsafe.function.CheckedSupplier;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class MongoCommandHandler<C extends Command<R>, R> implements CommandHandler<C, R> {

  @Override
  public R execute(C command, CommandHandlerContext context) {
    return this.execute(command, (MongoCommandHandlerContext) context);
  }

  protected abstract R execute(C command, MongoCommandHandlerContext context);

  public ExecutionBuilder<R> newExecution(MongoClient client) {
    return new ExecutionBuilder<>(client);
  }

  public static class ExecutionBuilder<R> {
    private final MongoClient client;
    private final List<Policy<Object>> policies = new ArrayList<>(4);
    private final List<RetryPolicyBuilder<Object>> retryPolicyBuilderList = new ArrayList<>();
    private long delay;
    private long maxDelay;
    private ChronoUnit unit;
    private int maxRetrie;

    private ExecutionBuilder(MongoClient client) {
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

    public ExecutionBuilder<R> withTimeout(Duration timeout) {
      this.policies.add(Timeout.of(timeout));
      return this;
    }

    public ExecutionBuilder<R> retryOnCode(MongoErrorCode... codes) {
      final Set<Integer> codeSet =
          Stream.of(codes).map(MongoErrorCode::getCode).collect(Collectors.toSet());

      this.retryPolicyBuilderList.add(
          RetryPolicy.builder()
              .handleIf(
                  throwable ->
                      throwable instanceof MongoException dbError
                          && codeSet.contains(dbError.getCode())));
      return this;
    }

    public ExecutionBuilder<R> retryOnResult(Predicate<Object> resultPredicate) {
      this.retryPolicyBuilderList.add(RetryPolicy.builder().handleResultIf(resultPredicate::test));
      return this;
    }

    public ExecutionBuilder<R> retryOnException(Predicate<Throwable> exceptionPredicate) {
      this.retryPolicyBuilderList.add(RetryPolicy.builder().handleIf(exceptionPredicate::test));
      return this;
    }

    public ExecutionBuilder<R> retryOnException(Throwable... interestedExceptions) {
      this.retryPolicyBuilderList.add(
          RetryPolicy.builder()
              .handleIf(
                  e ->
                      Stream.of(interestedExceptions)
                          .anyMatch(t -> t.getClass().isAssignableFrom(e.getClass()))));
      return this;
    }

    public ExecutionBuilder<R> withBackoff(
        long delay, long maxDelay, ChronoUnit unit, int maxRetries) {
      this.delay = delay;
      this.maxDelay = maxDelay;
      this.unit = unit;
      this.maxRetrie = maxRetries;
      return this;
    }

    public Result<R> execute(Function<ClientSession, R> command) {

      if (this.unit != null) {
        for (RetryPolicyBuilder<Object> policyBuilder : this.retryPolicyBuilderList) {
          RetryPolicy<Object> policy =
              policyBuilder
                  .withMaxRetries(this.maxRetrie)
                  .withBackoff(delay, maxDelay, unit)
                  .build();
          this.policies.add(policy);
        }
      }

      CheckedSupplier<R> block =
          () -> {
            try (ClientSession session = client.startSession()) {
              return session.withTransaction(() -> command.apply(session));
            }
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
}
