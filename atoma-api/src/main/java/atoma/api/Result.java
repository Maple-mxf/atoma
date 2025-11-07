package atoma.api;

import java.util.function.Function;

public sealed interface Result<T> permits Result.Success, Result.Failure {

  T value();

  <M> M map(Function<T, M> mapper);

  T getOrThrow() throws Throwable;

  default Throwable getCause() {
    return null;
  }

  default boolean isSuccess() {
    return this instanceof Result.Success<T>;
  }

  default boolean isFailure() {
    return this instanceof Result.Failure<T>;
  }

  record Success<T>(T value) implements Result<T> {
    @Override
    public <M> M map(Function<T, M> mapper) {
      return mapper.apply(value);
    }

    @Override
    public T getOrThrow() {
      return value;
    }
  }

  record Failure<T>(Throwable cause) implements Result<T> {
    public Throwable getCause() {
      return cause.getCause();
    }

    @Override
    public T value() {
      return null;
    }

    @Override
    public <M> M map(Function<T, M> mapper) {
      return null;
    }

    @Override
    public T getOrThrow() throws Throwable {
      throw cause;
    }
  }
}
