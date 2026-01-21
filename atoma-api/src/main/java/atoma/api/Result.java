package atoma.api;

import java.util.function.Function;

/**
 * Represents the result of an operation that can either succeed with a value of type {@code T}
 * or fail with a {@link Throwable} cause. This sealed interface provides a type-safe way
 * to handle success and failure outcomes, preventing null pointer exceptions for missing results
 * or unhandled exceptions.
 *
 * <p>This pattern is inspired by functional programming concepts like Either or Result types,
 * promoting explicit error handling over exceptions for control flow.
 *
 * @param <T> The type of the value on success.
 */
public sealed interface Result<T> permits Result.Success, Result.Failure {

  /**
   * Returns the encapsulated value if this result is a {@link Success}.
   * If this result is a {@link Failure}, it returns {@code null}.
   * Consider using {@link #getOrThrow()} for explicit handling of failure cases,
   * or {@link #isSuccess()} to check the outcome before accessing the value.
   *
   * @return The successful value, or {@code null} if it's a failure.
   */
  T value();

  /**
   * Applies the given mapping function to the encapsulated value if this result is a {@link Success}.
   * If this result is a {@link Failure}, the mapping function is not applied, and this method
   * currently returns {@code null}.
   *
   * @param mapper The function to apply to the value.
   * @param <M> The type of the value returned by the mapping function.
   * @return The result of applying the mapping function if successful, or {@code null} if it's a failure.
   */
  <M> M map(Function<T, M> mapper);

  /**
   * Returns the encapsulated value if this result is a {@link Success}.
   * If this result is a {@link Failure}, it throws the encapsulated {@link Throwable} cause.
   * This method is useful when you expect the operation to succeed and want to unwrap the value
   * directly, letting the caller handle potential exceptions.
   *
   * @return The successful value.
   * @throws Throwable The encapsulated cause if this result is a failure.
   */
  T getOrThrow() throws Throwable;

  /**
   * Returns the cause of the failure if this result is a {@link Failure}.
   *
   * @return The {@link Throwable} cause if it's a failure, otherwise {@code null}.
   */
  default Throwable getCause() {
    return null;
  }

  /**
   * Indicates whether this result represents a successful outcome.
   *
   * @return {@code true} if this is a {@link Success}, {@code false} otherwise.
   */
  default boolean isSuccess() {
    return this instanceof Result.Success<T>;
  }

  /**
   * Indicates whether this result represents a failed outcome.
   *
   * @return {@code true} if this is a {@link Failure}, {@code false} otherwise.
   */
  default boolean isFailure() {
    return this instanceof Result.Failure<T>;
  }

  /**
   * Represents a successful outcome of an operation, encapsulating the successful value.
   *
   * @param <T> The type of the successful value.
   * @param value The value of the successful outcome.
   */
  record Success<T>(T value) implements Result<T> {
    /**
     * Applies the given mapping function to the encapsulated value.
     *
     * @param mapper The function to apply to the value.
     * @param <M> The type of the value returned by the mapping function.
     * @return The result of applying the mapping function.
     */
    @Override
    public <M> M map(Function<T, M> mapper) {
      return mapper.apply(value);
    }

    /**
     * Returns the encapsulated value.
     *
     * @return The successful value.
     */
    @Override
    public T getOrThrow() {
      return value;
    }
  }

  /**
   * Represents a failed outcome of an operation, encapsulating the {@link Throwable} cause.
   *
   * @param <T> The type of the expected successful value (not present in failure).
   * @param cause The {@link Throwable} that caused the failure.
   */
  record Failure<T>(Throwable cause) implements Result<T> {
    /**
     * Returns the encapsulated {@link Throwable} cause.
     * Note: This method returns the actual cause encapsulated by this record,
     * which might be different from the one returned by the interface's default method
     * if the original cause was wrapped.
     *
     * @return The {@link Throwable} that caused the failure.
     */
    public Throwable getCause() {
      return cause.getCause();
    }

    /**
     * Always returns {@code null} for a failed result.
     *
     * @return {@code null}.
     */
    @Override
    public T value() {
      return null;
    }

    /**
     * The mapping function is not applied for a failed result, and this method
     * currently returns {@code null}.
     *
     * @param mapper The function to apply to the value.
     * @param <M> The type of the value returned by the mapping function.
     * @return {@code null}.
     */
    @Override
    public <M> M map(Function<T, M> mapper) {
      return null;
    }

    /**
     * Throws the encapsulated {@link Throwable} cause.
     *
     * @throws Throwable The encapsulated cause.
     */
    @Override
    public T getOrThrow() throws Throwable {
      throw cause;
    }
  }
}
