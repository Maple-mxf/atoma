package atoma.api;

public class IllegalOwnershipException extends AtomaException {
  public IllegalOwnershipException(Throwable cause) {
    super(cause);
  }

  public IllegalOwnershipException(String message) {
    super(message);
  }
}
