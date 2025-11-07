package atoma.api;

public class AtomaStateException extends AtomaException {
    public AtomaStateException(Throwable cause) {
        super(cause);
    }

    public AtomaStateException(String message) {
        super(message);
    }
}
