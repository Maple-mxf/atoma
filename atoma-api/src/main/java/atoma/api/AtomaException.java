package atoma.api;

public class AtomaException extends RuntimeException {

    public AtomaException(Throwable cause) {
        super(cause);
    }

    public AtomaException(String message) {
        super(message);
    }
}
