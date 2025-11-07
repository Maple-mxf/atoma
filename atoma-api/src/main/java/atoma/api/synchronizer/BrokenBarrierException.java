package atoma.api.synchronizer;

import atoma.api.AtomaException;

public class BrokenBarrierException extends AtomaException {
  public BrokenBarrierException(String message) {
    super(message);
  }
}