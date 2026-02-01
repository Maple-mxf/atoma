package atoma.api.lock;

public abstract class CompositeLock extends Lock {

  protected final String[] compositeKeySet;

  protected CompositeLock(String[] compositeKeySet) {
    this.compositeKeySet = compositeKeySet;
  }

  @Override
  public String getResourceId() {
    throw new UnsupportedOperationException();
  }
}
