package atoma.core.internal;

public class ThreadUtils {

  public static String getCurrentThreadId() {
    return String.format("%s-%d", Thread.currentThread().getName(), Thread.currentThread().getId());
  }

  public static String getCurrentHolderId(String leaseId) {
    return String.format("%s___%s", leaseId, getCurrentThreadId());
  }
}
