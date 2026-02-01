package atoma.storage.mongo;

import atoma.api.coordination.Subscription;
import com.google.errorprone.annotations.MustBeClosed;

import java.util.concurrent.atomic.AtomicBoolean;

public final class MongoSubscription implements Subscription {
  private final String resourceKey;
  private final AtomicBoolean subscribed = new AtomicBoolean(false);
  private final Runnable unsubscribeAction;

  @MustBeClosed
  public MongoSubscription(String resourceKey, Runnable unsubscribeAction) {
    this.unsubscribeAction = unsubscribeAction;
    this.resourceKey = resourceKey;
  }

  @Override
  public void unsubscribe() {
    if (subscribed.compareAndSet(true, false)) {
      unsubscribeAction.run();
    }
  }

  @Override
  public boolean isSubscribed() {
    return subscribed.get();
  }

  @Override
  public String getResourceKey() {
    return resourceKey;
  }
}
