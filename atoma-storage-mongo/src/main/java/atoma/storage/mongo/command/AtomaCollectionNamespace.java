package atoma.storage.mongo.command;

public interface AtomaCollectionNamespace {

  String COUNTDOWN_LATCH_NAMESPACE = "atoma_countdown_latches";
  String MUTEX_LOCK_NAMESPACE = "atoma_mutex_locks";
  String RW_LOCK_NAMESPACE = "atoma_rw_locks";
  String SEMAPHORE_NAMESPACE = "atoma_semaphores";
  String BARRIER_NAMESPACE = "atoma_cyclic_barriers";
  String DOUBLE_BARRIER_NAMESPACE = "atoma_double_barriers";
  String LEASE_NAMESPACE = "atoma_leases";
}
