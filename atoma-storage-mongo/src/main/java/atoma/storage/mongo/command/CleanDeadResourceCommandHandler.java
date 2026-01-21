package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CleanDeadResourceCommand;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static com.mongodb.client.model.Accumulators.addToSet;
import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Accumulators.push;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.unset;

/**
 * Handles the server-side logic for cleaning up dead resources.
 *
 * <p>A dead resource is a Lock, ReadWriteLock, or Semaphore that references a {@link
 * atoma.api.Lease} that no longer exists. This handler uses aggregation pipelines to efficiently
 * find and remove these stale resources and references from the database.
 *
 * @see atoma.api.Leasable
 * @see atoma.api.lock.Lock
 * @see atoma.api.lock.ReadWriteLock
 * @see atoma.api.synchronizer.Semaphore
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@AutoService({CommandHandler.class})
@HandlesCommand(CleanDeadResourceCommand.Clean.class)
public class CleanDeadResourceCommandHandler
    extends MongoCommandHandler<CleanDeadResourceCommand.Clean, Void> {

  @Override
  public Void execute(CleanDeadResourceCommand.Clean command, MongoCommandHandlerContext context) {
    final MongoClient client = context.getClient();

    final Function<ClientSession, Void> cmdBlock =
        session -> {
          cleanMutexLocks(context);
          cleanReadWriteLocks(context);
          cleanSemaphores(context);
          return null;
        };

    final CommandExecutor<Void> executor = newCommandExecutor(client).withoutTxn();
    final Result<Void> result = executor.execute(cmdBlock);

    try {
      result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
    return null;
  }

  /**
   * Finds and deletes all mutex locks that reference a non-existent lease.
   *
   * @param context the command handler context
   */
  private void cleanMutexLocks(MongoCommandHandlerContext context) {
    final MongoCollection<Document> lockCollection =
        getCollection(context, AtomaCollectionNamespace.MUTEX_LOCK_NAMESPACE);

    final List<Bson> pipeline =
        Arrays.asList(
            lookup(AtomaCollectionNamespace.LEASE_NAMESPACE, "lease", "_id", "lease_doc"),
            match(eq("lease_doc", new ArrayList<>())),
            project(fields(include("_id"))));

    final List<Object> idsToDelete =
        lockCollection.aggregate(pipeline).map(doc -> doc.get("_id")).into(new ArrayList<>());

    if (!idsToDelete.isEmpty()) {
      lockCollection.deleteMany(in("_id", idsToDelete));
    }
  }

  /**
   * Finds and cleans all read-write locks that contain references to non-existent leases.
   *
   * <p>It removes stale entries from the {@code read_locks} array and unsets the {@code write_lock}
   * if its lease is dead. It then deletes any read-write lock documents that have become empty.
   *
   * @param context the command handler context
   */
  private void cleanReadWriteLocks(MongoCommandHandlerContext context) {
    final MongoCollection<Document> rwLockCollection =
        getCollection(context, AtomaCollectionNamespace.RW_LOCK_NAMESPACE);

    final List<Bson> pipeline =
        Arrays.asList(
            project(
                fields(
                    computed(
                        "leases",
                        new Document(
                            "$concatArrays",
                            Arrays.asList(
                                new Document(
                                    "$map",
                                    new Document("input", "$read_locks")
                                        .append("as", "rl")
                                        .append("in", "$$rl.lease")),
                                new Document(
                                    "$ifNull",
                                    Arrays.asList("$write_lock.lease", new ArrayList<>()))))),
                    computed("doc", "$$ROOT"))),
            unwind("$leases"),
            lookup(AtomaCollectionNamespace.LEASE_NAMESPACE, "leases", "_id", "lease_doc"),
            match(eq("lease_doc", new ArrayList<>())),
            group("$_id", first("doc", "$doc"), addToSet("dead_leases", "$leases")));

    final List<Document> locksToClean =
        rwLockCollection.aggregate(pipeline).into(new ArrayList<>());

    for (Document lockInfo : locksToClean) {
      final Document doc = (Document) lockInfo.get("doc");
      final List<String> deadLeases = (List<String>) lockInfo.get("dead_leases");

      final List<Bson> updates = new ArrayList<>();
      updates.add(pull("read_locks", in("lease", deadLeases)));

      final Document writeLock = (Document) doc.get("write_lock");
      if (writeLock != null && deadLeases.contains(writeLock.getString("lease"))) {
        updates.add(unset("write_lock"));
      }

      rwLockCollection.updateOne(eq("_id", doc.get("_id")), combine(updates));
    }

    rwLockCollection.deleteMany(
        and(
            or(eq("write_lock", null), exists("write_lock", false)),
            or(
                eq("read_locks", null),
                exists("read_locks", false),
                eq("read_locks", new ArrayList<>()))));
  }

  /**
   * Finds and cleans all semaphores that have acquired permits with non-existent leases.
   *
   * <p>It calculates the total number of permits to return from dead leases and removes the stale
   * lease entries from the {@code leases} map.
   *
   * @param context the command handler context
   */
  private void cleanSemaphores(MongoCommandHandlerContext context) {
    final MongoCollection<Document> semaphoreCollection =
        getCollection(context, AtomaCollectionNamespace.SEMAPHORE_NAMESPACE);

    final List<Bson> pipeline =
        Arrays.asList(
            project(
                fields(
                    include("_id", "available_permits"),
                    computed("leases_as_array", new Document("$objectToArray", "$leases")))),
            unwind("$leases_as_array"),
            lookup(
                AtomaCollectionNamespace.LEASE_NAMESPACE, "leases_as_array.k", "_id", "lease_doc"),
            match(eq("lease_doc", new ArrayList<>())),
            group(
                "$_id",
                sum("permits_to_return", "$leases_as_array.v"),
                push("dead_leases_keys", "$leases_as_array.k")));

    final List<Document> semaphoresToClean =
        semaphoreCollection.aggregate(pipeline).into(new ArrayList<>());

    for (Document semaphoreInfo : semaphoresToClean) {
      final int permitsToReturn = ((Number) semaphoreInfo.get("permits_to_return")).intValue();
      final List<String> deadLeasesKeys = (List<String>) semaphoreInfo.get("dead_leases_keys");

      final List<Bson> updates = new ArrayList<>();
      updates.add(inc("available_permits", permitsToReturn));
      for (String deadLease : deadLeasesKeys) {
        updates.add(unset("leases." + deadLease));
      }
      semaphoreCollection.updateOne(eq("_id", semaphoreInfo.get("_id")), combine(updates));
    }
  }
}
