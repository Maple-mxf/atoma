package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CleanDeadResourceCommand;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import com.google.auto.service.AutoService;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.LEASE_NAMESPACE;
import static atoma.storage.mongo.command.AtomaCollectionNamespace.MUTEX_LOCK_NAMESPACE;
import static atoma.storage.mongo.command.AtomaCollectionNamespace.RW_LOCK_NAMESPACE;
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
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

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

  final Logger log = LoggerFactory.getLogger(CleanDeadResourceCommandHandler.class);

  @Override
  public Void execute(CleanDeadResourceCommand.Clean command, MongoCommandHandlerContext context) {
    final MongoClient client = context.getClient();

    final Function<ClientSession, Void> cmdBlock =
        session -> {
          cleanMutexLocks(context, command);
          cleanReadWriteLocks(context, command);
          cleanSemaphores(context, command);
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
   * @param command the clean command
   */
  private void cleanMutexLocks(
      MongoCommandHandlerContext context, CleanDeadResourceCommand.Clean command) {
    final MongoCollection<Document> lockCollection = getCollection(context, MUTEX_LOCK_NAMESPACE);

    final List<Bson> pipeline =
        Arrays.asList(
            lookup(LEASE_NAMESPACE, "lease", "_id", "lease_doc"),
            match(
                new Document(
                    "$expr", new Document("$eq", List.of(new Document("$size", "$lease_doc"), 0)))),
            project(fields(include("_id", "lease"))));

    List<DeleteOneModel<Document>> deleteOneModelList =
        lockCollection.aggregate(pipeline).into(new ArrayList<>()).stream()
            .map(
                doc ->
                    new DeleteOneModel<Document>(
                        and(eq("_id", doc.getString("_id")), eq("lease", doc.getString("lease")))))
            .toList();

    if (!deleteOneModelList.isEmpty()) {
      BulkWriteResult bulkWriteResult = lockCollection.bulkWrite(deleteOneModelList);
      log.info(
          "Detected the presence of inactive mutex locks. delete count: {}",
          bulkWriteResult.getDeletedCount());
    }
  }

  /**
   * Finds and cleans all read-write locks that contain references to non-existent leases.
   *
   * <p>It removes stale entries from the {@code read_locks} array and unsets the {@code write_lock}
   * if its lease is dead. It then deletes any read-write lock documents that have become empty.
   *
   * @param context the command handler context
   * @param command the clean command
   */
  private void cleanReadWriteLocks(
      MongoCommandHandlerContext context, CleanDeadResourceCommand.Clean command) {
    final MongoCollection<Document> collection = getCollection(context, RW_LOCK_NAMESPACE);

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
                                    "$cond",
                                    List.of(
                                        new Document("$isArray", "$read_locks"),
                                        new Document(
                                            "$map",
                                            new Document("input", "$read_locks")
                                                .append("as", "rl")
                                                .append("in", "$$rl.lease")),
                                        emptyList())),
                                new Document(
                                    "$ifNull",
                                    Arrays.asList(
                                        singletonList("$write_lock.lease"), emptyList()))))),
                    computed("doc", "$$ROOT"))),
            unwind("$leases"),
            lookup(LEASE_NAMESPACE, "leases", "_id", "lease_doc"),
            match(
                new Document(
                    "$expr", new Document("$eq", List.of(new Document("$size", "$lease_doc"), 0)))),
            group("$_id", first("doc", "$doc"), addToSet("dead_leases", "$leases")));

    final List<Document> locksToClean = collection.aggregate(pipeline).into(new ArrayList<>());

    for (Document lockInfo : locksToClean) {
      final Document doc = (Document) lockInfo.get("doc");
      final List<String> deadLeases = (List<String>) lockInfo.get("dead_leases");

      final List<Bson> updates = new ArrayList<>();
      updates.add(pull("read_locks", in("lease", deadLeases)));

      final Document writeLock = (Document) doc.get("write_lock");
      if (writeLock != null && deadLeases.contains(writeLock.getString("lease"))) {
        updates.add(unset("write_lock"));
      }
      UpdateResult updateResult = collection.updateOne(eq("_id", doc.get("_id")), combine(updates));
    }

    DeleteResult deleteResult =
        collection.deleteMany(
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
   * @param command the clean command
   */
  private void cleanSemaphores(
      MongoCommandHandlerContext context, CleanDeadResourceCommand.Clean command) {
    final MongoCollection<Document> semaphoreCollection =
        getCollection(context, AtomaCollectionNamespace.SEMAPHORE_NAMESPACE);

    final List<Bson> pipeline =
        Arrays.asList(
            project(
                fields(
                    include("_id", "available_permits"),
                    computed("leases_as_array", new Document("$objectToArray", "$leases")))),
            unwind("$leases_as_array"),
            lookup(LEASE_NAMESPACE, "leases_as_array.k", "_id", "lease_doc"),
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
