/*
 * Copyright 2025 XueFeng Ma
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.BARRIER;
import static atoma.storage.mongo.command.AtomaCollectionNamespace.COUNTDOWN_LATCH;
import static atoma.storage.mongo.command.AtomaCollectionNamespace.LEASE;
import static atoma.storage.mongo.command.AtomaCollectionNamespace.MUTEX_LOCK;
import static atoma.storage.mongo.command.AtomaCollectionNamespace.RW_LOCK;
import static atoma.storage.mongo.command.AtomaCollectionNamespace.SEMAPHORE;
import static com.mongodb.client.model.Accumulators.addToSet;
import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Accumulators.push;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
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
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.unset;
import static java.util.Arrays.asList;
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
          cleanCyclicBarriers(context, command);
          cleanCountDownLatches(context, command);
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
    final MongoCollection<Document> collection = getCollection(context, MUTEX_LOCK);

    final List<Bson> pipeline =
        asList(
            lookup(LEASE, "lease", "_id", "lease_doc"),
            match(
                new Document(
                    "$expr", new Document("$eq", List.of(new Document("$size", "$lease_doc"), 0)))),
            project(fields(include("_id", "lease"))));

    List<DeleteOneModel<Document>> deleteOneModelList =
        collection.aggregate(pipeline).into(new ArrayList<>()).stream()
            .map(
                doc ->
                    new DeleteOneModel<Document>(
                        and(eq("_id", doc.getString("_id")), eq("lease", doc.getString("lease")))))
            .toList();

    if (!deleteOneModelList.isEmpty()) {
      BulkWriteResult bulkWriteResult = collection.bulkWrite(deleteOneModelList);
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
    final MongoCollection<Document> collection = getCollection(context, RW_LOCK);

    final List<Bson> pipeline =
        asList(
            project(
                fields(
                    computed(
                        "leases",
                        new Document(
                            "$concatArrays",
                            asList(
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
                                    asList(singletonList("$write_lock.lease"), emptyList()))))),
                    computed("doc", "$$ROOT"))),
            unwind("$leases"),
            lookup(LEASE, "leases", "_id", "lease_doc"),
            match(
                new Document(
                    "$expr", new Document("$eq", List.of(new Document("$size", "$lease_doc"), 0)))),
            group("$_id", first("doc", "$doc"), addToSet("dead_leases", "$leases")));

    final List<Document> locksToClean = collection.aggregate(pipeline).into(new ArrayList<>());

    if (!locksToClean.isEmpty()) {
      List<com.mongodb.client.model.UpdateOneModel<Document>> bulkUpdates = new ArrayList<>();

      for (Document lockInfo : locksToClean) {
        final Document doc = (Document) lockInfo.get("doc");
        final List<String> deadLeases = (List<String>) lockInfo.get("dead_leases");

        final List<Bson> updates = new ArrayList<>();
        updates.add(pull("read_locks", in("lease", deadLeases)));
        updates.add(inc("version", 1L));

        final Document writeLock = (Document) doc.get("write_lock");
        if (writeLock != null && deadLeases.contains(writeLock.getString("lease"))) {
          updates.add(unset("write_lock"));
        }

        bulkUpdates.add(new UpdateOneModel<>(eq("_id", doc.get("_id")), combine(updates)));
      }

      if (!bulkUpdates.isEmpty()) {
        // 执行批量更新
        BulkWriteResult bulkWriteResult = collection.bulkWrite(bulkUpdates);
        log.info(
            "Cleaned dead leases from {} read-write locks, modified count: {}",
            bulkWriteResult.getModifiedCount(),
            bulkWriteResult.getModifiedCount());
      }
    }

    // 删除空的读写锁文档
    DeleteResult deleteResult =
        collection.deleteMany(
            and(
                or(eq("write_lock", null), exists("write_lock", false)),
                or(
                    eq("read_locks", null),
                    exists("read_locks", false),
                    eq("read_locks", new ArrayList<>()))));

    if (deleteResult.getDeletedCount() > 0) {
      log.info("Deleted {} empty read-write lock documents", deleteResult.getDeletedCount());
    }
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
    final MongoCollection<Document> collection = getCollection(context, SEMAPHORE);

    final List<Bson> pipeline =
        asList(
            project(
                fields(
                    include("_id", "available_permits", "version"),
                    computed("leases_as_array", new Document("$objectToArray", "$leases")))),
            unwind("$leases_as_array"),
            lookup(LEASE, "leases_as_array.k", "_id", "lease_doc"),
            match(
                new Document(
                    "$expr", new Document("$eq", List.of(new Document("$size", "$lease_doc"), 0)))),
            limit(500),
            group(
                "$_id",
                first("version", "$version"),
                sum("permits_to_return", "$leases_as_array.v"),
                push("dead_leases_keys", "$leases_as_array.k")));

    while (true) {
      final List<Document> semaphoresToClean =
          collection.aggregate(pipeline).into(new ArrayList<>());
      if (semaphoresToClean.isEmpty()) break;
      List<UpdateOneModel<Document>> bulkUpdates =
          semaphoresToClean.stream()
              .map(
                  sem -> {
                    final int permitsToReturn = ((Number) sem.get("permits_to_return")).intValue();
                    final List<String> deadLeasesKeys = (List<String>) sem.get("dead_leases_keys");
                    Long version = sem.getLong("version");

                    final List<Bson> updates = new ArrayList<>();
                    updates.add(inc("available_permits", permitsToReturn));
                    updates.add(inc("version", 1L));

                    for (String deadLease : deadLeasesKeys) {
                      updates.add(unset("leases." + deadLease));
                    }
                    return new UpdateOneModel<Document>(
                        and(eq("_id", sem.get("_id")), eq("version", version)), combine(updates));
                  })
              .toList();

      if (!bulkUpdates.isEmpty()) {
        BulkWriteResult bulkWriteResult = collection.bulkWrite(bulkUpdates);
        log.info(
            "Cleaned dead leases from {} semaphores, modified count: {}",
            bulkWriteResult.getModifiedCount(),
            bulkWriteResult.getModifiedCount());
      }
    }
  }

  /**
   * Finds and cleans all count-down-latches that have count equals 0.
   *
   * @param context the command handler context
   * @param command the clean command
   */
  private void cleanCountDownLatches(
      MongoCommandHandlerContext context, CleanDeadResourceCommand.Clean command) {
    final MongoCollection<Document> collection = getCollection(context, COUNTDOWN_LATCH);

    while (true) {
      ArrayList<Document> staleCountDownLatches =
          collection
              .find(eq("count", 0))
              .projection(fields(include("_id"), include("version")))
              .limit(500)
              .into(new ArrayList<>());

      if (staleCountDownLatches.isEmpty()) break;

      List<DeleteOneModel<Document>> deleteOneModels =
          staleCountDownLatches.stream()
              .map(
                  cdl ->
                      new DeleteOneModel<Document>(
                          and(
                              eq("_id", cdl.getString("_id")),
                              eq("version", cdl.getLong("version")))))
              .toList();

      BulkWriteResult bulkWriteResult = collection.bulkWrite(deleteOneModels);

      if (log.isDebugEnabled()) {
        log.debug("Clean stale count-down-latches result {} ", bulkWriteResult);
      }
    }
  }

  /**
   * Finds and cleans all cyclic-barriers that have waited with non-existent leases.
   *
   * <p>It removes stale entries from the {@code participants} array where the associated lease no
   * longer exists.
   *
   * @param context the command handler context
   * @param command the clean command
   */
  private void cleanCyclicBarriers(
      MongoCommandHandlerContext context, CleanDeadResourceCommand.Clean command) {
    final MongoCollection<Document> collection = getCollection(context, BARRIER);
    final List<Bson> pipeline =
        asList(
            unwind("$participants"),
            lookup(LEASE, "participants.lease", "_id", "lease_doc"),
            match(
                new Document(
                    "$expr", new Document("$eq", List.of(new Document("$size", "$lease_doc"), 0)))),
            limit(500),
            group(
                "$_id",
                first("version", "$version"),
                addToSet("dead_participant_leases", "$participants.lease")));

    while (true) {
      final List<Document> barriersToClean = collection.aggregate(pipeline).into(new ArrayList<>());
      if (barriersToClean.isEmpty()) break;

      List<UpdateOneModel<Document>> bulkUpdates =
          barriersToClean.stream()
              .map(
                  b -> {
                    final List<String> deadParticipantLeases =
                        (List<String>) b.get("dead_participant_leases");
                    if (deadParticipantLeases == null || deadParticipantLeases.isEmpty())
                      return null;

                    if (log.isDebugEnabled()) {
                      log.debug(
                          "Barrier[{}] will be broken.  version {} ",
                          b.get("_id"),
                          b.get("version"));
                    }

                    return new UpdateOneModel<Document>(
                        and(eq("_id", b.get("_id")), eq("version", b.get("version"))),
                        combine(
                            pull("participants", in("lease", deadParticipantLeases)),
                            set("is_broken", true),
                            inc("version", 1L)));
                  })
              .toList();
      if (!bulkUpdates.isEmpty()) {
        BulkWriteResult bulkWriteResult = collection.bulkWrite(bulkUpdates);
        log.info(
            "Cleaned dead participants from {} barriers, modified count: {}",
            bulkWriteResult.getModifiedCount(),
            bulkWriteResult.getModifiedCount());
      }
    }
  }
}
