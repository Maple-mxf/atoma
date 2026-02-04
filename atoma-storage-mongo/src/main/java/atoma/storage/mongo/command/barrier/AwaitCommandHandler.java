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

package atoma.storage.mongo.command.barrier;

import atoma.api.AtomaStateException;
import atoma.api.OperationTimeoutException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.CyclicBarrierCommand;
import atoma.api.coordination.command.HandlesCommand;
import atoma.storage.mongo.CommonUtils;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import dev.failsafe.TimeoutExceededException;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.BARRIER;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.emptyList;

/**
 * Handles the {@code await} operation for a distributed {@code CyclicBarrier}.
 *
 * <p>This implementation is highly optimized for correctness and performance under high contention.
 * It uses a combination of a "check-then-act" pattern and optimistic locking via a {@code version}
 * field.
 *
 * <h4>Core Logic</h4>
 *
 * The execution flow is as follows:
 *
 * <ol>
 *   <li><b>Get-or-Create State:</b> It first performs a single atomic {@code findOneAndUpdate} with
 *       {@code upsert=true} to get the current state of the barrier document or create it if it's
 *       the very first participant ever. This operation also retrieves the document's current
 *       {@code version}.
 *   <li><b>Optimistic Locking:</b> This {@code version} number is then used in the filter of all
 *       subsequent write operations. If the document has been modified by another process between
 *       the read and the write, the version number will have changed, causing the update to fail
 *       safely. This prevents lost updates and race conditions.
 *   <li><b>Intelligent "Check-Then-Act":</b> Based on the state read in the first step, the handler
 *       intelligently chooses one of three distinct atomic operations:
 *       <ul>
 *         <li><b>Initialize Generation:</b> If this is the first participant for a new generation,
 *             it sets the entire {@code waiters} sub-document.
 *         <li><b>Trip Barrier:</b> If this is the last participant for the current generation, it
 *             increments the global {@code generation}, unsets the {@code waiters} document, and
 *             resets the {@code is_broken} flag to {@code false}.
 *         <li><b>Join Barrier:</b> Otherwise, it increments the {@code waiters.count} and pushes
 *             itself to the {@code waiters.participants} array.
 *       </ul>
 * </ol>
 *
 * This strategy avoids unnecessary database commands and ensures atomicity in a highly concurrent
 * environment.
 *
 * <h3>MongoDB Document Schema</h3>
 *
 * <pre>{@code
 * [
 *   {
 *     "_id": "BARRIER-TC-028",
 *     "generation": 17,
 *     "is_broken": false,
 *     "participants": [],
 *     "parties": 2,
 *     "version": NumberLong(1)
 *   }
 * ]
 * }</pre>
 */
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(CyclicBarrierCommand.Await.class)
public class AwaitCommandHandler
    extends MongoCommandHandler<CyclicBarrierCommand.Await, CyclicBarrierCommand.AwaitResult> {

  /**
   *
   *
   * <h3>Fake-code for await logical</h3>
   *
   * <pre>{@code
   * if ( barrier exists ) : {
   *    if ( <input parties> != parties )  {
   *                 $$ROOT
   *                 _inconsistent_parties = true
   *                 _passed = false
   *                 return
   *    }else{
   *         if ( is_broken ) {
   *             $$ROOT
   *             return
   *         }else{
   *             if ( generation == <input generation> ) {
   *                 if ( participants.size() + 1 == parties ) {
   *                     $$ROOT
   *                     participants = []
   *                     generation += 1
   *                     _passed = true
   *                     version += 1
   *                     return
   *                 }else{
   *                    $$ROOT
   *                    _passed = false
   *                    participants = $contactArray(participants, [ { participant:"participantId", lease: "leaseId" } ])
   *                    version += 1
   *                    return
   *                 }
   *             }else {
   *                 $$ROOT
   *                  _passed = false
   *             }
   *         }
   *
   *    }
   * }else{
   *     $$ROOT
   *     _inconsistent_parties = true
   * }
   * }</pre>
   *
   * @param command the command for await
   * @return single replace-root pipeline state
   */
  private List<Bson> buildAggregationPipeline(CyclicBarrierCommand.Await command) {
    return List.of(
        replaceRoot(
            new Document(
                "$cond",
                List.of(
                    // if ( barrier exists )
                    new Document("$ne", List.of(new Document("$type", "$parties"), "missing")),

                    // ===== barrier exists =====
                    new Document(
                        "$cond",
                        List.of(
                            // if ( input parties != parties )
                            new Document("$ne", List.of(command.parties(), "$parties")),

                            // inconsistent parties
                            new Document(
                                "$mergeObjects",
                                List.of(
                                    "$$ROOT",
                                    new Document("_inconsistent_parties", true)
                                        .append("_passed", false))),

                            // else: parties equals
                            new Document(
                                "$cond",
                                List.of(
                                    // if ( is_broken )
                                    "$is_broken",

                                    // return $$ROOT + is_broken=true
                                    new Document(
                                        "$mergeObjects",
                                        List.of("$$ROOT", new Document("is_broken", true))),

                                    // else: not broken
                                    new Document(
                                        "$cond",
                                        List.of(
                                            // if ( generation == command.generation )
                                            new Document(
                                                "$eq",
                                                List.of("$generation", command.generation())),

                                            // generation equals
                                            new Document(
                                                "$cond",
                                                List.of(
                                                    // if ( participants.size() + 1 == parties )
                                                    new Document(
                                                        "$eq",
                                                        List.of(
                                                            new Document(
                                                                "$add",
                                                                List.of(
                                                                    new Document(
                                                                        "$size", "$participants"),
                                                                    1)),
                                                            "$parties")),

                                                    // matched barrier
                                                    new Document(
                                                        "$mergeObjects",
                                                        List.of(
                                                            "$$ROOT",
                                                            new Document(
                                                                    "participants", emptyList())
                                                                .append(
                                                                    "generation",
                                                                    new Document(
                                                                        "$add",
                                                                        List.of("$generation", 1L)))
                                                                .append(
                                                                    "version",
                                                                    new Document(
                                                                        "$add",
                                                                        List.of("$version", 1L)))
                                                                .append("_passed", true))),

                                                    // not matched barrier
                                                    new Document(
                                                        "$mergeObjects",
                                                        List.of(
                                                            "$$ROOT",
                                                            new Document("_passed", false)
                                                                .append(
                                                                    "participants",
                                                                    new Document(
                                                                        "$setUnion",
                                                                        List.of(
                                                                            "$participants",
                                                                            List.of(
                                                                                new Document(
                                                                                        "participant",
                                                                                        command
                                                                                            .participantId())
                                                                                    .append(
                                                                                        "lease",
                                                                                        command
                                                                                            .leaseId())))))
                                                                .append(
                                                                    "version",
                                                                    new Document(
                                                                        "$add",
                                                                        List.of(
                                                                            "$version", 1L))))))),

                                            // generation not equals
                                            new Document(
                                                "$mergeObjects",
                                                List.of(
                                                    "$$ROOT",
                                                    new Document("_passed", false))))))))),

                    // ===== barrier does not existed. =====
                    new Document(
                        "$mergeObjects",
                        List.of("$$ROOT", new Document("_inconsistent_parties", true)))))));
  }

  @Override
  public CyclicBarrierCommand.AwaitResult execute(
      CyclicBarrierCommand.Await command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, BARRIER);

    List<Bson> pipeline = buildAggregationPipeline(command);

    Function<ClientSession, CyclicBarrierCommand.AwaitResult> cmdBlock =
        session -> {
          Document barrierDoc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  pipeline,
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

          if (barrierDoc == null) {
            throw new AtomaStateException("Failed to find or create barrier document.");
          }

          System.err.printf(
              "Await pipeline: %s%n Result Barrier-Doc: %s%n",
              CommonUtils.formatScript(pipeline), barrierDoc);

          if (barrierDoc.getBoolean("_inconsistent_parties", false)) {
            throw new AtomaStateException(
                String.format(
                    "Failed to waiting on document. Parties was %d. expected %d",
                    barrierDoc.getInteger("parties"), command.parties()));
          }
          List<Document> participants = barrierDoc.getList("participants", Document.class);
          var waited =
              participants.stream()
                      .anyMatch(
                          t ->
                              t.getString("participant").equals(command.participantId())
                                  && t.getString("lease").equals(command.leaseId()))
                  && barrierDoc.getLong("generation") == command.generation()
                  && !barrierDoc.getBoolean("_passed", false);

          return new CyclicBarrierCommand.AwaitResult(
              barrierDoc.getBoolean("_passed", false),
              barrierDoc.getBoolean("is_broken", false),
              waited,
              barrierDoc.getLong("generation"));
        };

    Result<CyclicBarrierCommand.AwaitResult> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .withoutCausallyConsistent()
            .retryOnCode(WRITE_CONFLICT)
            .withTimeout(Duration.of(command.timeout(), command.timeUnit().toChronoUnit()))
            .execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      // Translate Exception
      if (e instanceof TimeoutExceededException timeoutEx) {
        throw new OperationTimeoutException(timeoutEx);
      }
      throw new AtomaStateException(e);
    }
  }
}
