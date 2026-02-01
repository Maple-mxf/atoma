package atoma.test.mutex;

import atoma.api.coordination.command.SemaphoreCommand;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TmpTest {
  public static void printScript(List<? extends Bson> pipeline) {
    var text =
        String.format(
            " [ %s ] ",
            pipeline.stream()
                .map(t -> t.toBsonDocument().toJson())
                .collect(Collectors.joining(",")));

    System.out.printf("pipeline script %s %n", text);
  }

  public static void main(String[] args) {

    SemaphoreCommand.Acquire command =
        new SemaphoreCommand.Acquire(1, "abc", -1, TimeUnit.SECONDS, 1);

    var pipeline =
        List.of(
            new Document(
                "$replaceRoot",
                new Document(
                    "newRoot",
                    new Document(
                        "$cond",
                        List.of(
                            new Document(
                                "$or",
                                List.of(
                                    new Document(
                                        "$and",
                                        List.of(
                                            new Document(
                                                "$eq",
                                                List.of(
                                                    new Document("$type", "$available_permits"),
                                                    "missing")),
                                            new Document(
                                                "$eq",
                                                List.of(
                                                    new Document("$type", "$initial_permits"),
                                                    "missing")))),

                                    // available_permits >= command.permits()
                                    new Document(
                                        "$gte", List.of("$available_permits", command.permits())))),

                            // ===== then：acquire =====
                            new Document()
                                // initial_permits
                                .append(
                                    "initial_permits",
                                    new Document(
                                        "$ifNull",
                                        List.of("$initial_permits", command.initialPermits())))

                                // available_permits
                                .append(
                                    "available_permits",
                                    new Document(
                                        "$cond",
                                        List.of(
                                            new Document(
                                                "$or",
                                                List.of(
                                                    new Document(
                                                        "$eq",
                                                        List.of(
                                                            new Document(
                                                                "$type", "$available_permits"),
                                                            "missing")),
                                                    new Document(
                                                        "$eq",
                                                        List.of(
                                                            "$available_permits",
                                                            BsonNull.VALUE)))),

                                            // command.initialPermits - command.permits()
                                            new Document(
                                                "$subtract",
                                                List.of(
                                                    command.initialPermits(), command.permits())),

                                            // acquire：available_permits - command.permits()
                                            new Document(
                                                "$subtract",
                                                List.of("$available_permits", command.permits())))))

                                // leases
                                .append(
                                    "leases",
                                    new Document(
                                        "$let",
                                        new Document(
                                                "vars",
                                                new Document(
                                                    "old",
                                                    new Document(
                                                        "$ifNull",
                                                        List.of("$leases", new Document()))))
                                            .append(
                                                "in",
                                                new Document(
                                                    "$setField",
                                                    new Document("field", command.leaseId())
                                                        .append("input", "$$old")
                                                        .append(
                                                            "value",
                                                            new Document(
                                                                "$add",
                                                                List.of(
                                                                    new Document(
                                                                        "$ifNull",
                                                                        List.of(
                                                                            new Document(
                                                                                "$getField",
                                                                                new Document(
                                                                                        "field",
                                                                                        command
                                                                                            .leaseId())
                                                                                    .append(
                                                                                        "input",
                                                                                        "$$old")),
                                                                            0)),
                                                                    command.permits())))))))

                                // version
                                .append(
                                    "version",
                                    new Document(
                                        "$add",
                                        List.of(
                                            new Document("$ifNull", List.of("$version", 0L)), 1L)))
                                .append("_update_flag", true),

                            // ===== else =====
                            new Document(
                                "$mergeObjects",
                                List.of("$$ROOT", new Document("_update_flag", false))))))));

    printScript(pipeline);
  }
}
