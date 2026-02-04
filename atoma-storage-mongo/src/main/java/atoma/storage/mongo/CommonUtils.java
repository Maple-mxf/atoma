package atoma.storage.mongo;

import org.bson.conversions.Bson;

import java.util.List;
import java.util.stream.Collectors;

public class CommonUtils {
  public static String formatScript(List<? extends Bson> pipeline) {
    var text =
        String.format(
            " [ %s ] ",
            pipeline.stream()
                .map(t -> t.toBsonDocument().toJson())
                .collect(Collectors.joining(",")));

    return text;
  }

  public static void printScript(List<? extends Bson> pipeline) {
    var text =
        String.format(
            " [ %s ] ",
            pipeline.stream()
                .map(t -> t.toBsonDocument().toJson())
                .collect(Collectors.joining(",")));

    System.out.printf("pipeline script %s %n", text);
  }
}
