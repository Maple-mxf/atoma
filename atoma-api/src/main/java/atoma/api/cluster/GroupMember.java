package atoma.api.cluster;

import java.util.Map;

/**
 * Represents a member within a distributed group.
 */
public interface GroupMember {

  /**
   * Returns the unique ID of this member, typically derived from its lease ID.
   *
   * @return the member's unique ID.
   */
  String getId();

  /**
   * Returns custom metadata associated with this member.
   *
   * @return an unmodifiable map of custom metadata.
   */
  Map<String, String> getMetadata();
}