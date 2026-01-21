package atoma.api.coordination.command;

import java.time.Duration;
import java.time.Instant;

/**
 * A container for all commands and result types related to distributed
 * {@link atoma.api.Lease} management. This class encapsulates the lifecycle
 * actions for a lease, such as granting, revoking, and renewing.
 */
public class LeaseCommand {

  /**
   * Command to request the granting of a new lease.
   *
   * @param id  The proposed unique identifier for the lease.
   * @param ttl The time-to-live duration requested for the lease.
   */
  public record Grant(String id, Duration ttl) implements Command<GrantResult> {}

  /**
   * Command to revoke an existing lease, terminating it permanently.
   *
   * @param id The unique identifier of the lease to revoke.
   */
  public record Revoke(String id) implements Command<RevokeResult> {}

  /**
   * Command to perform a time-to-live (TTL) renewal for an existing lease,
   * extending its expiration time.
   *
   * @param id             The unique identifier of the lease to renew.
   * @param nextExpireTime The new expiration time for the lease.
   */
  public record TimeToLive(String id, Instant nextExpireTime) implements Command<TimeToLiveResult> {}

  /**
   * Represents the result of a {@link Grant} command.
   *
   * @param success        {@code true} if the lease was successfully granted.
   * @param id             The unique identifier of the granted lease.
   * @param nextExpireTime The instant at which the newly granted lease will expire if not renewed.
   */
  public record GrantResult(Boolean success, String id, Instant nextExpireTime) {}

  /**
   * Represents the result of a {@link Revoke} command.
   *
   * @param success {@code true} if the lease was successfully revoked.
   */
  public record RevokeResult(Boolean success) {}

  /**
   * Represents the result of a {@link TimeToLive} renewal command.
   *
   * @param success        {@code true} if the lease renewal was successful.
   * @param nextExpireTime The new expiration time of the lease after renewal.
   */
  public record TimeToLiveResult(Boolean success, Instant nextExpireTime) {}
}
