package atoma.test.mutex;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import atoma.test.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test Case: TC-01
 * Function Scope: 锁获取测试
 * Description: 单客户端成功获取锁
 */
public class SingleClientLockAcquisitionTest extends BaseTest {

    @Test
    @DisplayName("TC-01: 单客户端成功获取锁")
    void testSingleClientLockAcquisition() throws InterruptedException {
        // Given
        String resourceId = "test-resource-tc01";
        Lease lease = atomaClient.grantLease(Duration.ofSeconds(30));
        Lock lock = lease.getLock(resourceId);

        // When
        lock.lock();

        try {
            // Then
            assertThat(lock).isNotNull();
            System.out.println("TC-01: 单客户端成功获取锁 - PASSED");
        } finally {
            lock.unlock();
            lease.revoke();
        }
    }
}