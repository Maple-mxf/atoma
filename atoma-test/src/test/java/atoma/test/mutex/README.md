# Atoma Mutex Lock Tests

This package contains comprehensive test cases for the Atoma distributed mutex lock implementation using TestContainers.

## Test Structure

The tests are organized by functional categories as defined in `MutexTestCASE.csv`:

### 1. Lock Acquisition Tests (TC-01 to TC-04)
- **TC-01**: Single client lock acquisition
- **TC-02**: Multi-client competition (only one succeeds)
- **TC-03**: Acquiring expired lock (lock release mechanism)
- **TC-04**: Lock acquisition with different lease durations

### 2. Lock Release Tests (TC-05 to TC-07)
- **TC-05**: Owner releases lock, others can acquire immediately
- **TC-06**: Non-owner attempts to release lock (should fail)
- **TC-07**: Idempotent lock release (multiple releases)

### 3. High Concurrency Tests (TC-12 to TC-14)
- **TC-12**: 50+ clients competing for the same lock
- **TC-13**: Massive contention when lock is released
- **TC-14**: Read-write lock scenario (simulated with mutex locks)

### 4. Client Failure Tests (TC-18 to TC-21)
- **TC-18**: Client crash while holding lock
- **TC-19**: Network partition of lock-holding client
- **TC-20**: GC pause causing lease renewal failure
- **TC-21**: Client restart and lock recovery

### 5. Network Exception Tests (TC-26 to TC-29)
- **TC-26**: Network timeout during lock acquisition
- **TC-27**: Packet loss during lease renewal
- **TC-28**: Connection drop during lock release
- **TC-29**: Reconnection after network flash

## Running the Tests

### Run All Tests
```bash
./gradlew :atoma-test:test
```

### Run Specific Test Class
```bash
./gradlew :atoma-test:test --tests "atoma.test.mutex.SingleClientLockAcquisitionTest"
```

### Run Test Suite
```bash
./gradlew :atoma-test:test --tests "atoma.test.mutex.BaseTest.MutexTestSuite"
```

## Test Infrastructure

- **Base Class**: `BaseMutexTest` provides MongoDB container setup
- **TestContainers**: Automatically manages MongoDB container lifecycle
- **Parallel Execution**: Tests use multiple threads to simulate concurrent scenarios
- **Assertions**: JUnit 5 assertions and AssertJ for fluent assertions

## Implementation Notes

1. Each test case is implemented as a separate Java class for clarity
2. Test case ID and description are included in display names
3. Tests simulate real-world distributed system scenarios
4. Lease expiration is used to handle client failures
5. Network issues are simulated through timing and connection manipulation

## Adding New Test Cases

To add a new test case:
1. Create a new class in `atoma.test.mutex` package
2. Extend `BaseMutexTest`
3. Add appropriate JUnit 5 annotations
4. Include test case ID and description in `@DisplayName`
5. Follow the existing pattern for setup and assertions