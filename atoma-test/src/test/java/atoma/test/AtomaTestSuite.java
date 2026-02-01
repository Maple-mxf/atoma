package atoma.test;

import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

@Suite
@SuiteDisplayName("Atoma Test Suite")
@SelectPackages(value = {"atoma.test.rwlock", "atoma.test.mutex", "atoma.test.semaphore","atoma.test.cdl"})
public class AtomaTestSuite {}
