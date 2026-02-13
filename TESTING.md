# Running Integration Tests

This document explains how to run the integration tests for the Hive Table Deletion Job.

## Test Overview

The integration test suite (`HiveTableDeletionJobIntegrationTest`) includes:

1. **testDeleteOneRecordFromTen**: Loads 10 records, deletes 1 by ID, validates count is 9
2. **testDeleteWithTimeWindow**: Deletes records within a time window
3. **testDeleteByStatus**: Deletes records by status field
4. **testDryRunMode**: Verifies dry-run doesn't actually delete records

## Prerequisites

- Java 1.8 or higher
- Maven 3.6+

## Running Tests

### Run All Tests

```bash
cd /Users/manojpawar/citi-workspace/hive-table-delete
mvn test
```

### Run Specific Test Class

```bash
mvn test -Dtest=HiveTableDeletionJobIntegrationTest
```

### Run Specific Test Method

```bash
mvn test -Dtest=HiveTableDeletionJobIntegrationTest#testDeleteOneRecordFromTen
```

### Run with Verbose Output

```bash
mvn test -X
```

## Test Environment

The tests use:
- **Embedded Spark**: Local Spark session with 2 cores
- **Derby Database**: In-memory Hive metastore
- **Temporary Warehouse**: Created in system temp directory
- **Automatic Cleanup**: Tables and data cleaned up after each test

## Expected Output

Successful test run:

```
[INFO] -------------------------------------------------------
[INFO]  T E S T S
[INFO] -------------------------------------------------------
[INFO] Running com.bigdata.hive.deletion.HiveTableDeletionJobIntegrationTest
SparkSession created with warehouse: /tmp/spark-warehouse-1234567890
Test table created: test_db.test_deletion_table
Loaded 10 sample records
Initial record count: 10
Affected partitions: [20260213]
Deleted record count: 1
Final record count: 9
✓ Test passed: Successfully deleted 1 record from 10
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

## Troubleshooting

### Issue: "Derby system home directory already exists"

**Solution**: Clean up Derby databases
```bash
rm -rf /tmp/metastore_db-*
```

### Issue: "Permission denied on warehouse directory"

**Solution**: Ensure write permissions
```bash
chmod -R 755 /tmp/spark-warehouse-*
```

### Issue: Tests timeout

**Solution**: Increase test timeout in pom.xml
```xml
<plugin>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <forkedProcessTimeoutInSeconds>600</forkedProcessTimeoutInSeconds>
    </configuration>
</plugin>
```

## Test Data

Each test creates a temporary table with schema:

```sql
CREATE TABLE test_db.test_deletion_table (
  id BIGINT,
  name STRING,
  status STRING,
  row_create_ts TIMESTAMP
) PARTITIONED BY (partition_id STRING)
STORED AS ORC
```

Sample data pattern:
- IDs: 1-10
- Names: User1-User10
- Status: ACTIVE or INACTIVE (every 3rd record is INACTIVE)
- Timestamps: Configurable per test

## Adding New Tests

To add a new test case:

```java
@Test
@DisplayName("Your test description")
public void testYourScenario() {
    // 1. Load test data
    loadSampleData(10);
    
    // 2. Configure deletion criteria
    DeletionCriteria criteria = new DeletionCriteria.Builder()
        .whereClause("your_condition")
        .build();
    
    // 3. Execute deletion
    // ... deletion logic
    
    // 4. Validate results
    assertEquals(expectedCount, actualCount);
}
```

## CI/CD Integration

To run tests in CI/CD pipeline:

```yaml
# Example GitHub Actions
- name: Run Tests
  run: mvn clean test
  
- name: Publish Test Results
  uses: dorny/test-reporter@v1
  if: always()
  with:
    name: JUnit Tests
    path: target/surefire-reports/*.xml
    reporter: java-junit
```

## Performance Considerations

- Each test creates a new SparkSession (shared across test methods in the class)
- Derby database is in-memory (fast but limited capacity)
- Tests run sequentially to avoid conflicts
- Typical test execution time: 30-60 seconds for all 4 tests

## Best Practices

1. **Keep tests isolated**: Each test should be independent
2. **Clean up resources**: Use @AfterEach to clean up tables
3. **Use meaningful assertions**: Include descriptive messages
4. **Test edge cases**: Empty tables, large datasets, invalid criteria
5. **Mock external dependencies**: Use Mockito for unit tests
