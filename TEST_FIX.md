# Test Failure Fix - partitionBy() with insertInto()

## Issue

The integration tests were failing with the following error:

```
org.apache.spark.sql.AnalysisException: insertInto() can't be used together with partitionBy(). 
Partition columns have already been defined for the table. It is not necessary to use partitionBy().
```

## Root Cause

In `DeletionExecutor.java` line 166, we were calling both `partitionBy()` and `insertInto()`:

```java
dataToRetain.write()
    .mode(SaveMode.Overwrite)
    .format("orc")
    .partitionBy(config.getPartitionColumn())  // ❌ This is the problem
    .insertInto(config.getFullTableName());
```

**Why this fails:**
- The Hive table is already partitioned (defined in CREATE TABLE statement)
- When using `insertInto()` on a partitioned table, Spark already knows the partition structure
- Calling `partitionBy()` explicitly conflicts with the table's existing partition definition
- Spark's dynamic partition overwrite mode (configured in SparkSession) handles partitioning automatically

## Fix

Removed the `partitionBy()` call:

```java
// Overwrite partitions with retained data using dynamic partition mode
// Note: Don't use partitionBy() with insertInto() - table is already partitioned
dataToRetain.write()
    .mode(SaveMode.Overwrite)
    .format("orc")
    .insertInto(config.getFullTableName());  // ✅ Correct
```

## How It Works Now

1. **Dynamic Partition Overwrite**: Configured in `SparkSessionManager`:
   ```java
   spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
   ```

2. **Automatic Partitioning**: Spark reads the partition column from the retained data and automatically writes to the correct partitions

3. **Overwrite Mode**: Only the partitions present in the written data are overwritten, not the entire table

## Testing

Run the tests to verify the fix:

```bash
# Install Maven if not already installed
brew install maven

# Run all integration tests
mvn test -Dtest=HiveTableDeletionJobIntegrationTest

# Run specific test
mvn test -Dtest=HiveTableDeletionJobIntegrationTest#testDeleteOneRecordFromTen
```

## Expected Results

All 4 tests should now pass:
- ✅ testDeleteOneRecordFromTen
- ✅ testDeleteWithTimeWindow  
- ✅ testDeleteByStatus
- ✅ testDryRunMode

## Related Files

- Fixed: `src/main/java/com/bigdata/hive/deletion/deletion/DeletionExecutor.java`
- Tests: `src/test/java/com/bigdata/hive/deletion/HiveTableDeletionJobIntegrationTest.java`
