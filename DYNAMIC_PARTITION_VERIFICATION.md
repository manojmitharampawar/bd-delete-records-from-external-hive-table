# Dynamic Partition Overwrite Verification

## Configuration ✅

**Location**: `SparkSessionManager.java` line 30

```java
builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

## How It Works

### Dynamic vs Static Mode

#### Static Mode (DEFAULT - ❌ Not what we want)
```java
spark.sql.sources.partitionOverwriteMode = "static"
```

**Behavior**: Overwrites **ALL** partitions in the table, even if you're only writing to one partition.

**Example**:
```
Table has partitions: [20260101, 20260102, 20260103]
Write data for partition: [20260102]
Result: ALL partitions deleted, only 20260102 remains ❌
```

#### Dynamic Mode (CONFIGURED - ✅ What we want)
```java
spark.sql.sources.partitionOverwriteMode = "dynamic"
```

**Behavior**: Overwrites **ONLY** the partitions present in the data being written.

**Example**:
```
Table has partitions: [20260101, 20260102, 20260103]
Write data for partition: [20260102]
Result: Only 20260102 is overwritten, others remain ✅
```

## Verification in Our Code

### 1. Deletion Executor

**File**: `DeletionExecutor.java`

```java
// Read data that should be RETAINED
String selectSql = String.format(
    "SELECT * FROM %s WHERE %s AND (%s)",
    config.getFullTableName(), partitionFilter, retentionWhereClause);

Dataset<Row> dataToRetain = spark.sql(selectSql);

// Write retained data back
dataToRetain.write()
    .mode(SaveMode.Overwrite)
    .format("orc")
    .insertInto(config.getFullTableName());
```

**What happens**:
1. Reads data from affected partitions (e.g., `partition_id IN ('20260101', '20260102')`)
2. Filters to keep only records that should be retained
3. Writes back to table
4. **Dynamic mode ensures**: Only partitions `20260101` and `20260102` are overwritten
5. Other partitions (e.g., `20260103`, `20260104`) remain **untouched**

### 2. Backup Restore

**File**: `HiveTableBackupStrategy.java`

```java
// Read all data from backup table
Dataset<Row> backupData = spark.table(backupLocation);

// Write data back to original table
backupData.write()
    .mode(SaveMode.Overwrite)
    .format("orc")
    .insertInto(config.getFullTableName());
```

**What happens**:
1. Reads all data from backup (which contains only the backed-up partitions)
2. Writes back to original table
3. **Dynamic mode ensures**: Only the partitions present in backup are overwritten
4. If backup has `[20260101, 20260102]`, only those partitions are restored
5. Other partitions remain unchanged

## Test Scenario

### Initial State
```
Table: my_table
Partitions:
  - 20260101: 1000 records
  - 20260102: 1500 records
  - 20260103: 2000 records
  - 20260104: 1800 records
Total: 6300 records
```

### Delete from partition 20260102 only
```java
// Deletion criteria targets partition 20260102
// Deletes 500 records from that partition
```

### Expected Result with Dynamic Mode ✅
```
Table: my_table
Partitions:
  - 20260101: 1000 records (UNCHANGED)
  - 20260102: 1000 records (500 deleted, 1000 retained)
  - 20260103: 2000 records (UNCHANGED)
  - 20260104: 1800 records (UNCHANGED)
Total: 5800 records
```

### What Would Happen with Static Mode ❌
```
Table: my_table
Partitions:
  - 20260102: 1000 records (only this partition exists!)
Total: 1000 records
ALL OTHER PARTITIONS DELETED! ❌
```

## How Spark Determines Which Partitions to Overwrite

When using `insertInto()` with dynamic partition mode:

1. **Spark reads the partition column values** from the DataFrame being written
2. **Identifies unique partition values** (e.g., `[20260101, 20260102]`)
3. **Overwrites ONLY those partitions** in the target table
4. **Leaves all other partitions untouched**

## Code Flow Example

```java
// Step 1: Original table has partitions [20260101, 20260102, 20260103]

// Step 2: Read data to retain from partitions [20260101, 20260102]
String partitionFilter = "partition_id IN ('20260101', '20260102')";
Dataset<Row> dataToRetain = spark.sql(
    "SELECT * FROM my_table WHERE " + partitionFilter + " AND status != 'DELETED'"
);

// Step 3: dataToRetain contains:
// - partition_id = 20260101: 800 records
// - partition_id = 20260102: 1200 records

// Step 4: Write back with dynamic mode
dataToRetain.write()
    .mode(SaveMode.Overwrite)
    .insertInto("my_table");

// Step 5: Spark detects partition values in dataToRetain
// Partition values found: [20260101, 20260102]

// Step 6: Overwrites ONLY those partitions
// - partition_id = 20260101: OVERWRITTEN with 800 records
// - partition_id = 20260102: OVERWRITTEN with 1200 records
// - partition_id = 20260103: UNCHANGED (not in dataToRetain)
```

## Verification Commands

To verify dynamic mode is working in your Spark session:

```scala
// Check current setting
spark.conf.get("spark.sql.sources.partitionOverwriteMode")
// Should return: "dynamic"

// Set explicitly if needed
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

## Additional Safety: Partition Filter

Our code adds an extra layer of safety by using partition filters:

```java
String partitionFilter = PartitionUtils.buildPartitionFilter(
    config.getPartitionColumn(), 
    partitionsToBackup
);
// Generates: partition_id IN ('20260101', '20260102')
```

This ensures we only read and write the specific partitions we intend to modify.

## Conclusion

✅ **YES, the configuration is correct!**

The dynamic partition overwrite mode ensures that:
1. Only affected partitions are overwritten
2. Other partitions remain completely untouched
3. No accidental data loss from unrelated partitions
4. Safe for production use

The configuration is set in `SparkSessionManager.java` and applies to all write operations throughout the application.
