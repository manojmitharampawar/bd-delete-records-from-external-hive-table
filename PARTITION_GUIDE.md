# Summary: partitionBy() vs saveAsTable() vs insertInto()

## Quick Reference

### ✅ CORRECT Usage

```java
// 1. Creating NEW table with partitionBy() + saveAsTable()
dataFrame.write()
    .mode(SaveMode.Overwrite)
    .format("orc")
    .partitionBy("partition_column")  // ✅ OK with saveAsTable()
    .saveAsTable("database.new_table");

// 2. Writing to EXISTING partitioned table with insertInto()
dataFrame.write()
    .mode(SaveMode.Overwrite)
    .format("orc")
    // NO partitionBy() here!
    .insertInto("database.existing_table");  // ✅ Correct

// 3. Saving to HDFS path with partitionBy()
dataFrame.write()
    .mode(SaveMode.Overwrite)
    .format("orc")
    .partitionBy("partition_column")  // ✅ OK with save()
    .save("/hdfs/path");
```

### ❌ INCORRECT Usage

```java
// WRONG: partitionBy() + insertInto() together
dataFrame.write()
    .mode(SaveMode.Overwrite)
    .format("orc")
    .partitionBy("partition_column")  // ❌ ERROR!
    .insertInto("database.existing_table");

// Error: insertInto() can't be used together with partitionBy()
```

## Why This Matters

### When Table is Already Partitioned

If a Hive table is created with:
```sql
CREATE TABLE my_table (
  id BIGINT,
  name STRING
) PARTITIONED BY (partition_id STRING)
STORED AS ORC;
```

The partition structure is **already defined** in the metastore.

### Using insertInto()

- `insertInto()` writes to an **existing table**
- Spark reads the partition structure from the metastore
- The partition columns are **already known**
- Adding `partitionBy()` creates a conflict

### Using saveAsTable()

- `saveAsTable()` creates a **new table**
- Spark needs to know how to partition the new table
- You **must** specify `partitionBy()` to create partitions

## Fixed Files

1. ✅ `DeletionExecutor.java` - Removed `partitionBy()` from deletion logic
2. ✅ `HiveTableBackupStrategy.java` - Removed `partitionBy()` from restore
3. ✅ `HDFSBackupStrategy.java` - Removed `partitionBy()` from restore

## Dynamic Partition Overwrite

Our code uses dynamic partition overwrite mode:

```java
spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
```

**What this does:**
- Only overwrites partitions that exist in the written data
- Preserves other partitions in the table
- Automatically detects partition values from the data
- No need to explicitly specify `partitionBy()`

## Example Scenario

```java
// Original table has partitions: [20260101, 20260102, 20260103]

// Write data for only partition 20260102
dataFrame.write()
    .mode(SaveMode.Overwrite)
    .insertInto("my_table");

// Result: Only 20260102 is overwritten
// Partitions 20260101 and 20260103 remain unchanged
```

## Key Takeaways

1. **partitionBy() + saveAsTable()** = ✅ Creating new partitioned table
2. **partitionBy() + save()** = ✅ Writing to HDFS with partitions
3. **insertInto()** alone = ✅ Writing to existing partitioned table
4. **partitionBy() + insertInto()** = ❌ ERROR - Don't do this!
