# Hive Table Deletion Job

Production-grade Apache Spark job for safely deleting records from external Hive tables with ORC format.

## Features

- ✅ **Data Safety**: Automatic backup before deletion with rollback capability
- ✅ **Efficiency**: Partition-aware processing to minimize data scanning
- ✅ **Accuracy**: Multi-level validation to prevent accidental data loss
- ✅ **Robustness**: Comprehensive error handling and recovery mechanisms
- ✅ **Flexibility**: Support for complex partition naming schemes
- ✅ **Dry Run**: Preview deletions without making changes
- ✅ **Audit Trail**: Comprehensive logging for compliance

## Quick Start

```bash
# Build
mvn clean package

# Run with dry-run
spark-submit \
  --class com.bigdata.hive.deletion.HiveTableDeletionJob \
  target/hive-table-deletion-1.0.0.jar \
  --database my_db \
  --table my_table \
  --where "status = 'INACTIVE'" \
  --start-time "2026-01-01 00:00:00" \
  --end-time "2026-02-01 00:00:00" \
  --dry-run
```

## Documentation

- [README.md](README.md) - Complete usage guide
- [TESTING.md](TESTING.md) - Test documentation
- [PARTITION_GUIDE.md](PARTITION_GUIDE.md) - Partition handling guide
- [DYNAMIC_PARTITION_VERIFICATION.md](DYNAMIC_PARTITION_VERIFICATION.md) - Dynamic partition verification

## Requirements

- Java 1.8+
- Apache Spark 3.3.2
- Apache Hadoop 3.3.2
- Maven 3.6+

## License

Internal use only.
