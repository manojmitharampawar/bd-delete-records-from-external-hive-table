package com.bigdata.hive.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.sql.Timestamp;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.bigdata.hive.deletion.config.DeletionCriteria;
import com.bigdata.hive.deletion.config.JobConfig;
import com.bigdata.hive.deletion.deletion.DeletionExecutor;
import com.bigdata.hive.deletion.deletion.PartitionHandler;
import com.bigdata.hive.deletion.util.MetricsCollector;

/**
 * Integration test for HiveTableDeletionJob.
 * Creates a temporary Hive table, loads sample data, performs deletion, and
 * validates results.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HiveTableDeletionJobIntegrationTest {

    private SparkSession spark;
    private static final String TEST_DATABASE = "test_db";
    private static final String TEST_TABLE = "test_deletion_table";
    private static final String TEST_PARTITION = "20260213";

    @BeforeAll
    public void setUp() {
        // Create SparkSession with Hive support for testing
        String warehouseDir = System.getProperty("java.io.tmpdir") + "/spark-warehouse-" + System.currentTimeMillis();
        String metastoreDir = System.getProperty("java.io.tmpdir") + "/metastore_db-" + System.currentTimeMillis();

        spark = SparkSession.builder()
                .appName("HiveTableDeletionJobTest")
                .master("local[2]")
                .config("spark.sql.warehouse.dir", warehouseDir)
                .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + metastoreDir + ";create=true")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.catalogImplementation", "hive")
                .enableHiveSupport()
                .getOrCreate();

        // Set log level to reduce noise
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("SparkSession created with warehouse: " + warehouseDir);
    }

    @AfterAll
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @BeforeEach
    public void setupTestTable() {
        // Create test database
        spark.sql("CREATE DATABASE IF NOT EXISTS " + TEST_DATABASE);

        // Drop test table if exists
        spark.sql("DROP TABLE IF EXISTS " + TEST_DATABASE + "." + TEST_TABLE);

        // Create test table with partition
        String createTableSql = String.format(
                "CREATE TABLE %s.%s (" +
                        "  id BIGINT, " +
                        "  name STRING, " +
                        "  status STRING, " +
                        "  row_create_ts TIMESTAMP" +
                        ") PARTITIONED BY (partition_id STRING) " +
                        "STORED AS ORC",
                TEST_DATABASE, TEST_TABLE);

        spark.sql(createTableSql);
        System.out.println("Test table created: " + TEST_DATABASE + "." + TEST_TABLE);
    }

    @AfterEach
    public void cleanupTestTable() {
        // Drop test table after each test
        spark.sql("DROP TABLE IF EXISTS " + TEST_DATABASE + "." + TEST_TABLE);

        // Drop any backup tables
        Dataset<Row> tables = spark.sql("SHOW TABLES IN " + TEST_DATABASE);
        List<Row> tableList = tables.collectAsList();

        for (Row table : tableList) {
            String tableName = table.getString(1);
            if (tableName.startsWith(TEST_TABLE + "_backup_")) {
                spark.sql("DROP TABLE IF EXISTS " + TEST_DATABASE + "." + tableName);
                System.out.println("Dropped backup table: " + tableName);
            }
        }
    }

    @Test
    @DisplayName("Test deletion of 1 record from 10 records")
    public void testDeleteOneRecordFromTen() {
        // Step 1: Load 10 sample records
        loadSampleData(10);

        // Step 2: Verify initial count
        long initialCount = getRecordCount();
        assertEquals(10, initialCount, "Initial record count should be 10");
        System.out.println("Initial record count: " + initialCount);

        // Step 3: Configure deletion criteria to delete 1 record (id = 5)
        DeletionCriteria criteria = new DeletionCriteria.Builder()
                .whereClause("id = 5")
                .startTime(Timestamp.valueOf("2026-02-13 00:00:00"))
                .endTime(Timestamp.valueOf("2026-02-13 23:59:59"))
                .build();

        JobConfig config = new JobConfig.Builder()
                .database(TEST_DATABASE)
                .tableName(TEST_TABLE)
                .partitionColumn("partition_id")
                .deletionCriteria(criteria)
                .backupStrategy("hive_table")
                .validationEnabled(true)
                .build();

        // Step 4: Execute deletion
        MetricsCollector metrics = new MetricsCollector();
        DeletionExecutor executor = new DeletionExecutor(spark, config, metrics);

        PartitionHandler partitionHandler = new PartitionHandler(spark, config);
        List<String> affectedPartitions = partitionHandler.identifyAffectedPartitions();

        assertFalse(affectedPartitions.isEmpty(), "Should have affected partitions");
        System.out.println("Affected partitions: " + affectedPartitions);

        long deletedCount = executor.executeDeletion(affectedPartitions);

        // Step 5: Validate deletion
        assertEquals(1, deletedCount, "Should have deleted 1 record");
        System.out.println("Deleted record count: " + deletedCount);

        // Step 6: Verify final count
        long finalCount = getRecordCount();
        assertEquals(9, finalCount, "Final record count should be 9");
        System.out.println("Final record count: " + finalCount);

        // Step 7: Verify the deleted record is gone
        long deletedRecordCount = spark.sql(
                String.format("SELECT COUNT(*) FROM %s.%s WHERE id = 5",
                        TEST_DATABASE, TEST_TABLE))
                .first().getLong(0);

        assertEquals(0, deletedRecordCount, "Record with id=5 should be deleted");

        // Step 8: Verify remaining records
        Dataset<Row> remainingRecords = spark.sql(
                String.format("SELECT id FROM %s.%s ORDER BY id", TEST_DATABASE, TEST_TABLE));

        List<Row> records = remainingRecords.collectAsList();
        assertEquals(9, records.size(), "Should have 9 remaining records");

        // Verify IDs are 1,2,3,4,6,7,8,9,10 (missing 5)
        long[] expectedIds = { 1, 2, 3, 4, 6, 7, 8, 9, 10 };
        for (int i = 0; i < expectedIds.length; i++) {
            assertEquals(expectedIds[i], records.get(i).getLong(0),
                    "Record at position " + i + " should have expected ID");
        }

        System.out.println("✓ Test passed: Successfully deleted 1 record from 10");
    }

    @Test
    @DisplayName("Test deletion with time window filter")
    public void testDeleteWithTimeWindow() {
        // Load 10 records with different timestamps
        loadSampleDataWithTimestamps();

        long initialCount = getRecordCount();
        assertEquals(10, initialCount, "Initial record count should be 10");

        // Delete records created between 10:00 and 12:00 (should be 2 records: id 3 and
        // 4)
        DeletionCriteria criteria = new DeletionCriteria.Builder()
                .startTime(Timestamp.valueOf("2026-02-13 10:00:00"))
                .endTime(Timestamp.valueOf("2026-02-13 12:00:00"))
                .build();

        JobConfig config = new JobConfig.Builder()
                .database(TEST_DATABASE)
                .tableName(TEST_TABLE)
                .partitionColumn("partition_id")
                .deletionCriteria(criteria)
                .backupStrategy("hive_table")
                .build();

        MetricsCollector metrics = new MetricsCollector();
        DeletionExecutor executor = new DeletionExecutor(spark, config, metrics);
        PartitionHandler partitionHandler = new PartitionHandler(spark, config);

        List<String> affectedPartitions = partitionHandler.identifyAffectedPartitions();
        long deletedCount = executor.executeDeletion(affectedPartitions);

        assertEquals(2, deletedCount, "Should have deleted 2 records");

        long finalCount = getRecordCount();
        assertEquals(8, finalCount, "Final record count should be 8");

        System.out.println("✓ Test passed: Time window deletion successful");
    }

    @Test
    @DisplayName("Test deletion with status filter")
    public void testDeleteByStatus() {
        // Load sample data
        loadSampleData(10);

        long initialCount = getRecordCount();
        assertEquals(10, initialCount, "Initial record count should be 10");

        // Delete records with status = 'INACTIVE' (should be 3 records: id 2, 5, 8)
        DeletionCriteria criteria = new DeletionCriteria.Builder()
                .whereClause("status = 'INACTIVE'")
                .build();

        JobConfig config = new JobConfig.Builder()
                .database(TEST_DATABASE)
                .tableName(TEST_TABLE)
                .partitionColumn("partition_id")
                .deletionCriteria(criteria)
                .backupStrategy("hive_table")
                .build();

        MetricsCollector metrics = new MetricsCollector();
        DeletionExecutor executor = new DeletionExecutor(spark, config, metrics);
        PartitionHandler partitionHandler = new PartitionHandler(spark, config);

        List<String> affectedPartitions = partitionHandler.identifyAffectedPartitions();
        long deletedCount = executor.executeDeletion(affectedPartitions);

        assertEquals(3, deletedCount, "Should have deleted 3 records");

        long finalCount = getRecordCount();
        assertEquals(7, finalCount, "Final record count should be 7");

        // Verify no INACTIVE records remain
        long inactiveCount = spark.sql(
                String.format("SELECT COUNT(*) FROM %s.%s WHERE status = 'INACTIVE'",
                        TEST_DATABASE, TEST_TABLE))
                .first().getLong(0);

        assertEquals(0, inactiveCount, "No INACTIVE records should remain");

        System.out.println("✓ Test passed: Status-based deletion successful");
    }

    @Test
    @DisplayName("Test dry run mode does not delete records")
    public void testDryRunMode() {
        // Load sample data
        loadSampleData(10);

        long initialCount = getRecordCount();
        assertEquals(10, initialCount, "Initial record count should be 10");

        // Configure deletion with dry-run enabled
        DeletionCriteria criteria = new DeletionCriteria.Builder()
                .whereClause("id = 5")
                .build();

        JobConfig config = new JobConfig.Builder()
                .database(TEST_DATABASE)
                .tableName(TEST_TABLE)
                .partitionColumn("partition_id")
                .deletionCriteria(criteria)
                .dryRun(true) // Enable dry-run
                .build();

        MetricsCollector metrics = new MetricsCollector();
        DeletionExecutor executor = new DeletionExecutor(spark, config, metrics);
        PartitionHandler partitionHandler = new PartitionHandler(spark, config);

        List<String> affectedPartitions = partitionHandler.identifyAffectedPartitions();
        long deletedCount = executor.executeDeletion(affectedPartitions);

        // In dry-run mode, it reports what would be deleted
        assertEquals(1, deletedCount, "Should report 1 record would be deleted");

        // But actual count should remain unchanged
        long finalCount = getRecordCount();
        assertEquals(10, finalCount, "Record count should still be 10 (no actual deletion)");

        System.out.println("✓ Test passed: Dry-run mode does not delete records");
    }

    /**
     * Helper method to load sample data into the test table.
     */
    private void loadSampleData(int recordCount) {
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(TEST_DATABASE).append(".").append(TEST_TABLE)
                .append(" PARTITION (partition_id='").append(TEST_PARTITION).append("') VALUES ");

        for (int i = 1; i <= recordCount; i++) {
            if (i > 1) {
                insertSql.append(", ");
            }

            String status = (i % 3 == 2) ? "INACTIVE" : "ACTIVE";
            String timestamp = "2026-02-13 09:00:00";

            insertSql.append(String.format("(%d, 'User%d', '%s', TIMESTAMP '%s')",
                    i, i, status, timestamp));
        }

        spark.sql(insertSql.toString());
        System.out.println("Loaded " + recordCount + " sample records");
    }

    /**
     * Helper method to load sample data with varying timestamps.
     */
    private void loadSampleDataWithTimestamps() {
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(TEST_DATABASE).append(".").append(TEST_TABLE)
                .append(" PARTITION (partition_id='").append(TEST_PARTITION).append("') VALUES ");

        for (int i = 1; i <= 10; i++) {
            if (i > 1) {
                insertSql.append(", ");
            }

            String timestamp = String.format("2026-02-13 %02d:00:00", 8 + i);

            insertSql.append(String.format("(%d, 'User%d', 'ACTIVE', TIMESTAMP '%s')",
                    i, i, timestamp));
        }

        spark.sql(insertSql.toString());
        System.out.println("Loaded 10 sample records with varying timestamps");
    }

    /**
     * Helper method to get current record count.
     */
    private long getRecordCount() {
        return spark.sql(
                String.format("SELECT COUNT(*) FROM %s.%s", TEST_DATABASE, TEST_TABLE)).first().getLong(0);
    }
}
