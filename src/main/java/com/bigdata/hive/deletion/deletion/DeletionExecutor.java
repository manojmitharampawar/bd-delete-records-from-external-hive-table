package com.bigdata.hive.deletion.deletion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.hive.deletion.config.JobConfig;
import com.bigdata.hive.deletion.util.MetricsCollector;
import com.bigdata.hive.deletion.util.PartitionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes the deletion operation on Hive table partitions.
 */
public class DeletionExecutor {
    private static final Logger logger = LoggerFactory.getLogger(DeletionExecutor.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

    private final SparkSession spark;
    private final JobConfig config;
    private final MetricsCollector metrics;

    public DeletionExecutor(SparkSession spark, JobConfig config, MetricsCollector metrics) {
        this.spark = spark;
        this.config = config;
        this.metrics = metrics;
    }

    /**
     * Executes deletion on specified partitions.
     *
     * @param partitions List of partition IDs to process
     * @return Number of records deleted
     */
    public long executeDeletion(List<String> partitions) {
        logger.info("Starting deletion execution for {} partitions", partitions.size());
        auditLogger.info("DELETION_START - Table: {}, Partitions: {}, Criteria: {}",
                config.getFullTableName(), partitions, config.getDeletionCriteria());

        long totalDeleted = 0;
        long startTime = System.currentTimeMillis();

        try {
            if (config.isDryRun()) {
                logger.info("DRY RUN MODE - No actual deletion will be performed");
                totalDeleted = performDryRun(partitions);
            } else {
                totalDeleted = performActualDeletion(partitions);
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Deletion completed. Records deleted: {}, Duration: {} ms", totalDeleted, duration);
            auditLogger.info("DELETION_SUCCESS - Records deleted: {}, Duration: {} ms", totalDeleted, duration);

            metrics.recordRecordsDeleted(totalDeleted);

            return totalDeleted;

        } catch (Exception e) {
            logger.error("Deletion execution failed", e);
            auditLogger.error("DELETION_FAILED - Error: {}", e.getMessage());
            throw new RuntimeException("Failed to execute deletion", e);
        }
    }

    /**
     * Performs dry run to preview deletion without making changes.
     */
    private long performDryRun(List<String> partitions) {
        logger.info("Performing dry run analysis");

        String partitionFilter = PartitionUtils.buildPartitionFilter(config.getPartitionColumn(), partitions);
        String whereClause = config.getDeletionCriteria().getCompleteWhereClause();

        // Count records that would be deleted
        String deleteSql = String.format(
                "SELECT COUNT(*) FROM %s WHERE %s AND (%s)",
                config.getFullTableName(), partitionFilter, whereClause);

        long recordsToDelete = spark.sql(deleteSql).first().getLong(0);

        // Count records that would be retained
        String retentionWhereClause = config.getDeletionCriteria().getRetentionWhereClause();
        String retainSql = String.format(
                "SELECT COUNT(*) FROM %s WHERE %s AND (%s)",
                config.getFullTableName(), partitionFilter, retentionWhereClause);

        long recordsToRetain = spark.sql(retainSql).first().getLong(0);

        logger.info("DRY RUN RESULTS:");
        logger.info("  Records to delete: {}", recordsToDelete);
        logger.info("  Records to retain: {}", recordsToRetain);
        logger.info("  Affected partitions: {}", partitions);

        auditLogger.info("DRY_RUN - Would delete {} records, retain {} records",
                recordsToDelete, recordsToRetain);

        return recordsToDelete;
    }

    /**
     * Performs actual deletion by overwriting partitions with filtered data.
     */
    private long performActualDeletion(List<String> partitions) {
        logger.info("Performing actual deletion");

        long totalRecordsDeleted = 0;

        // Process partitions in batches for better performance
        int batchSize = Math.min(config.getPartitionParallelism(), partitions.size());
        List<List<String>> batches = createBatches(partitions, batchSize);

        logger.info("Processing {} partitions in {} batches", partitions.size(), batches.size());

        for (int i = 0; i < batches.size(); i++) {
            List<String> batch = batches.get(i);
            logger.info("Processing batch {}/{} with {} partitions", i + 1, batches.size(), batch.size());

            long batchDeleted = processBatch(batch);
            totalRecordsDeleted += batchDeleted;

            metrics.incrementPartitionsProcessed();
        }

        return totalRecordsDeleted;
    }

    /**
     * Processes a batch of partitions.
     */
    private long processBatch(List<String> partitionBatch) {
        String partitionFilter = PartitionUtils.buildPartitionFilter(config.getPartitionColumn(), partitionBatch);

        // Count records before deletion
        String countBeforeSql = String.format(
                "SELECT COUNT(*) FROM %s WHERE %s",
                config.getFullTableName(), partitionFilter);
        long recordsBefore = spark.sql(countBeforeSql).first().getLong(0);
        metrics.recordRecordsRead(recordsBefore);

        // Get retention WHERE clause (inverse of deletion criteria)
        String retentionWhereClause = config.getDeletionCriteria().getRetentionWhereClause();

        // Read data that should be retained
        String selectSql = String.format(
                "SELECT * FROM %s WHERE %s AND (%s)",
                config.getFullTableName(), partitionFilter, retentionWhereClause);

        Dataset<Row> dataToRetain = spark.sql(selectSql);
        long recordsToRetain = dataToRetain.count();
        metrics.recordRecordsRetained(recordsToRetain);

        logger.info("Batch: {} records before, {} to retain, {} to delete",
                recordsBefore, recordsToRetain, recordsBefore - recordsToRetain);

        // Overwrite partitions with retained data using dynamic partition mode
        // Note: Don't use partitionBy() with insertInto() - table is already
        // partitioned
        dataToRetain.write()
                .mode(SaveMode.Overwrite)
                .format("orc")
                .insertInto(config.getFullTableName());

        // Record metrics for each partition
        for (String partition : partitionBatch) {
            metrics.recordPartitionMetric(partition, recordsToRetain);
        }

        return recordsBefore - recordsToRetain;
    }

    /**
     * Creates batches from partition list.
     */
    private List<List<String>> createBatches(List<String> partitions, int batchSize) {
        List<List<String>> batches = new ArrayList<>();

        for (int i = 0; i < partitions.size(); i += batchSize) {
            int end = Math.min(i + batchSize, partitions.size());
            batches.add(partitions.subList(i, end));
        }

        return batches;
    }
}
