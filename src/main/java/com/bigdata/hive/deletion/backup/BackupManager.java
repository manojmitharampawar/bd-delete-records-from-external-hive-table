package com.bigdata.hive.deletion.backup;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.hive.deletion.config.JobConfig;
import com.bigdata.hive.deletion.util.MetricsCollector;
import com.bigdata.hive.deletion.util.PartitionUtils;

/**
 * Manages backup operations for Hive table data.
 * Delegates to specific BackupStrategy implementations.
 */
public class BackupManager {
    private static final Logger logger = LoggerFactory.getLogger(BackupManager.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

    private final BackupStrategy strategy;
    private final MetricsCollector metrics;

    public BackupManager(BackupStrategy strategy, MetricsCollector metrics) {
        this.strategy = strategy;
        this.metrics = metrics;
    }

    /**
     * Creates a backup of affected partitions before deletion.
     *
     * @param spark              SparkSession
     * @param config             Job configuration
     * @param partitionsToBackup List of partition IDs to backup
     * @return Backup location/identifier
     * @throws Exception if backup fails
     */
    public String createBackup(SparkSession spark, JobConfig config, List<String> partitionsToBackup) throws Exception {
        logger.info("Starting backup creation for {} partitions", partitionsToBackup.size());
        auditLogger.info("BACKUP_START - Table: {}, Partitions: {}", config.getFullTableName(), partitionsToBackup);

        long startTime = System.currentTimeMillis();

        try {
            // Validate partitions exist
            validatePartitionsExist(spark, config, partitionsToBackup);

            // Create backup using strategy
            String backupLocation = strategy.createBackup(spark, config, partitionsToBackup);

            // Validate backup
            long recordCount = countRecordsInPartitions(spark, config, partitionsToBackup);
            boolean isValid = strategy.validateBackup(spark, config, backupLocation, recordCount);

            if (!isValid) {
                throw new RuntimeException("Backup validation failed");
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Backup created successfully in {} ms. Location: {}", duration, backupLocation);
            auditLogger.info("BACKUP_SUCCESS - Location: {}, Records: {}, Duration: {} ms",
                    backupLocation, recordCount, duration);

            metrics.markBackupCreated(backupLocation);

            return backupLocation;

        } catch (Exception e) {
            logger.error("Backup creation failed", e);
            auditLogger.error("BACKUP_FAILED - Table: {}, Error: {}", config.getFullTableName(), e.getMessage());
            throw new RuntimeException("Failed to create backup", e);
        }
    }

    /**
     * Restores data from backup.
     *
     * @param spark          SparkSession
     * @param config         Job configuration
     * @param backupLocation Backup location/identifier
     * @throws Exception if restore fails
     */
    public void restoreFromBackup(SparkSession spark, JobConfig config, String backupLocation) throws Exception {
        logger.info("Starting restore from backup: {}", backupLocation);
        auditLogger.info("RESTORE_START - Table: {}, Backup: {}", config.getFullTableName(), backupLocation);

        long startTime = System.currentTimeMillis();

        try {
            strategy.restoreFromBackup(spark, config, backupLocation);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Restore completed successfully in {} ms", duration);
            auditLogger.info("RESTORE_SUCCESS - Table: {}, Duration: {} ms", config.getFullTableName(), duration);

        } catch (Exception e) {
            logger.error("Restore failed", e);
            auditLogger.error("RESTORE_FAILED - Table: {}, Error: {}", config.getFullTableName(), e.getMessage());
            throw new RuntimeException("Failed to restore from backup", e);
        }
    }

    /**
     * Validates that all partitions exist in the table.
     */
    private void validatePartitionsExist(SparkSession spark, JobConfig config, List<String> partitions) {
        String sql = String.format("SHOW PARTITIONS %s", config.getFullTableName());
        Dataset<Row> existingPartitions = spark.sql(sql);

        List<String> existingPartitionIds = existingPartitions.collectAsList().stream()
                .map(row -> row.getString(0).split("=")[1])
                .collect(java.util.stream.Collectors.toList());

        for (String partition : partitions) {
            if (!existingPartitionIds.contains(partition)) {
                throw new IllegalArgumentException("Partition does not exist: " + partition);
            }
        }

        logger.info("All {} partitions validated successfully", partitions.size());
    }

    /**
     * Counts total records in specified partitions.
     */
    private long countRecordsInPartitions(SparkSession spark, JobConfig config, List<String> partitions) {
        String partitionFilter = PartitionUtils.buildPartitionFilter(config.getPartitionColumn(), partitions);

        String sql = String.format("SELECT COUNT(*) FROM %s WHERE %s",
                config.getFullTableName(), partitionFilter);

        long count = spark.sql(sql).first().getLong(0);
        logger.debug("Total records in partitions: {}", count);

        return count;
    }

    /**
     * Cleans up old backups based on retention policy.
     */
    public void cleanupOldBackups(SparkSession spark, JobConfig config) {
        logger.info("Cleaning up old backups (retention: {} days)", config.getBackupRetentionDays());
        try {
            strategy.cleanupOldBackups(spark, config);
            logger.info("Old backups cleaned up successfully");
        } catch (Exception e) {
            logger.warn("Failed to cleanup old backups", e);
        }
    }

    /**
     * Factory method to create BackupManager with appropriate strategy.
     */
    public static BackupManager create(JobConfig config, MetricsCollector metrics) {
        BackupStrategy strategy;

        switch (config.getBackupStrategy().toLowerCase()) {
            case "hive_table":
                strategy = new HiveTableBackupStrategy();
                break;
            case "hdfs":
                strategy = new HDFSBackupStrategy();
                break;
            default:
                throw new IllegalArgumentException("Unknown backup strategy: " + config.getBackupStrategy());
        }

        return new BackupManager(strategy, metrics);
    }
}
