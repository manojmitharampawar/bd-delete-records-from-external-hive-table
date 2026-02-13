package com.bigdata.hive.deletion.backup;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.sql.SparkSession;

import com.bigdata.hive.deletion.config.JobConfig;

/**
 * Interface defining backup strategies for Hive table data.
 */
public interface BackupStrategy extends Serializable {

    /**
     * Creates a backup of the specified partitions.
     *
     * @param spark              SparkSession
     * @param config             Job configuration
     * @param partitionsToBackup List of partition IDs to backup
     * @return Backup location/identifier
     * @throws Exception if backup fails
     */
    String createBackup(SparkSession spark, JobConfig config, List<String> partitionsToBackup) throws Exception;

    /**
     * Restores data from backup.
     *
     * @param spark          SparkSession
     * @param config         Job configuration
     * @param backupLocation Backup location/identifier
     * @throws Exception if restore fails
     */
    void restoreFromBackup(SparkSession spark, JobConfig config, String backupLocation) throws Exception;

    /**
     * Validates that the backup was created successfully.
     *
     * @param spark               SparkSession
     * @param config              Job configuration
     * @param backupLocation      Backup location/identifier
     * @param expectedRecordCount Expected number of records in backup
     * @return true if backup is valid, false otherwise
     */
    boolean validateBackup(SparkSession spark, JobConfig config, String backupLocation, long expectedRecordCount);

    /**
     * Cleans up old backups based on retention policy.
     *
     * @param spark  SparkSession
     * @param config Job configuration
     */
    void cleanupOldBackups(SparkSession spark, JobConfig config);
}
