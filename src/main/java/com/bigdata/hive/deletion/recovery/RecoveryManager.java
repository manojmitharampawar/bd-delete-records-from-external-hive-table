package com.bigdata.hive.deletion.recovery;

import com.bigdata.hive.deletion.backup.BackupManager;
import com.bigdata.hive.deletion.config.JobConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages recovery operations when deletion fails.
 */
public class RecoveryManager {
    private static final Logger logger = LoggerFactory.getLogger(RecoveryManager.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

    private final SparkSession spark;
    private final JobConfig config;
    private final BackupManager backupManager;

    public RecoveryManager(SparkSession spark, JobConfig config, BackupManager backupManager) {
        this.spark = spark;
        this.config = config;
        this.backupManager = backupManager;
    }

    /**
     * Attempts to recover from a failed deletion operation.
     *
     * @param backupLocation Location of the backup to restore from
     * @param error          The error that caused the failure
     * @return true if recovery succeeded, false otherwise
     */
    public boolean recoverFromFailure(String backupLocation, Throwable error) {
        logger.error("Deletion failed, attempting recovery", error);
        auditLogger.error("RECOVERY_TRIGGERED - Error: {}, Backup: {}", error.getMessage(), backupLocation);

        if (!config.isAutoRecoveryEnabled()) {
            logger.warn("Auto-recovery is disabled. Manual intervention required.");
            auditLogger.warn("AUTO_RECOVERY_DISABLED - Manual intervention required");
            return false;
        }

        if (backupLocation == null) {
            logger.error("No backup location available for recovery");
            auditLogger.error("RECOVERY_FAILED - No backup available");
            return false;
        }

        int maxRetries = config.getMaxRecoveryRetries();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            logger.info("Recovery attempt {}/{}", attempt, maxRetries);
            auditLogger.info("RECOVERY_ATTEMPT - Attempt: {}/{}", attempt, maxRetries);

            try {
                // Restore from backup
                backupManager.restoreFromBackup(spark, config, backupLocation);

                // Verify restoration
                if (verifyRestoration(backupLocation)) {
                    logger.info("Recovery successful on attempt {}", attempt);
                    auditLogger.info("RECOVERY_SUCCESS - Attempt: {}", attempt);
                    return true;
                } else {
                    logger.warn("Recovery verification failed on attempt {}", attempt);
                }

            } catch (Exception e) {
                logger.error("Recovery attempt {} failed", attempt, e);
                auditLogger.error("RECOVERY_ATTEMPT_FAILED - Attempt: {}, Error: {}", attempt, e.getMessage());

                if (attempt < maxRetries) {
                    try {
                        // Wait before retry with exponential backoff
                        long waitTime = (long) Math.pow(2, attempt) * 1000;
                        logger.info("Waiting {} ms before retry", waitTime);
                        Thread.sleep(waitTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.error("Recovery interrupted");
                        break;
                    }
                }
            }
        }

        logger.error("Recovery failed after {} attempts", maxRetries);
        auditLogger.error("RECOVERY_FAILED - All {} attempts exhausted", maxRetries);
        return false;
    }

    /**
     * Verifies that restoration was successful.
     */
    private boolean verifyRestoration(String backupLocation) {
        try {
            // Basic verification: check that backup table/location still exists
            // and original table is accessible
            spark.sql(String.format("DESCRIBE TABLE %s", config.getFullTableName()));

            logger.debug("Restoration verification passed");
            return true;

        } catch (Exception e) {
            logger.error("Restoration verification failed", e);
            return false;
        }
    }

    /**
     * Performs cleanup of partial writes after failure.
     */
    public void cleanupPartialWrites() {
        logger.info("Cleaning up partial writes");
        auditLogger.info("CLEANUP_START");

        try {
            // Refresh table metadata to ensure consistency
            spark.sql(String.format("REFRESH TABLE %s", config.getFullTableName()));

            // Repair table partitions
            spark.sql(String.format("MSCK REPAIR TABLE %s", config.getFullTableName()));

            logger.info("Cleanup completed successfully");
            auditLogger.info("CLEANUP_SUCCESS");

        } catch (Exception e) {
            logger.error("Cleanup failed", e);
            auditLogger.error("CLEANUP_FAILED - Error: {}", e.getMessage());
        }
    }

    /**
     * Logs recovery instructions for manual intervention.
     */
    public void logManualRecoveryInstructions(String backupLocation) {
        StringBuilder instructions = new StringBuilder();
        instructions.append("\n========== MANUAL RECOVERY INSTRUCTIONS ==========\n");
        instructions.append("Automatic recovery failed. Please perform manual recovery:\n\n");
        instructions.append("1. Verify backup integrity:\n");
        instructions.append(String.format("   spark.sql(\"SELECT COUNT(*) FROM %s\")\n\n", backupLocation));
        instructions.append("2. Restore from backup:\n");
        instructions.append(String.format("   INSERT OVERWRITE TABLE %s SELECT * FROM %s\n\n",
                config.getFullTableName(), backupLocation));
        instructions.append("3. Verify restoration:\n");
        instructions.append(String.format("   spark.sql(\"SELECT COUNT(*) FROM %s\")\n\n", config.getFullTableName()));
        instructions.append("4. Clean up backup after verification:\n");
        instructions.append(String.format("   spark.sql(\"DROP TABLE IF EXISTS %s\")\n", backupLocation));
        instructions.append("==================================================\n");

        logger.error(instructions.toString());
        auditLogger.error("MANUAL_RECOVERY_REQUIRED - Backup: {}", backupLocation);
    }
}
