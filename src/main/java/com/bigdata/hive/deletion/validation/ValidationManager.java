package com.bigdata.hive.deletion.validation;

import com.bigdata.hive.deletion.config.JobConfig;
import com.bigdata.hive.deletion.deletion.PartitionHandler;
import com.bigdata.hive.deletion.util.MetricsCollector;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Manages validation operations before and after deletion.
 */
public class ValidationManager {
    private static final Logger logger = LoggerFactory.getLogger(ValidationManager.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

    private final SparkSession spark;
    private final JobConfig config;
    private final MetricsCollector metrics;
    private final DataIntegrityValidator integrityValidator;

    public ValidationManager(SparkSession spark, JobConfig config, MetricsCollector metrics) {
        this.spark = spark;
        this.config = config;
        this.metrics = metrics;
        this.integrityValidator = new DataIntegrityValidator(spark, config);
    }

    /**
     * Performs pre-deletion validation.
     *
     * @param partitions Partitions to be processed
     * @throws ValidationException if validation fails
     */
    public void validatePreDeletion(List<String> partitions) throws ValidationException {
        logger.info("Starting pre-deletion validation");
        auditLogger.info("PRE_VALIDATION_START - Partitions: {}", partitions.size());

        try {
            // Validate table exists and is accessible
            validateTableExists();

            // Validate partitions exist
            validatePartitionsExist(partitions);

            // Validate deletion criteria
            validateDeletionCriteria();

            // Validate sufficient storage for backup
            // (This would require HDFS API calls - simplified here)

            logger.info("Pre-deletion validation passed");
            auditLogger.info("PRE_VALIDATION_SUCCESS");

        } catch (Exception e) {
            logger.error("Pre-deletion validation failed", e);
            auditLogger.error("PRE_VALIDATION_FAILED - Error: {}", e.getMessage());
            throw new ValidationException("Pre-deletion validation failed", e);
        }
    }

    /**
     * Performs post-deletion validation.
     *
     * @param partitions      Partitions that were processed
     * @param recordsDeleted  Number of records deleted
     * @param recordsRetained Number of records retained
     * @throws ValidationException if validation fails
     */
    public void validatePostDeletion(List<String> partitions, long recordsDeleted, long recordsRetained)
            throws ValidationException {

        if (!config.isValidationEnabled()) {
            logger.info("Post-deletion validation is disabled");
            return;
        }

        logger.info("Starting post-deletion validation");
        auditLogger.info("POST_VALIDATION_START - Expected deleted: {}, Expected retained: {}",
                recordsDeleted, recordsRetained);

        try {
            // Validate record counts
            validateRecordCounts(partitions, recordsDeleted, recordsRetained);

            // Validate data integrity
            validateDataIntegrity(partitions);

            // Validate no records matching deletion criteria remain
            validateNoMatchingRecordsRemain(partitions);

            logger.info("Post-deletion validation passed");
            auditLogger.info("POST_VALIDATION_SUCCESS");

            metrics.markValidationPassed(true);

        } catch (Exception e) {
            logger.error("Post-deletion validation failed", e);
            auditLogger.error("POST_VALIDATION_FAILED - Error: {}", e.getMessage());
            metrics.markValidationPassed(false);
            throw new ValidationException("Post-deletion validation failed", e);
        }
    }

    /**
     * Validates that the table exists and is accessible.
     */
    private void validateTableExists() {
        try {
            spark.sql(String.format("DESCRIBE TABLE %s", config.getFullTableName()));
            logger.debug("Table exists: {}", config.getFullTableName());
        } catch (Exception e) {
            throw new ValidationException("Table does not exist or is not accessible: " + config.getFullTableName(), e);
        }
    }

    /**
     * Validates that all partitions exist.
     */
    private void validatePartitionsExist(List<String> partitions) {
        PartitionHandler partitionHandler = new PartitionHandler(spark, config);
        partitionHandler.validatePartitionsExist(partitions);
    }

    /**
     * Validates deletion criteria syntax and logic.
     */
    private void validateDeletionCriteria() {
        try {
            config.getDeletionCriteria().validate();
            logger.debug("Deletion criteria validated");
        } catch (Exception e) {
            throw new ValidationException("Invalid deletion criteria", e);
        }
    }

    /**
     * Validates record counts after deletion.
     */
    private void validateRecordCounts(List<String> partitions, long expectedDeleted, long expectedRetained) {
        PartitionHandler partitionHandler = new PartitionHandler(spark, config);

        // Get actual record count after deletion
        long actualCount = partitionHandler.getRecordCount(partitions);

        // Calculate tolerance
        double tolerancePercent = config.getValidationTolerancePercent();
        long tolerance = (long) (expectedRetained * tolerancePercent / 100.0);

        long lowerBound = expectedRetained - tolerance;
        long upperBound = expectedRetained + tolerance;

        if (actualCount < lowerBound || actualCount > upperBound) {
            String message = String.format(
                    "Record count validation failed. Expected: %d (±%d), Actual: %d",
                    expectedRetained, tolerance, actualCount);
            throw new ValidationException(message);
        }

        logger.info("Record count validation passed. Expected: {}, Actual: {}", expectedRetained, actualCount);
    }

    /**
     * Validates data integrity using sampling.
     */
    private void validateDataIntegrity(List<String> partitions) {
        boolean isValid = integrityValidator.validateIntegrity(partitions);

        if (!isValid) {
            throw new ValidationException("Data integrity validation failed");
        }

        logger.info("Data integrity validation passed");
    }

    /**
     * Validates that no records matching deletion criteria remain.
     */
    private void validateNoMatchingRecordsRemain(List<String> partitions) {
        PartitionHandler partitionHandler = new PartitionHandler(spark, config);

        long matchingRecords = partitionHandler.getMatchingRecordCount(partitions);

        if (matchingRecords > 0) {
            String message = String.format(
                    "Found %d records still matching deletion criteria after deletion",
                    matchingRecords);
            throw new ValidationException(message);
        }

        logger.info("Verified no records matching deletion criteria remain");
    }

    /**
     * Custom exception for validation failures.
     */
    public static class ValidationException extends RuntimeException {
        private static final long serialVersionUID = -6182306104167465263L;

		public ValidationException(String message) {
            super(message);
        }

        public ValidationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
