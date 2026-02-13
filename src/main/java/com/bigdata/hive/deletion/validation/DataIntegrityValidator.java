package com.bigdata.hive.deletion.validation;

import com.bigdata.hive.deletion.config.JobConfig;
import com.bigdata.hive.deletion.util.PartitionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Validates data integrity after deletion operations.
 */
public class DataIntegrityValidator {
    private static final Logger logger = LoggerFactory.getLogger(DataIntegrityValidator.class);

    private final SparkSession spark;
    private final JobConfig config;

    public DataIntegrityValidator(SparkSession spark, JobConfig config) {
        this.spark = spark;
        this.config = config;
    }

    /**
     * Validates data integrity by sampling retained records.
     *
     * @param partitions Partitions to validate
     * @return true if validation passes, false otherwise
     */
    public boolean validateIntegrity(List<String> partitions) {
        logger.info("Starting data integrity validation");

        try {
            // Sample records from retained data
            Dataset<Row> sampledData = sampleRetainedData(partitions);

            if (sampledData.isEmpty()) {
                logger.info("No data to validate (all records deleted)");
                return true;
            }

            // Verify sampled records don't match deletion criteria
            boolean noMatchingRecords = verifyNoMatchingRecords(sampledData);

            if (!noMatchingRecords) {
                logger.error("Found records matching deletion criteria in retained data");
                return false;
            }

            // Verify partition structure is intact
            boolean partitionStructureValid = verifyPartitionStructure(partitions);

            if (!partitionStructureValid) {
                logger.error("Partition structure validation failed");
                return false;
            }

            logger.info("Data integrity validation passed");
            return true;

        } catch (Exception e) {
            logger.error("Error during data integrity validation", e);
            return false;
        }
    }

    /**
     * Samples retained data from specified partitions.
     */
    private Dataset<Row> sampleRetainedData(List<String> partitions) {
        String partitionFilter = PartitionUtils.buildPartitionFilter(config.getPartitionColumn(), partitions);

        String sql = String.format("SELECT * FROM %s WHERE %s",
                config.getFullTableName(), partitionFilter);

        Dataset<Row> data = spark.sql(sql);

        // Sample data based on configuration
        int sampleSize = config.getValidationSampleSize();
        long totalRecords = data.count();

        if (totalRecords == 0) {
            return data;
        }

        if (totalRecords <= sampleSize) {
            return data;
        }

        // Calculate sample fraction
        double fraction = (double) sampleSize / totalRecords;
        return data.sample(false, fraction);
    }

    /**
     * Verifies that sampled records don't match deletion criteria.
     */
    private boolean verifyNoMatchingRecords(Dataset<Row> sampledData) {
        String whereClause = config.getDeletionCriteria().getCompleteWhereClause();

        // Create temporary view for validation
        sampledData.createOrReplaceTempView("sampled_data");

        String sql = String.format("SELECT COUNT(*) FROM sampled_data WHERE %s", whereClause);

        long matchingCount = spark.sql(sql).first().getLong(0);

        if (matchingCount > 0) {
            logger.error("Found {} sampled records matching deletion criteria", matchingCount);
            return false;
        }

        logger.debug("No sampled records match deletion criteria");
        return true;
    }

    /**
     * Verifies partition structure is intact.
     */
    private boolean verifyPartitionStructure(List<String> partitions) {
        try {
            for (String partition : partitions) {
                String sql = String.format("SHOW PARTITIONS %s PARTITION (%s)",
                        config.getFullTableName(),
                        PartitionUtils.buildPartitionSpec(config.getPartitionColumn(), partition));

                Dataset<Row> result = spark.sql(sql);

                if (result.isEmpty()) {
                    logger.error("Partition structure validation failed for: {}", partition);
                    return false;
                }
            }

            logger.debug("Partition structure validated for {} partitions", partitions.size());
            return true;

        } catch (Exception e) {
            logger.error("Error verifying partition structure", e);
            return false;
        }
    }
}
