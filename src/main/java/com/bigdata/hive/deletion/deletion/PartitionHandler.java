package com.bigdata.hive.deletion.deletion;

import com.bigdata.hive.deletion.config.JobConfig;
import com.bigdata.hive.deletion.util.PartitionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handles partition-related operations for the deletion job.
 */
public class PartitionHandler {
    private static final Logger logger = LoggerFactory.getLogger(PartitionHandler.class);

    private final SparkSession spark;
    private final JobConfig config;

    public PartitionHandler(SparkSession spark, JobConfig config) {
        this.spark = spark;
        this.config = config;
    }

    /**
     * Identifies partitions affected by the deletion criteria.
     *
     * @return List of partition IDs that contain records matching deletion criteria
     */
    public List<String> identifyAffectedPartitions() {
        logger.info("Identifying affected partitions for table: {}", config.getFullTableName());

        try {
            // Get all partitions from the table
            List<String> allPartitions = getAllPartitions();
            logger.info("Total partitions in table: {}", allPartitions.size());

            // Filter partitions based on deletion criteria
            List<String> affectedPartitions = filterPartitionsByCriteria(allPartitions);
            logger.info("Affected partitions: {}", affectedPartitions.size());

            if (affectedPartitions.isEmpty()) {
                logger.warn("No partitions match the deletion criteria");
            } else {
                logger.info("Affected partition IDs: {}", affectedPartitions);
            }

            return affectedPartitions;

        } catch (Exception e) {
            logger.error("Error identifying affected partitions", e);
            throw new RuntimeException("Failed to identify affected partitions", e);
        }
    }

    /**
     * Gets all partition IDs from the table.
     */
    private List<String> getAllPartitions() {
        String sql = String.format("SHOW PARTITIONS %s", config.getFullTableName());
        Dataset<Row> partitions = spark.sql(sql);

        return partitions.collectAsList().stream()
                .map(row -> {
                    // Extract partition value from "partition_id=20260213" format
                    String partSpec = row.getString(0);
                    return partSpec.split("=")[1];
                })
                .collect(Collectors.toList());
    }

    /**
     * Filters partitions based on deletion criteria.
     * Only includes partitions that actually contain records matching the criteria.
     */
    private List<String> filterPartitionsByCriteria(List<String> allPartitions) {
        List<String> affectedPartitions = new ArrayList<>();

        // First, filter by time window if specified
        List<String> candidatePartitions = allPartitions;
        if (config.getDeletionCriteria().getStartTime() != null ||
                config.getDeletionCriteria().getEndTime() != null) {

            try {
                candidatePartitions = PartitionUtils.filterPartitionsByDateRange(
                        allPartitions,
                        config.getDeletionCriteria().getStartTime(),
                        config.getDeletionCriteria().getEndTime());
                logger.info("Partitions after date range filter: {}", candidatePartitions.size());
            } catch (Exception e) {
                logger.warn("Error filtering by date range, using all partitions", e);
            }
        }

        // For each candidate partition, check if it contains matching records
        String whereClause = config.getDeletionCriteria().getCompleteWhereClause();

        for (String partition : candidatePartitions) {
            String partitionFilter = PartitionUtils.buildPartitionSpec(config.getPartitionColumn(), partition);

            String countSql = String.format(
                    "SELECT COUNT(*) FROM %s WHERE %s AND (%s)",
                    config.getFullTableName(),
                    partitionFilter,
                    whereClause);

            try {
                long matchingRecords = spark.sql(countSql).first().getLong(0);

                if (matchingRecords > 0) {
                    affectedPartitions.add(partition);
                    logger.debug("Partition {} has {} matching records", partition, matchingRecords);
                }
            } catch (Exception e) {
                logger.error("Error checking partition {}: {}", partition, e.getMessage());
                // Include partition to be safe
                affectedPartitions.add(partition);
            }
        }

        return affectedPartitions;
    }

    /**
     * Validates that all specified partitions exist.
     */
    public void validatePartitionsExist(List<String> partitions) {
        List<String> allPartitions = getAllPartitions();

        for (String partition : partitions) {
            if (!allPartitions.contains(partition)) {
                throw new IllegalArgumentException("Partition does not exist: " + partition);
            }
        }

        logger.info("All {} partitions validated successfully", partitions.size());
    }

    /**
     * Gets record count for specific partitions.
     */
    public long getRecordCount(List<String> partitions) {
        if (partitions.isEmpty()) {
            return 0;
        }

        String partitionFilter = PartitionUtils.buildPartitionFilter(config.getPartitionColumn(), partitions);
        String sql = String.format("SELECT COUNT(*) FROM %s WHERE %s",
                config.getFullTableName(), partitionFilter);

        return spark.sql(sql).first().getLong(0);
    }

    /**
     * Gets record count matching deletion criteria in specific partitions.
     */
    public long getMatchingRecordCount(List<String> partitions) {
        if (partitions.isEmpty()) {
            return 0;
        }

        String partitionFilter = PartitionUtils.buildPartitionFilter(config.getPartitionColumn(), partitions);
        String whereClause = config.getDeletionCriteria().getCompleteWhereClause();

        String sql = String.format("SELECT COUNT(*) FROM %s WHERE %s AND (%s)",
                config.getFullTableName(), partitionFilter, whereClause);

        return spark.sql(sql).first().getLong(0);
    }
}
