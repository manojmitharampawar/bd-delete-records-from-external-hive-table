package com.bigdata.hive.deletion.backup;

import com.bigdata.hive.deletion.config.JobConfig;
import com.bigdata.hive.deletion.util.PartitionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Backup strategy that creates a separate Hive table for backup.
 */
public class HiveTableBackupStrategy implements BackupStrategy {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(HiveTableBackupStrategy.class);
    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmss");

    @Override
    public String createBackup(SparkSession spark, JobConfig config, List<String> partitionsToBackup) throws Exception {
        String backupTableName = generateBackupTableName(config);
        String fullBackupTableName = config.getDatabase() + "." + backupTableName;

        logger.info("Creating backup table: {}", fullBackupTableName);

        // Read data from partitions to backup
        String partitionFilter = PartitionUtils.buildPartitionFilter(config.getPartitionColumn(), partitionsToBackup);
        String selectSql = String.format("SELECT * FROM %s WHERE %s",
                config.getFullTableName(), partitionFilter);

        Dataset<Row> dataToBackup = spark.sql(selectSql);

        // Create backup table with same schema
        dataToBackup.write()
                .mode(SaveMode.Overwrite)
                .format("orc")
                .partitionBy(config.getPartitionColumn())
                .saveAsTable(fullBackupTableName);

        // Add table properties to mark as backup
        String alterSql = String.format(
                "ALTER TABLE %s SET TBLPROPERTIES ('backup_source'='%s', 'backup_timestamp'='%s', 'backup_partitions'='%s')",
                fullBackupTableName,
                config.getFullTableName(),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),
                String.join(",", partitionsToBackup));
        spark.sql(alterSql);

        logger.info("Backup table created successfully: {}", fullBackupTableName);

        return fullBackupTableName;
    }

    @Override
    public void restoreFromBackup(SparkSession spark, JobConfig config, String backupLocation) throws Exception {
        logger.info("Restoring from backup table: {}", backupLocation);

        // Read all data from backup table
        Dataset<Row> backupData = spark.table(backupLocation);

        // Write data back to original table using dynamic partition overwrite
        // Note: Don't use partitionBy() with insertInto() - table is already
        // partitioned
        backupData.write()
                .mode(SaveMode.Overwrite)
                .format("orc")
                .insertInto(config.getFullTableName());

        logger.info("Data restored successfully from backup table");
    }

    @Override
    public boolean validateBackup(SparkSession spark, JobConfig config, String backupLocation,
            long expectedRecordCount) {
        try {
            long backupCount = spark.table(backupLocation).count();

            if (backupCount != expectedRecordCount) {
                logger.error("Backup validation failed. Expected: {}, Actual: {}", expectedRecordCount, backupCount);
                return false;
            }

            logger.info("Backup validation passed. Record count: {}", backupCount);
            return true;

        } catch (Exception e) {
            logger.error("Error validating backup", e);
            return false;
        }
    }

    @Override
    public void cleanupOldBackups(SparkSession spark, JobConfig config) {
        try {
            // Get all tables in database
            Dataset<Row> tables = spark.sql(String.format("SHOW TABLES IN %s", config.getDatabase()));

            // Filter backup tables for this source table
            String backupPrefix = config.getTableName() + "_backup_";
            long cutoffTime = System.currentTimeMillis() - (config.getBackupRetentionDays() * 24L * 60 * 60 * 1000);

            List<Row> backupTables = tables.collectAsList().stream()
                    .filter(row -> row.getString(1).startsWith(backupPrefix))
                    .collect(java.util.stream.Collectors.toList());

            for (Row table : backupTables) {
                String tableName = table.getString(1);
                String fullTableName = config.getDatabase() + "." + tableName;

                try {
                    // Get backup timestamp from table properties
                    Dataset<Row> props = spark.sql(String.format("SHOW TBLPROPERTIES %s", fullTableName));
                    String timestampStr = props.filter("key = 'backup_timestamp'")
                            .select("value")
                            .first()
                            .getString(0);

                    Date backupDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestampStr);

                    if (backupDate.getTime() < cutoffTime) {
                        logger.info("Dropping old backup table: {}", fullTableName);
                        spark.sql(String.format("DROP TABLE IF EXISTS %s", fullTableName));
                    }

                } catch (Exception e) {
                    logger.warn("Error processing backup table {}: {}", tableName, e.getMessage());
                }
            }

        } catch (Exception e) {
            logger.error("Error cleaning up old backups", e);
        }
    }

    /**
     * Generates a unique backup table name with timestamp.
     */
    private String generateBackupTableName(JobConfig config) {
        String timestamp = TIMESTAMP_FORMAT.format(new Date());
        return config.getTableName() + "_backup_" + timestamp;
    }
}
