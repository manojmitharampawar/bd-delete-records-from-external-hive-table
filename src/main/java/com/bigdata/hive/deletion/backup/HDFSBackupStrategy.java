package com.bigdata.hive.deletion.backup;

import com.bigdata.hive.deletion.config.JobConfig;
import com.bigdata.hive.deletion.util.PartitionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * Backup strategy that writes data to HDFS directory.
 */
public class HDFSBackupStrategy implements BackupStrategy {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(HDFSBackupStrategy.class);
    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmss");

    @Override
    public String createBackup(SparkSession spark, JobConfig config, List<String> partitionsToBackup) throws Exception {
        String backupPath = generateBackupPath(config);

        logger.info("Creating HDFS backup at: {}", backupPath);

        // Read data from partitions to backup
        String partitionFilter = PartitionUtils.buildPartitionFilter(config.getPartitionColumn(), partitionsToBackup);
        String selectSql = String.format("SELECT * FROM %s WHERE %s",
                config.getFullTableName(), partitionFilter);

        Dataset<Row> dataToBackup = spark.sql(selectSql);

        // Write to HDFS in ORC format with partitioning
        dataToBackup.write()
                .mode(SaveMode.Overwrite)
                .format("orc")
                .partitionBy(config.getPartitionColumn())
                .save(backupPath);

        // Write metadata file
        writeBackupMetadata(spark, config, backupPath, partitionsToBackup);

        logger.info("HDFS backup created successfully at: {}", backupPath);

        return backupPath;
    }

    @Override
    public void restoreFromBackup(SparkSession spark, JobConfig config, String backupLocation) throws Exception {
        logger.info("Restoring from HDFS backup: {}", backupLocation);

        // Read data from HDFS backup
        Dataset<Row> backupData = spark.read()
                .format("orc")
                .load(backupLocation);

        // Write data back to original table using dynamic partition overwrite
        // Note: Don't use partitionBy() with insertInto() - table is already
        // partitioned
        backupData.write()
                .mode(SaveMode.Overwrite)
                .format("orc")
                .insertInto(config.getFullTableName());

        logger.info("Data restored successfully from HDFS backup");
    }

    @Override
    public boolean validateBackup(SparkSession spark, JobConfig config, String backupLocation,
            long expectedRecordCount) {
        try {
            Dataset<Row> backupData = spark.read()
                    .format("orc")
                    .load(backupLocation);

            long backupCount = backupData.count();

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
            String baseBackupPath = getBaseBackupPath(config);
            Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
            FileSystem fs = FileSystem.get(hadoopConf);

            Path basePath = new Path(baseBackupPath);
            if (!fs.exists(basePath)) {
                return;
            }

            long cutoffTime = System.currentTimeMillis() - (config.getBackupRetentionDays() * 24L * 60 * 60 * 1000);

            org.apache.hadoop.fs.FileStatus[] backupDirs = fs.listStatus(basePath);

            for (org.apache.hadoop.fs.FileStatus dir : backupDirs) {
                if (dir.isDirectory()) {
                    long modTime = dir.getModificationTime();

                    if (modTime < cutoffTime) {
                        logger.info("Deleting old backup directory: {}", dir.getPath());
                        fs.delete(dir.getPath(), true);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Error cleaning up old HDFS backups", e);
        }
    }

    /**
     * Generates backup path in HDFS.
     */
    private String generateBackupPath(JobConfig config) {
        String baseBackupPath = getBaseBackupPath(config);
        String timestamp = TIMESTAMP_FORMAT.format(new Date());
        return baseBackupPath + "/" + timestamp;
    }

    /**
     * Gets base backup path from config or uses default.
     */
    private String getBaseBackupPath(JobConfig config) {
        if (config.getBackupLocation() != null) {
            return config.getBackupLocation();
        }
        return "/backup/" + config.getDatabase() + "/" + config.getTableName();
    }

    /**
     * Writes metadata file with backup information.
     */
    private void writeBackupMetadata(SparkSession spark, JobConfig config, String backupPath, List<String> partitions) {
        try {
            Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
            FileSystem fs = FileSystem.get(hadoopConf);

            Path metadataPath = new Path(backupPath + "/_metadata.txt");
            java.io.OutputStream out = fs.create(metadataPath, true);

            StringBuilder metadata = new StringBuilder();
            metadata.append("Source Table: ").append(config.getFullTableName()).append("\n");
            metadata.append("Backup Timestamp: ").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
                    .append("\n");
            metadata.append("Partitions: ").append(String.join(",", partitions)).append("\n");

            out.write(metadata.toString().getBytes());
            out.close();

            logger.debug("Backup metadata written to: {}", metadataPath);

        } catch (Exception e) {
            logger.warn("Failed to write backup metadata", e);
        }
    }
}
