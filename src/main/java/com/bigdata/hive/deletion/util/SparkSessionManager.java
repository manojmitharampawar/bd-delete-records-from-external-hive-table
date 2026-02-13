package com.bigdata.hive.deletion.util;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Manages SparkSession creation and configuration for Hive table operations.
 */
public class SparkSessionManager {
    private static final Logger logger = LoggerFactory.getLogger(SparkSessionManager.class);

    /**
     * Creates and configures a SparkSession with Hive support.
     *
     * @param appName          Application name
     * @param additionalConfig Additional Spark configuration properties
     * @return Configured SparkSession
     */
    public static SparkSession createSparkSession(String appName, Properties additionalConfig) {
        logger.info("Creating SparkSession with app name: {}", appName);

        SparkSession.Builder builder = SparkSession.builder()
                .appName(appName)
                .enableHiveSupport();

        // Apply default configurations for ORC and Hive operations
        builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.orc.impl", "native")
                .config("spark.sql.orc.enableVectorizedReader", "true")
                .config("spark.sql.orc.filterPushdown", "true")
                .config("spark.sql.hive.convertMetastoreOrc", "true")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.hive.verifyPartitionPath", "true")
                .config("spark.sql.hive.metastorePartitionPruning", "true")
                .config("spark.sql.orc.compression.codec", "snappy");

        // Apply additional configurations
        if (additionalConfig != null) {
            for (String key : additionalConfig.stringPropertyNames()) {
                if (key.startsWith("spark.")) {
                    String value = additionalConfig.getProperty(key);
                    logger.debug("Applying Spark config: {} = {}", key, value);
                    builder.config(key, value);
                }
            }
        }

        SparkSession spark = builder.getOrCreate();

        logger.info("SparkSession created successfully");
        logger.info("Spark version: {}", spark.version());
        logger.info("Hive metastore: {}", spark.conf().get("spark.sql.warehouse.dir", "default"));

        return spark;
    }

    /**
     * Creates a SparkSession with default configuration.
     *
     * @param appName Application name
     * @return Configured SparkSession
     */
    public static SparkSession createSparkSession(String appName) {
        return createSparkSession(appName, null);
    }

    /**
     * Stops the SparkSession gracefully.
     *
     * @param spark SparkSession to stop
     */
    public static void stopSparkSession(SparkSession spark) {
        if (spark != null) {
            logger.info("Stopping SparkSession");
            spark.stop();
            logger.info("SparkSession stopped successfully");
        }
    }

    /**
     * Validates that the SparkSession has Hive support enabled.
     *
     * @param spark SparkSession to validate
     * @throws IllegalStateException if Hive support is not enabled
     */
    public static void validateHiveSupport(SparkSession spark) {
        try {
            spark.sql("SHOW DATABASES").count();
            logger.info("Hive support validated successfully");
        } catch (Exception e) {
            throw new IllegalStateException("SparkSession does not have Hive support enabled", e);
        }
    }
}
