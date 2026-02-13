package com.bigdata.hive.deletion;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.hive.deletion.backup.BackupManager;
import com.bigdata.hive.deletion.config.DeletionCriteria;
import com.bigdata.hive.deletion.config.JobConfig;
import com.bigdata.hive.deletion.deletion.DeletionExecutor;
import com.bigdata.hive.deletion.deletion.PartitionHandler;
import com.bigdata.hive.deletion.recovery.RecoveryManager;
import com.bigdata.hive.deletion.util.MetricsCollector;
import com.bigdata.hive.deletion.util.SparkSessionManager;
import com.bigdata.hive.deletion.validation.ValidationManager;

/**
 * Main entry point for the Hive Table Deletion Job.
 * Orchestrates backup, deletion, validation, and recovery operations.
 */
public class HiveTableDeletionJob {
    private static final Logger logger = LoggerFactory.getLogger(HiveTableDeletionJob.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

    public static void main(String[] args) {
        SparkSession spark = null;
        MetricsCollector metrics = new MetricsCollector();
        try {
            // Parse command line arguments
            CommandLine cmd = parseArguments(args);

            // Load configuration
            JobConfig config = loadConfiguration(cmd);

            logger.info("Starting Hive Table Deletion Job");
            logger.info("Configuration: {}", config);
            auditLogger.info("JOB_START - Table: {}, Criteria: {}", config.getFullTableName(),
                    config.getDeletionCriteria());

            // Create Spark session
            spark = SparkSessionManager.createSparkSession(config.getFullTableName() + "_deletion");
            SparkSessionManager.validateHiveSupport(spark);

            // Execute deletion workflow
            boolean success = executeDeletionWorkflow(spark, config, metrics);

            if (success) {
                logger.info("Job completed successfully");
                logger.info(metrics.generateReport());
                auditLogger.info("JOB_SUCCESS");
                System.exit(0);
            } else {
                logger.error("Job completed with errors");
                logger.info(metrics.generateReport());
                auditLogger.error("JOB_FAILED");
                System.exit(1);
            }

        } catch (Exception e) {
            logger.error("Job failed with exception", e);
            auditLogger.error("JOB_EXCEPTION - Error: {}", e.getMessage());
            System.exit(1);

        } finally {
            metrics.markJobEnd();
            if (spark != null) {
                SparkSessionManager.stopSparkSession(spark);
            }
        }
    }

    /**
     * Executes the main deletion workflow.
     */
    private static boolean executeDeletionWorkflow(SparkSession spark, JobConfig config, MetricsCollector metrics) {
        String backupLocation = null;
        BackupManager backupManager = null;
        RecoveryManager recoveryManager = null;

        try {
            // Initialize managers
            PartitionHandler partitionHandler = new PartitionHandler(spark, config);
            ValidationManager validationManager = new ValidationManager(spark, config, metrics);
            backupManager = BackupManager.create(config, metrics);
            recoveryManager = new RecoveryManager(spark, config, backupManager);

            // Step 1: Identify affected partitions
            logger.info("Step 1: Identifying affected partitions");
            List<String> affectedPartitions = partitionHandler.identifyAffectedPartitions();

            if (affectedPartitions.isEmpty()) {
                logger.warn("No partitions affected by deletion criteria. Exiting.");
                return true;
            }

            // Step 2: Pre-deletion validation
            logger.info("Step 2: Performing pre-deletion validation");
            validationManager.validatePreDeletion(affectedPartitions);

            // Step 3: Create backup
            logger.info("Step 3: Creating backup");
            backupLocation = backupManager.createBackup(spark, config, affectedPartitions);

            // Step 4: Get counts before deletion
            long recordsBefore = partitionHandler.getRecordCount(affectedPartitions);
            long recordsToDelete = partitionHandler.getMatchingRecordCount(affectedPartitions);
            long expectedRetained = recordsBefore - recordsToDelete;

            logger.info("Records before deletion: {}", recordsBefore);
            logger.info("Records to delete: {}", recordsToDelete);
            logger.info("Expected records after deletion: {}", expectedRetained);

            // Step 5: Execute deletion
            logger.info("Step 5: Executing deletion");
            DeletionExecutor deletionExecutor = new DeletionExecutor(spark, config, metrics);
            long actualDeleted = deletionExecutor.executeDeletion(affectedPartitions);

            if (config.isDryRun()) {
                logger.info("Dry run completed. No actual changes made.");
                return true;
            }

            // Step 6: Post-deletion validation
            logger.info("Step 6: Performing post-deletion validation");
            validationManager.validatePostDeletion(affectedPartitions, actualDeleted, expectedRetained);

            // Step 7: Cleanup old backups
            logger.info("Step 7: Cleaning up old backups");
            backupManager.cleanupOldBackups(spark, config);

            logger.info("Deletion workflow completed successfully");
            return true;

        } catch (Exception e) {
            logger.error("Deletion workflow failed", e);

            // Attempt recovery if backup was created
            if (backupLocation != null && recoveryManager != null) {
                boolean recovered = recoveryManager.recoverFromFailure(backupLocation, e);

                if (!recovered) {
                    recoveryManager.logManualRecoveryInstructions(backupLocation);
                }
            }

            return false;
        }
    }

    /**
     * Parses command line arguments.
     */
    private static CommandLine parseArguments(String[] args) throws ParseException {
        Options options = new Options();

        options.addOption(Option.builder("d")
                .longOpt("database")
                .hasArg()
                .required()
                .desc("Database name")
                .build());

        options.addOption(Option.builder("t")
                .longOpt("table")
                .hasArg()
                .required()
                .desc("Table name")
                .build());

        options.addOption(Option.builder("w")
                .longOpt("where")
                .hasArg()
                .desc("WHERE clause for deletion criteria")
                .build());

        options.addOption(Option.builder("s")
                .longOpt("start-time")
                .hasArg()
                .desc("Start time for time window (format: yyyy-MM-dd HH:mm:ss)")
                .build());

        options.addOption(Option.builder("e")
                .longOpt("end-time")
                .hasArg()
                .desc("End time for time window (format: yyyy-MM-dd HH:mm:ss)")
                .build());

        options.addOption(Option.builder("tc")
                .longOpt("time-column")
                .hasArg()
                .desc("Time column name (default: row_create_ts)")
                .build());

        options.addOption(Option.builder("pc")
                .longOpt("partition-column")
                .hasArg()
                .desc("Partition column name (default: partition_id)")
                .build());

        options.addOption(Option.builder("bs")
                .longOpt("backup-strategy")
                .hasArg()
                .desc("Backup strategy: hive_table or hdfs (default: hive_table)")
                .build());

        options.addOption(Option.builder("bl")
                .longOpt("backup-location")
                .hasArg()
                .desc("Backup location (for HDFS strategy)")
                .build());

        options.addOption(Option.builder("dr")
                .longOpt("dry-run")
                .hasArg(false)
                .desc("Dry run mode (no actual deletion)")
                .build());

        options.addOption(Option.builder("c")
                .longOpt("config")
                .hasArg()
                .desc("Path to configuration properties file")
                .build());

        options.addOption(Option.builder("h")
                .longOpt("help")
                .hasArg(false)
                .desc("Print help message")
                .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("HiveTableDeletionJob", options);
                System.exit(0);
            }

            return cmd;

        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HiveTableDeletionJob", options);
            throw e;
        }
    }

    /**
     * Loads job configuration from command line arguments and properties file.
     */
    private static JobConfig loadConfiguration(CommandLine cmd) throws Exception {
        // Load default properties
        Properties props = new Properties();
        try (InputStream input = HiveTableDeletionJob.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input != null) {
                props.load(input);
            }
        }

        // Load custom properties file if specified
        if (cmd.hasOption("config")) {
            try (FileInputStream input = new FileInputStream(cmd.getOptionValue("config"))) {
                props.load(input);
            }
        }

        // Build deletion criteria
        DeletionCriteria.Builder criteriaBuilder = new DeletionCriteria.Builder();

        if (cmd.hasOption("where")) {
            criteriaBuilder.whereClause(cmd.getOptionValue("where"));
        }

        if (cmd.hasOption("start-time")) {
            criteriaBuilder.startTime(cmd.getOptionValue("start-time"));
        }

        if (cmd.hasOption("end-time")) {
            criteriaBuilder.endTime(cmd.getOptionValue("end-time"));
        }

        if (cmd.hasOption("time-column")) {
            criteriaBuilder.timeColumn(cmd.getOptionValue("time-column"));
        }

        DeletionCriteria criteria = criteriaBuilder.build();

        // Build job configuration
        JobConfig.Builder configBuilder = new JobConfig.Builder()
                .database(cmd.getOptionValue("database"))
                .tableName(cmd.getOptionValue("table"))
                .deletionCriteria(criteria)
                .fromProperties(props);

        if (cmd.hasOption("partition-column")) {
            configBuilder.partitionColumn(cmd.getOptionValue("partition-column"));
        }

        if (cmd.hasOption("backup-strategy")) {
            configBuilder.backupStrategy(cmd.getOptionValue("backup-strategy"));
        }

        if (cmd.hasOption("backup-location")) {
            configBuilder.backupLocation(cmd.getOptionValue("backup-location"));
        }

        if (cmd.hasOption("dry-run")) {
            configBuilder.dryRun(true);
        }

        return configBuilder.build();
    }
}
