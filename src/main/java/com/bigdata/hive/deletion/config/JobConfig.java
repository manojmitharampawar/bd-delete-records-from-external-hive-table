package com.bigdata.hive.deletion.config;

import org.apache.commons.lang3.StringUtils;
import java.io.Serializable;
import java.util.Properties;

/**
 * Configuration holder for the Hive table deletion job.
 * Contains all parameters needed for backup, deletion, validation, and
 * recovery.
 */
public class JobConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // Table configuration
    private final String database;
    private final String tableName;
    private final String partitionColumn;

    // Deletion criteria
    private final DeletionCriteria deletionCriteria;

    // Backup configuration
    private final String backupStrategy;
    private final String backupLocation;
    private final int backupRetentionDays;

    // Validation configuration
    private final boolean validationEnabled;
    private final int validationSampleSize;
    private final double validationTolerancePercent;

    // Recovery configuration
    private final boolean autoRecoveryEnabled;
    private final int maxRecoveryRetries;

    // Performance configuration
    private final int partitionParallelism;
    private final long batchSize;

    // Execution mode
    private final boolean dryRun;

    private JobConfig(Builder builder) {
        this.database = builder.database;
        this.tableName = builder.tableName;
        this.partitionColumn = builder.partitionColumn;
        this.deletionCriteria = builder.deletionCriteria;
        this.backupStrategy = builder.backupStrategy;
        this.backupLocation = builder.backupLocation;
        this.backupRetentionDays = builder.backupRetentionDays;
        this.validationEnabled = builder.validationEnabled;
        this.validationSampleSize = builder.validationSampleSize;
        this.validationTolerancePercent = builder.validationTolerancePercent;
        this.autoRecoveryEnabled = builder.autoRecoveryEnabled;
        this.maxRecoveryRetries = builder.maxRecoveryRetries;
        this.partitionParallelism = builder.partitionParallelism;
        this.batchSize = builder.batchSize;
        this.dryRun = builder.dryRun;
    }

    // Getters
    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFullTableName() {
        return database + "." + tableName;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public DeletionCriteria getDeletionCriteria() {
        return deletionCriteria;
    }

    public String getBackupStrategy() {
        return backupStrategy;
    }

    public String getBackupLocation() {
        return backupLocation;
    }

    public int getBackupRetentionDays() {
        return backupRetentionDays;
    }

    public boolean isValidationEnabled() {
        return validationEnabled;
    }

    public int getValidationSampleSize() {
        return validationSampleSize;
    }

    public double getValidationTolerancePercent() {
        return validationTolerancePercent;
    }

    public boolean isAutoRecoveryEnabled() {
        return autoRecoveryEnabled;
    }

    public int getMaxRecoveryRetries() {
        return maxRecoveryRetries;
    }

    public int getPartitionParallelism() {
        return partitionParallelism;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    /**
     * Validates the configuration.
     * 
     * @throws IllegalArgumentException if configuration is invalid
     */
    public void validate() {
        if (StringUtils.isBlank(database)) {
            throw new IllegalArgumentException("Database name cannot be empty");
        }
        if (StringUtils.isBlank(tableName)) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }
        if (StringUtils.isBlank(partitionColumn)) {
            throw new IllegalArgumentException("Partition column cannot be empty");
        }
        if (deletionCriteria == null) {
            throw new IllegalArgumentException("Deletion criteria cannot be null");
        }
        deletionCriteria.validate();

        if (backupRetentionDays < 0) {
            throw new IllegalArgumentException("Backup retention days must be non-negative");
        }
        if (validationSampleSize < 0) {
            throw new IllegalArgumentException("Validation sample size must be non-negative");
        }
        if (validationTolerancePercent < 0 || validationTolerancePercent > 100) {
            throw new IllegalArgumentException("Validation tolerance percent must be between 0 and 100");
        }
        if (maxRecoveryRetries < 0) {
            throw new IllegalArgumentException("Max recovery retries must be non-negative");
        }
        if (partitionParallelism <= 0) {
            throw new IllegalArgumentException("Partition parallelism must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive");
        }
    }

    @Override
    public String toString() {
        return "JobConfig{" +
                "database='" + database + '\'' +
                ", tableName='" + tableName + '\'' +
                ", partitionColumn='" + partitionColumn + '\'' +
                ", deletionCriteria=" + deletionCriteria +
                ", backupStrategy='" + backupStrategy + '\'' +
                ", dryRun=" + dryRun +
                '}';
    }

    /**
     * Builder for JobConfig
     */
    public static class Builder {
        private String database;
        private String tableName;
        private String partitionColumn = "partition_id";
        private DeletionCriteria deletionCriteria;
        private String backupStrategy = "hive_table";
        private String backupLocation;
        private int backupRetentionDays = 7;
        private boolean validationEnabled = true;
        private int validationSampleSize = 10000;
        private double validationTolerancePercent = 0.0;
        private boolean autoRecoveryEnabled = true;
        private int maxRecoveryRetries = 3;
        private int partitionParallelism = 10;
        private long batchSize = 1000000;
        private boolean dryRun = false;

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder partitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
            return this;
        }

        public Builder deletionCriteria(DeletionCriteria deletionCriteria) {
            this.deletionCriteria = deletionCriteria;
            return this;
        }

        public Builder backupStrategy(String backupStrategy) {
            this.backupStrategy = backupStrategy;
            return this;
        }

        public Builder backupLocation(String backupLocation) {
            this.backupLocation = backupLocation;
            return this;
        }

        public Builder backupRetentionDays(int backupRetentionDays) {
            this.backupRetentionDays = backupRetentionDays;
            return this;
        }

        public Builder validationEnabled(boolean validationEnabled) {
            this.validationEnabled = validationEnabled;
            return this;
        }

        public Builder validationSampleSize(int validationSampleSize) {
            this.validationSampleSize = validationSampleSize;
            return this;
        }

        public Builder validationTolerancePercent(double validationTolerancePercent) {
            this.validationTolerancePercent = validationTolerancePercent;
            return this;
        }

        public Builder autoRecoveryEnabled(boolean autoRecoveryEnabled) {
            this.autoRecoveryEnabled = autoRecoveryEnabled;
            return this;
        }

        public Builder maxRecoveryRetries(int maxRecoveryRetries) {
            this.maxRecoveryRetries = maxRecoveryRetries;
            return this;
        }

        public Builder partitionParallelism(int partitionParallelism) {
            this.partitionParallelism = partitionParallelism;
            return this;
        }

        public Builder batchSize(long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder dryRun(boolean dryRun) {
            this.dryRun = dryRun;
            return this;
        }

        public Builder fromProperties(Properties props) {
            if (props.containsKey("backup.strategy")) {
                this.backupStrategy = props.getProperty("backup.strategy");
            }
            if (props.containsKey("backup.retention.days")) {
                this.backupRetentionDays = Integer.parseInt(props.getProperty("backup.retention.days"));
            }
            if (props.containsKey("validation.enabled")) {
                this.validationEnabled = Boolean.parseBoolean(props.getProperty("validation.enabled"));
            }
            if (props.containsKey("validation.sample.size")) {
                this.validationSampleSize = Integer.parseInt(props.getProperty("validation.sample.size"));
            }
            if (props.containsKey("validation.tolerance.percent")) {
                this.validationTolerancePercent = Double.parseDouble(props.getProperty("validation.tolerance.percent"));
            }
            if (props.containsKey("recovery.auto.enabled")) {
                this.autoRecoveryEnabled = Boolean.parseBoolean(props.getProperty("recovery.auto.enabled"));
            }
            if (props.containsKey("recovery.max.retries")) {
                this.maxRecoveryRetries = Integer.parseInt(props.getProperty("recovery.max.retries"));
            }
            if (props.containsKey("partition.parallelism")) {
                this.partitionParallelism = Integer.parseInt(props.getProperty("partition.parallelism"));
            }
            if (props.containsKey("batch.size")) {
                this.batchSize = Long.parseLong(props.getProperty("batch.size"));
            }
            if (props.containsKey("dry.run.enabled")) {
                this.dryRun = Boolean.parseBoolean(props.getProperty("dry.run.enabled"));
            }
            return this;
        }

        public JobConfig build() {
            JobConfig config = new JobConfig(this);
            config.validate();
            return config;
        }
    }
}
