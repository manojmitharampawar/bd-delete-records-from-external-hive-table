package com.bigdata.hive.deletion.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Collects and tracks metrics during the deletion job execution.
 */
public class MetricsCollector implements Serializable {
    private static final long serialVersionUID = 1L;

    private long totalRecordsRead = 0;
    private long totalRecordsDeleted = 0;
    private long totalRecordsRetained = 0;
    private long startTime = 0;
    private long endTime = 0;
    private int partitionsProcessed = 0;
    private int partitionsFailed = 0;
    private Map<String, Long> partitionMetrics = new HashMap<>();
    private boolean backupCreated = false;
    private boolean validationPassed = false;
    private String backupLocation;

    public MetricsCollector() {
        this.startTime = System.currentTimeMillis();
    }

    public void recordRecordsRead(long count) {
        this.totalRecordsRead += count;
    }

    public void recordRecordsDeleted(long count) {
        this.totalRecordsDeleted += count;
    }

    public void recordRecordsRetained(long count) {
        this.totalRecordsRetained += count;
    }

    public void incrementPartitionsProcessed() {
        this.partitionsProcessed++;
    }

    public void incrementPartitionsFailed() {
        this.partitionsFailed++;
    }

    public void recordPartitionMetric(String partitionId, long recordCount) {
        this.partitionMetrics.put(partitionId, recordCount);
    }

    public void markBackupCreated(String location) {
        this.backupCreated = true;
        this.backupLocation = location;
    }

    public void markValidationPassed(boolean passed) {
        this.validationPassed = passed;
    }

    public void markJobEnd() {
        this.endTime = System.currentTimeMillis();
    }

    // Getters
    public long getTotalRecordsRead() {
        return totalRecordsRead;
    }

    public long getTotalRecordsDeleted() {
        return totalRecordsDeleted;
    }

    public long getTotalRecordsRetained() {
        return totalRecordsRetained;
    }

    public long getExecutionTimeMs() {
        return endTime - startTime;
    }

    public int getPartitionsProcessed() {
        return partitionsProcessed;
    }

    public int getPartitionsFailed() {
        return partitionsFailed;
    }

    public Map<String, Long> getPartitionMetrics() {
        return partitionMetrics;
    }

    public boolean isBackupCreated() {
        return backupCreated;
    }

    public boolean isValidationPassed() {
        return validationPassed;
    }

    public String getBackupLocation() {
        return backupLocation;
    }

    /**
     * Generates a summary report of all metrics.
     */
    public String generateReport() {
        StringBuilder report = new StringBuilder();
        report.append("\n========== Deletion Job Metrics ==========\n");
        report.append(String.format("Execution Time: %.2f seconds\n", getExecutionTimeMs() / 1000.0));
        report.append(String.format("Total Records Read: %,d\n", totalRecordsRead));
        report.append(String.format("Total Records Deleted: %,d\n", totalRecordsDeleted));
        report.append(String.format("Total Records Retained: %,d\n", totalRecordsRetained));
        report.append(String.format("Partitions Processed: %d\n", partitionsProcessed));
        report.append(String.format("Partitions Failed: %d\n", partitionsFailed));
        report.append(String.format("Backup Created: %s\n", backupCreated ? "Yes" : "No"));
        if (backupCreated) {
            report.append(String.format("Backup Location: %s\n", backupLocation));
        }
        report.append(String.format("Validation Passed: %s\n", validationPassed ? "Yes" : "No"));

        if (!partitionMetrics.isEmpty()) {
            report.append("\nPartition-Level Metrics:\n");
            partitionMetrics.forEach(
                    (partition, count) -> report.append(String.format("  %s: %,d records\n", partition, count)));
        }

        report.append("==========================================\n");
        return report.toString();
    }

    @Override
    public String toString() {
        return generateReport();
    }
}
