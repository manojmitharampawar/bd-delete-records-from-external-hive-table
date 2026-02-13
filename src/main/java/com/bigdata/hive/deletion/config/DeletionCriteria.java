package com.bigdata.hive.deletion.config;

import org.apache.commons.lang3.StringUtils;
import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Encapsulates deletion criteria including WHERE clause conditions and time
 * window.
 */
public class DeletionCriteria implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String whereClause;
    private final Timestamp startTime;
    private final Timestamp endTime;
    private final String timeColumn;

    public DeletionCriteria(String whereClause, Timestamp startTime, Timestamp endTime, String timeColumn) {
        this.whereClause = whereClause;
        this.startTime = startTime;
        this.endTime = endTime;
        this.timeColumn = timeColumn != null ? timeColumn : "row_create_ts";
    }

    public DeletionCriteria(String whereClause, Timestamp startTime, Timestamp endTime) {
        this(whereClause, startTime, endTime, "row_create_ts");
    }

    public String getWhereClause() {
        return whereClause;
    }

    public Timestamp getStartTime() {
        return startTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    /**
     * Builds the complete WHERE clause including time window.
     * 
     * @return Complete WHERE clause for deletion
     */
    public String getCompleteWhereClause() {
        StringBuilder sb = new StringBuilder();

        // Add time window conditions
        if (startTime != null) {
            sb.append(timeColumn).append(" >= '").append(startTime).append("'");
        }

        if (endTime != null) {
            if (sb.length() > 0) {
                sb.append(" AND ");
            }
            sb.append(timeColumn).append(" < '").append(endTime).append("'");
        }

        // Add custom WHERE clause
        if (StringUtils.isNotBlank(whereClause)) {
            if (sb.length() > 0) {
                sb.append(" AND ");
            }
            sb.append("(").append(whereClause).append(")");
        }

        return sb.toString();
    }

    /**
     * Builds the inverse WHERE clause for retention (records to keep).
     * 
     * @return WHERE clause for records that should be retained
     */
    public String getRetentionWhereClause() {
        String completeClause = getCompleteWhereClause();
        if (StringUtils.isBlank(completeClause)) {
            return null;
        }
        return "NOT (" + completeClause + ")";
    }

    /**
     * Validates the deletion criteria.
     * 
     * @throws IllegalArgumentException if criteria is invalid
     */
    public void validate() {
        if (startTime == null && endTime == null && StringUtils.isBlank(whereClause)) {
            throw new IllegalArgumentException("Deletion criteria must have at least one condition");
        }

        if (startTime != null && endTime != null && startTime.after(endTime)) {
            throw new IllegalArgumentException("Start time must be before end time");
        }

        if (StringUtils.isBlank(timeColumn)) {
            throw new IllegalArgumentException("Time column cannot be empty");
        }

        // Basic SQL injection prevention
        if (whereClause != null && containsSuspiciousPatterns(whereClause)) {
            throw new IllegalArgumentException("WHERE clause contains potentially unsafe patterns");
        }
    }

    /**
     * Basic check for suspicious SQL patterns.
     */
    private boolean containsSuspiciousPatterns(String clause) {
        String lowerClause = clause.toLowerCase();
        // Check for common SQL injection patterns
        String[] suspiciousPatterns = {
                ";", "--", "/*", "*/", "xp_", "sp_", "exec ", "execute ",
                "drop ", "truncate ", "alter ", "create ", "insert "
        };

        for (String pattern : suspiciousPatterns) {
            if (lowerClause.contains(pattern)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "DeletionCriteria{" +
                "whereClause='" + whereClause + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", timeColumn='" + timeColumn + '\'' +
                '}';
    }

    /**
     * Builder for DeletionCriteria
     */
    public static class Builder {
        private String whereClause;
        private Timestamp startTime;
        private Timestamp endTime;
        private String timeColumn = "row_create_ts";

        public Builder whereClause(String whereClause) {
            this.whereClause = whereClause;
            return this;
        }

        public Builder startTime(Timestamp startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder startTime(String startTimeStr) {
            this.startTime = Timestamp.valueOf(startTimeStr);
            return this;
        }

        public Builder endTime(Timestamp endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder endTime(String endTimeStr) {
            this.endTime = Timestamp.valueOf(endTimeStr);
            return this;
        }

        public Builder timeColumn(String timeColumn) {
            this.timeColumn = timeColumn;
            return this;
        }

        public DeletionCriteria build() {
            DeletionCriteria criteria = new DeletionCriteria(whereClause, startTime, endTime, timeColumn);
            criteria.validate();
            return criteria;
        }
    }
}
