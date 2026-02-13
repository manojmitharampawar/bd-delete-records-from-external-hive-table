package com.bigdata.hive.deletion.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for handling partition operations and parsing partition IDs.
 * Supports formats: yyyyMMdd, yyyyMMdd-1, history_yyyyMMdd-n
 */
public class PartitionUtils {
    private static final Logger logger = LoggerFactory.getLogger(PartitionUtils.class);

    // Regex patterns for partition ID formats
    private static final Pattern CURRENT_DATE_PATTERN = Pattern.compile("^(\\d{8})$"); // yyyyMMdd
    private static final Pattern PREVIOUS_DATE_PATTERN = Pattern.compile("^(\\d{8})-(\\d+)$"); // yyyyMMdd-n
    private static final Pattern HISTORY_PATTERN = Pattern.compile("^history_(\\d{8})(?:-(\\d+))?$"); // history_yyyyMMdd
                                                                                                      // or
                                                                                                      // history_yyyyMMdd-n

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

    /**
     * Parses a partition ID and extracts the base date.
     *
     * @param partitionId Partition ID to parse
     * @return Date object representing the partition date
     * @throws ParseException if partition ID format is invalid
     */
    public static Date parsePartitionDate(String partitionId) throws ParseException {
        if (StringUtils.isBlank(partitionId)) {
            throw new ParseException("Partition ID cannot be empty", 0);
        }

        Matcher currentMatcher = CURRENT_DATE_PATTERN.matcher(partitionId);
        if (currentMatcher.matches()) {
            return DATE_FORMAT.parse(currentMatcher.group(1));
        }

        Matcher previousMatcher = PREVIOUS_DATE_PATTERN.matcher(partitionId);
        if (previousMatcher.matches()) {
            return DATE_FORMAT.parse(previousMatcher.group(1));
        }

        Matcher historyMatcher = HISTORY_PATTERN.matcher(partitionId);
        if (historyMatcher.matches()) {
            return DATE_FORMAT.parse(historyMatcher.group(1));
        }

        throw new ParseException("Invalid partition ID format: " + partitionId, 0);
    }

    /**
     * Determines the partition type.
     *
     * @param partitionId Partition ID
     * @return Partition type (CURRENT, PREVIOUS, HISTORY)
     */
    public static PartitionType getPartitionType(String partitionId) {
        if (CURRENT_DATE_PATTERN.matcher(partitionId).matches()) {
            return PartitionType.CURRENT;
        } else if (PREVIOUS_DATE_PATTERN.matcher(partitionId).matches()) {
            return PartitionType.PREVIOUS;
        } else if (HISTORY_PATTERN.matcher(partitionId).matches()) {
            return PartitionType.HISTORY;
        }
        return PartitionType.UNKNOWN;
    }

    /**
     * Builds a partition filter predicate for Spark SQL.
     *
     * @param partitionColumn Partition column name
     * @param partitionIds    List of partition IDs to filter
     * @return SQL WHERE clause for partition filtering
     */
    public static String buildPartitionFilter(String partitionColumn, List<String> partitionIds) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            return "";
        }

        StringBuilder filter = new StringBuilder();
        filter.append(partitionColumn).append(" IN (");

        for (int i = 0; i < partitionIds.size(); i++) {
            if (i > 0) {
                filter.append(", ");
            }
            filter.append("'").append(partitionIds.get(i)).append("'");
        }

        filter.append(")");
        return filter.toString();
    }

    /**
     * Filters partitions based on a date range.
     *
     * @param partitionIds List of all partition IDs
     * @param startDate    Start date (inclusive)
     * @param endDate      End date (exclusive)
     * @return Filtered list of partition IDs within the date range
     */
    public static List<String> filterPartitionsByDateRange(List<String> partitionIds, Date startDate, Date endDate) {
        List<String> filtered = new ArrayList<>();

        for (String partitionId : partitionIds) {
            try {
                Date partitionDate = parsePartitionDate(partitionId);

                boolean inRange = true;
                if (startDate != null && partitionDate.before(startDate)) {
                    inRange = false;
                }
                if (endDate != null && !partitionDate.before(endDate)) {
                    inRange = false;
                }

                if (inRange) {
                    filtered.add(partitionId);
                }
            } catch (ParseException e) {
                logger.warn("Skipping partition with invalid format: {}", partitionId);
            }
        }

        return filtered;
    }

    /**
     * Validates partition ID format.
     *
     * @param partitionId Partition ID to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidPartitionId(String partitionId) {
        if (StringUtils.isBlank(partitionId)) {
            return false;
        }

        return CURRENT_DATE_PATTERN.matcher(partitionId).matches() ||
                PREVIOUS_DATE_PATTERN.matcher(partitionId).matches() ||
                HISTORY_PATTERN.matcher(partitionId).matches();
    }

    /**
     * Generates a partition specification for Hive operations.
     *
     * @param partitionColumn Partition column name
     * @param partitionValue  Partition value
     * @return Partition specification string (e.g., "partition_id='20260213'")
     */
    public static String buildPartitionSpec(String partitionColumn, String partitionValue) {
        return partitionColumn + "='" + partitionValue + "'";
    }

    /**
     * Sorts partition IDs chronologically.
     *
     * @param partitionIds List of partition IDs
     * @return Sorted list of partition IDs
     */
    public static List<String> sortPartitionsChronologically(List<String> partitionIds) {
        List<String> sorted = new ArrayList<>(partitionIds);

        sorted.sort((p1, p2) -> {
            try {
                Date d1 = parsePartitionDate(p1);
                Date d2 = parsePartitionDate(p2);
                return d1.compareTo(d2);
            } catch (ParseException e) {
                logger.warn("Error comparing partitions: {} and {}", p1, p2);
                return p1.compareTo(p2);
            }
        });

        return sorted;
    }

    /**
     * Enum for partition types.
     */
    public enum PartitionType {
        CURRENT, // yyyyMMdd
        PREVIOUS, // yyyyMMdd-n
        HISTORY, // history_yyyyMMdd or history_yyyyMMdd-n
        UNKNOWN
    }
}
