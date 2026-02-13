#!/bin/bash

# Build script for Hive Table Deletion Job
# Requires: Maven 3.6+, Java 1.8+

set -e

echo "========================================="
echo "Building Hive Table Deletion Job"
echo "========================================="

# Check Maven installation
if ! command -v mvn &> /dev/null; then
    echo "ERROR: Maven is not installed"
    echo "Please install Maven 3.6+ to build this project"
    echo ""
    echo "Installation instructions:"
    echo "  macOS: brew install maven"
    echo "  Linux: sudo apt-get install maven"
    echo ""
    exit 1
fi

# Check Java version
if ! command -v java &> /dev/null; then
    echo "ERROR: Java is not installed"
    echo "Please install Java 1.8+ to build this project"
    exit 1
fi

echo "Maven version:"
mvn --version

echo ""
echo "Starting build..."
echo ""

# Clean and compile
mvn clean compile

echo ""
echo "Running tests..."
echo ""

# Run tests (skip if no tests yet)
# mvn test

echo ""
echo "Creating package..."
echo ""

# Package
mvn package -DskipTests

echo ""
echo "========================================="
echo "Build completed successfully!"
echo "========================================="
echo ""
echo "Output JAR: target/hive-table-deletion-1.0.0.jar"
echo ""
echo "To run the job:"
echo "  spark-submit \\"
echo "    --class com.bigdata.hive.deletion.HiveTableDeletionJob \\"
echo "    target/hive-table-deletion-1.0.0.jar \\"
echo "    --database <database> \\"
echo "    --table <table> \\"
echo "    --where \"<conditions>\" \\"
echo "    --start-time \"yyyy-MM-dd HH:mm:ss\" \\"
echo "    --end-time \"yyyy-MM-dd HH:mm:ss\""
echo ""
