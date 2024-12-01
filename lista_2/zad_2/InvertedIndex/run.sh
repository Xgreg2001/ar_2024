#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Variables
USER=$(whoami)
INPUT_DIR="/user/$USER/input"
OUTPUT_DIR="/user/$USER/output_invertedindex"
LOCAL_INPUT_DIR="input_data"
JAR_NAME="invertedindex.jar"
CLASS_FILES_DIR="classes"

# Clean previous HDFS output directory
echo "Cleaning up HDFS output directory..."
hdfs dfs -rm -r -skipTrash $OUTPUT_DIR || true

# Clean previous local class files and JAR
echo "Cleaning up previous builds..."
rm -rf $CLASS_FILES_DIR $JAR_NAME

# Compile the Java code
echo "Compiling Java code..."
mkdir -p $CLASS_FILES_DIR
javac -classpath $(hadoop classpath) -d $CLASS_FILES_DIR InvertedIndexMapper.java InvertedIndexReducer.java InvertedIndex.java

# Create JAR file
echo "Creating JAR file..."
jar -cvf $JAR_NAME -C $CLASS_FILES_DIR/ .

# Clean previous HDFS input directory
echo "Cleaning up HDFS input directory..."
hdfs dfs -rm -r -skipTrash $INPUT_DIR || true

# Copy input data to HDFS
echo "Copying input data to HDFS..."
hdfs dfs -mkdir -p $INPUT_DIR
hdfs dfs -put $LOCAL_INPUT_DIR/* $INPUT_DIR/

# Run the MapReduce job
echo "Running the MapReduce job..."
hadoop jar $JAR_NAME InvertedIndex $INPUT_DIR $OUTPUT_DIR

# Display the results
echo "Inverted Index Results:"
hdfs dfs -cat $OUTPUT_DIR/part-r-00000

