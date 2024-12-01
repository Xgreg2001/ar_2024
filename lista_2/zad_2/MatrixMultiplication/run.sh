#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Variables
USER=$(whoami)
INPUT_DIR="/user/$USER/matrix_multiplication_input"
OUTPUT_DIR="/user/$USER/matrix_multiplication_output"
LOCAL_INPUT_DIR="input_data"
JAR_NAME="matrixmultiplication.jar"
CLASS_FILES_DIR="classes"

# Clean previous HDFS directories
echo "Cleaning up HDFS directories..."
hdfs dfs -rm -r -skipTrash $INPUT_DIR || true
hdfs dfs -rm -r -skipTrash $OUTPUT_DIR || true

# Clean previous local class files and JAR
echo "Cleaning up previous builds..."
rm -rf $CLASS_FILES_DIR $JAR_NAME

# Compile the Java code
echo "Compiling Java code..."
mkdir -p $CLASS_FILES_DIR
javac -classpath $(hadoop classpath) -d $CLASS_FILES_DIR MatrixMultiplicationMapper.java MatrixMultiplicationReducer.java MatrixMultiplication.java

# Create JAR file
echo "Creating JAR file..."
jar -cvf $JAR_NAME -C $CLASS_FILES_DIR/ .

# Prepare input data
echo "Preparing input data..."
mkdir -p $LOCAL_INPUT_DIR

# Create matrix.txt with both matrices A and B
cat > $LOCAL_INPUT_DIR/matrix.txt << EOF
A 0 0 1
A 0 1 2
A 0 2 0
A 1 0 0
A 1 1 3
A 1 2 4
B 0 0 5
B 0 1 0
B 1 0 6
B 1 1 7
B 2 0 0
B 2 1 8
EOF

# Copy input data to HDFS
echo "Copying input data to HDFS..."
hdfs dfs -mkdir -p $INPUT_DIR
hdfs dfs -put $LOCAL_INPUT_DIR/matrix.txt $INPUT_DIR/

# Run the MapReduce job
echo "Running the MapReduce job..."
# n = shared dimension (number of columns in A or number of rows in B)
# m = number of rows in A
# p = number of columns in B
hadoop jar $JAR_NAME MatrixMultiplication $INPUT_DIR/ $OUTPUT_DIR 3 2 2

# Display the results
echo "Matrix Multiplication Results:"
hdfs dfs -cat $OUTPUT_DIR/part-r-00000

