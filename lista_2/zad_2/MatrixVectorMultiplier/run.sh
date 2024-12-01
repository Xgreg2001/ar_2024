#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Variables
USER=$(whoami)
INPUT_MATRIX_DIR="/user/$USER/matrix_input"
INPUT_VECTOR_DIR="/user/$USER/vector_input"
OUTPUT_DIR="/user/$USER/output_matrix_vector"
LOCAL_INPUT_DIR="input_data"
JAR_NAME="matrixvector.jar"
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
javac -classpath $(hadoop classpath) -d $CLASS_FILES_DIR MatrixMapper.java ProductReducer.java MatrixVectorMultiplier.java

# Create JAR file
echo "Creating JAR file..."
jar -cvf $JAR_NAME -C $CLASS_FILES_DIR/ .

# Prepare input data
echo "Preparing input data..."
mkdir -p $LOCAL_INPUT_DIR

# Create matrix.txt
cat > $LOCAL_INPUT_DIR/matrix.txt << EOF
0 0 1
0 1 2
1 1 3
1 2 4
2 0 5
2 2 6
EOF

# Create vector.txt
cat > $LOCAL_INPUT_DIR/vector.txt << EOF
0 7
1 8
2 9
EOF

# Clean previous HDFS input directories
echo "Cleaning up HDFS input directories..."
hdfs dfs -rm -r -skipTrash $INPUT_MATRIX_DIR || true
hdfs dfs -rm -r -skipTrash $INPUT_VECTOR_DIR || true

# Copy input data to HDFS
echo "Copying input data to HDFS..."
hdfs dfs -mkdir -p $INPUT_MATRIX_DIR
hdfs dfs -mkdir -p $INPUT_VECTOR_DIR
hdfs dfs -put $LOCAL_INPUT_DIR/matrix.txt $INPUT_MATRIX_DIR/
hdfs dfs -put $LOCAL_INPUT_DIR/vector.txt $INPUT_VECTOR_DIR/

# Run the MapReduce job
echo "Running the MapReduce job..."
hadoop jar $JAR_NAME MatrixVectorMultiplier $INPUT_MATRIX_DIR/ $INPUT_VECTOR_DIR/vector.txt $OUTPUT_DIR

# Display the results
echo "Matrix-Vector Multiplication Results:"
hdfs dfs -cat $OUTPUT_DIR/part-r-00000

