#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Variables
USER=$(whoami)
INPUT_DIR="/user/$USER/weather_input"
OUTPUT_DIR="/user/$USER/weather_output"
LOCAL_INPUT_DIR="input_data"
JAR_NAME="weatheranalysis.jar"
CLASS_FILES_DIR="classes"
YEAR="2022" # Możesz zmienić na wybrany rok lub pozostawić pusty, aby przetwarzać wszystkie lata

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
javac -classpath $(hadoop classpath) -d $CLASS_FILES_DIR WeatherMapper.java WeatherReducer.java WeatherAnalysis.java

# Create JAR file
echo "Creating JAR file..."
jar -cvf $JAR_NAME -C $CLASS_FILES_DIR/ .

# Prepare input data
echo "Preparing input data..."
mkdir -p $LOCAL_INPUT_DIR

# Create weather_data.csv with additional columns (e.g., precipitation, humidity)
cat > $LOCAL_INPUT_DIR/weather_data.csv << EOF
2022-01-01,5.0,0.0,80
2022-01-02,3.2,1.2,85
2022-01-03,6.1,0.0,78
2022-02-01,7.5,0.5,75
2022-02-02,8.0,0.0,70
2022-02-03,6.8,0.8,72
2022-03-01,10.0,0.0,65
2022-03-02,12.5,0.0,60
2022-03-03,11.2,0.2,63
EOF

# Copy input data to HDFS
echo "Copying input data to HDFS..."
hdfs dfs -mkdir -p $INPUT_DIR
hdfs dfs -put $LOCAL_INPUT_DIR/weather_data.csv $INPUT_DIR/

# Run the MapReduce job
echo "Running the MapReduce job..."
if [ -z "$YEAR" ]; then
    # Jeśli YEAR jest pusty, nie filtruj danych według roku
    hadoop jar $JAR_NAME WeatherAnalysis $INPUT_DIR/ $OUTPUT_DIR
else
    hadoop jar $JAR_NAME WeatherAnalysis $INPUT_DIR/ $OUTPUT_DIR $YEAR
fi

# Display the results
echo "Weather Analysis Results:"
hdfs dfs -cat $OUTPUT_DIR/part-r-00000

