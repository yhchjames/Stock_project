#!/bin/bash

# Define output directory and file with timestamp
OUTPUT_DIR="sh_logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_FILE="$OUTPUT_DIR/output_$TIMESTAMP.log"

# Create the output directory if it doesn't exist
mkdir -p $OUTPUT_DIR

# Clear previous log content if it exists
> $OUTPUT_FILE

# Activate Conda environment
source ~/miniconda3/bin/activate pyenv

# List of Python files to execute
python_files=(
    "trading_date.py"
    "download_stock.py"
    "daily_asyc_brokerdata.py"
    "split_brokerdata.py"
    "broker_analyze.py"
)

# Run each Python file and append stdout to the log file
for file in "${python_files[@]}"; do
    echo "Running $file..." >> $OUTPUT_FILE
    python3 "$file" >> $OUTPUT_FILE 2>&1
    if [ $? -ne 0 ]; then
        echo "Error encountered while running $file. Check the log for details." >> $OUTPUT_FILE
        break
    fi
    echo "Finished $file." >> $OUTPUT_FILE
    echo "--------------------------" >> $OUTPUT_FILE
done

# Deactivate Conda environment
conda deactivate

# Indicate completion
echo "All scripts executed. Check $OUTPUT_FILE for details."
