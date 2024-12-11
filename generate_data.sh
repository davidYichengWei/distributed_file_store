#!/bin/bash

# Default parameters
DEFAULT_FILE_SIZE=$((1024 * 100)) # 100 KB
DEFAULT_CHUNK_SIZE=$((1024 * 10)) # 10 KB
DEFAULT_OUTPUT_FILE="generated_input"

# Input parameters or defaults
FILE_SIZE=${1:-$DEFAULT_FILE_SIZE}         # Total file size in bytes
CHUNK_SIZE=${2:-$DEFAULT_CHUNK_SIZE}       # Size of each chunk in bytes
OUTPUT_FILE=${3:-$DEFAULT_OUTPUT_FILE}     # Name of the output file

# Ensure valid input
if (( FILE_SIZE <= 0 || CHUNK_SIZE <= 0 )); then
    echo "File size and chunk size must be positive integers."
    exit 1
fi

# Generate the file
> "$OUTPUT_FILE" # Clear the file
NUM_CHUNKS=$(( (FILE_SIZE + CHUNK_SIZE - 1) / CHUNK_SIZE )) # Calculate the number of chunks

for (( i=0; i<NUM_CHUNKS; i++ )); do
    # Generate chunk content
    CHUNK_CONTENT="chunk_$i_"
    PADDING_SIZE=$(( CHUNK_SIZE - ${#CHUNK_CONTENT} ))
    if (( PADDING_SIZE < 0 )); then
        echo "Error: Chunk size too small for chunk header content."
        exit 1
    fi

    # Add padding
    PADDING=$(head -c "$PADDING_SIZE" < /dev/zero | tr '\0' 'X')

    # Write the chunk to the file
    echo -n "${CHUNK_CONTENT}${PADDING}" >> "$OUTPUT_FILE"
done

echo "File generated: $OUTPUT_FILE (Size: $(stat --printf="%s" "$OUTPUT_FILE") bytes, Chunks: $NUM_CHUNKS)"
