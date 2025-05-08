# MapReduce Passenger Flight Analysis

A MapReduce implementation to analyze passenger flight data and determine passenger(s) with the highest number of flights.

## Project Overview

This project implements a custom MapReduce framework in Python to process flight data records. The framework emulates the MapReduce programming model without requiring a Hadoop cluster. The main objective is to identify which passenger(s) have taken the most flights based on the provided dataset.

## Features

- Custom MapReduce framework with parallel processing capabilities
- Multi-threaded implementation using Python's threading and concurrent.futures
- Flexible configuration for number of mappers and reducers
- Data partitioning for distributed processing
- CSV data parsing and processing
- Results visualization and export

## Implementation Details

### Architecture

The MapReduce framework consists of the following components:

1. **Data Loading**: Reads CSV flight records data into memory
2. **Data Split**: Divides data into chunks for parallel processing
3. **Map Phase**: Processes each record to extract key-value pairs (passenger_id, 1)
4. **Shuffle and Sort**: Groups values by key (passenger_id)
5. **Reduce Phase**: Aggregates values for each key to determine total flights per passenger
6. **Result Processing**: Identifies passenger(s) with the maximum number of flights

### Technical Specifications

- **Programming Language**: Python 3
- **Concurrency**: Multi-threading with ThreadPoolExecutor
- **Data Format**: CSV processing
- **Dependencies**: Standard Python libraries only (no external dependencies)

## Files in the Repository

- `Task_A_ver2.py`: Main MapReduce framework implementation
- `mapreduce_test.py`: Test script with sample data generation and testing
- `mapreduce_unittest.py`: Unit tests for the MapReduce framework
- `AComp_Passenger_data_no_error.csv`: Flight data for analysis (not included in repository)

## Usage

### Basic Usage

```bash
python Task_A_ver2.py
```

This will run the MapReduce job on the default input file (`AComp_Passenger_data_no_error.csv`) and output the results to `passenger_flight_counts.csv`.

### Custom Configuration

```bash
python Task_A_ver2.py --input custom_data.csv --output custom_results.csv --mappers 8 --reducers 4
```

### Testing

Run the test script to create a sample dataset and test the MapReduce framework:

```bash
python mapreduce_test.py
```

Run unit tests:

```bash
python mapreduce_unittest.py
```

## Data Format

The input data should be in CSV format with the following columns:
- passenger_id: Format XXXnnnnnXn (X is uppercase ASCII, n is digit 0-9)
- flight_id: Format XXXnnnnX
- source: IATA/FAA airport code (Format: XXX)
- destination: IATA/FAA airport code (Format: XXX)
- departure_time_unix: Unix epoch time
- flight_time: Flight duration in minutes

## Results

The framework outputs:
1. A CSV file containing all passengers and their flight counts
2. Identifies and displays the passenger(s) with the maximum number of flights

## Performance

- The framework is designed to take advantage of multi-threading for improved performance
- Scales with the number of available CPU cores
- Optimized for processing large datasets efficiently

## Future Improvements

- Add support for distributed processing across multiple machines
- Implement fault tolerance mechanisms
- Add more analytics capabilities for flight data

## Assignment Context

This project was developed as part of the Cloud Computing and Big Data module coursework, specifically for Task A which focuses on implementing a MapReduce solution to determine the passenger(s) with the highest number of flights.
