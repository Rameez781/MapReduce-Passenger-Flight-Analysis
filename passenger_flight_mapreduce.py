#!/usr/bin/env python3
"""
MapReduce Implementation to determine passenger(s) with highest number of flights.
Version 1.0 - Complete Implementation with all features
"""
import csv
import threading
import argparse
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple, Any, DefaultDict

class MapReduceFramework:
    """
    A custom MapReduce framework implementation for processing passenger flight data.
    This emulates core MapReduce concepts without requiring a Hadoop cluster.
    """
    
    def __init__(self, num_mappers: int = 4, num_reducers: int = 2):
        """
        Initialize the MapReduce framework.
        
        Args:
            num_mappers: Number of mapper threads
            num_reducers: Number of reducer threads
        """
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.intermediate_data = defaultdict(list)
        self.intermediate_lock = threading.Lock()
        self.final_results = []
        self.final_results_lock = threading.Lock()
        
    def read_csv_data(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Read CSV data into a list of dictionaries.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            List of dictionaries containing the CSV data
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        data = []
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            # Determine column names based on the input file
            # AComp_Passenger_data_no_error.csv has 6 columns
            if 'no_error_DateTime' in file_path:
                # This version has datetime columns
                headers = ['passenger_id', 'flight_id', 'source', 'destination', 
                          'departure_time_unix', 'departure_time_str', 'flight_time', 'arrival_time_str']
            else:
                # Standard version has 6 columns
                headers = ['passenger_id', 'flight_id', 'source', 'destination', 
                          'departure_time_unix', 'flight_time']
            
            for row in reader:
                # Create a dictionary with appropriate headers and values
                if len(row) == len(headers):
                    data.append(dict(zip(headers, row)))
        
        return data
        
    def split_data(self, data: List[Dict[str, Any]], num_splits: int) -> List[List[Dict[str, Any]]]:
        """
        Split the data into chunks for parallel processing.
        
        Args:
            data: Input data to split
            num_splits: Number of splits to create
            
        Returns:
            List of data chunks
        """
        chunk_size = max(1, len(data) // num_splits)
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
    def map_function(self, record: Dict[str, Any]) -> Tuple[str, int]:
        """
        Map function that extracts the passenger ID as key and emits 1 as value.
        
        Args:
            record: A single passenger flight record
            
        Returns:
            Tuple of (passenger_id, 1)
        """
        passenger_id = record['passenger_id']
        return (passenger_id, 1)
        
    def map_phase(self, data_chunk: List[Dict[str, Any]]) -> None:
        """
        Process a chunk of data through the map phase.
        
        Args:
            data_chunk: A subset of the input data to process
        """
        local_results = defaultdict(list)
        
        # Map each record
        for record in data_chunk:
            key, value = self.map_function(record)
            local_results[key].append(value)
            
        # Update the shared intermediate data structure with proper synchronization
        with self.intermediate_lock:
            for key, values in local_results.items():
                self.intermediate_data[key].extend(values)
    
    def shuffle_and_sort(self) -> Dict[str, List[int]]:
        """
        Shuffle and sort the intermediate data.
        
        Returns:
            Sorted intermediate data ready for the reduce phase
        """
        # In a real implementation, this would distribute data across reducers
        # Here we just return the intermediate data as it's already grouped by key
        return dict(self.intermediate_data)
        
    def reduce_function(self, key: str, values: List[int]) -> Tuple[str, int]:
        """
        Reduce function that counts the number of flights for a passenger.
        
        Args:
            key: Passenger ID
            values: List of 1's, one for each flight
            
        Returns:
            Tuple of (passenger_id, flight_count)
        """
        return (key, sum(values))
        
    def reduce_phase(self, key_values: List[Tuple[str, List[int]]]) -> None:
        """
        Process a chunk of key-values through the reduce phase.
        
        Args:
            key_values: List of (key, values) pairs to process
        """
        local_results = []
        
        for key, values in key_values:
            reduced_key, reduced_value = self.reduce_function(key, values)
            local_results.append((reduced_key, reduced_value))
            
        # Update the final results with proper synchronization
        with self.final_results_lock:
            self.final_results.extend(local_results)
    
    def partition_for_reducers(self, data: Dict[str, List[int]], num_partitions: int) -> List[List[Tuple[str, List[int]]]]:
        """
        Partition the data for parallel reducing.
        
        Args:
            data: Data to partition
            num_partitions: Number of partitions to create
            
        Returns:
            List of partitions containing key-value pairs
        """
        items = list(data.items())
        chunk_size = max(1, len(items) // num_partitions)
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
            
    def execute(self, input_file: str) -> List[Tuple[str, int]]:
        """
        Execute the MapReduce job on the input file.
        
        Args:
            input_file: Path to the input CSV file
            
        Returns:
            List of (passenger_id, flight_count) tuples
        """
        # Read and parse the input data
        data = self.read_csv_data(input_file)
        print(f"Loaded {len(data)} flight records from {input_file}")
        
        # Split the data for the map phase
        data_chunks = self.split_data(data, self.num_mappers)
        print(f"Split data into {len(data_chunks)} chunks for mapping")
        
        # Execute the map phase in parallel
        with ThreadPoolExecutor(max_workers=self.num_mappers) as executor:
            list(executor.map(self.map_phase, data_chunks))
            
        print(f"Map phase complete, found {len(self.intermediate_data)} unique passengers")
        
        # Shuffle and sort the intermediate data
        grouped_data = self.shuffle_and_sort()
        
        # Partition the data for the reduce phase
        reduce_partitions = self.partition_for_reducers(grouped_data, self.num_reducers)
        print(f"Created {len(reduce_partitions)} partitions for reducing")
        
        # Execute the reduce phase in parallel
        with ThreadPoolExecutor(max_workers=self.num_reducers) as executor:
            list(executor.map(self.reduce_phase, reduce_partitions))
            
        print(f"Reduce phase complete, processed {len(self.final_results)} results")
        
        return self.final_results
        
    def find_passengers_with_max_flights(self) -> List[Tuple[str, int]]:
        """
        Find the passenger(s) with the maximum number of flights.
        
        Returns:
            List of (passenger_id, flight_count) tuples for passengers with the most flights
        """
        if not self.final_results:
            return []
            
        # Find the maximum flight count
        max_flights = max(count for _, count in self.final_results)
        
        # Return all passengers with that flight count
        return [(passenger_id, count) for passenger_id, count in self.final_results if count == max_flights]

def save_results_to_csv(results: List[Tuple[str, int]], output_file: str) -> None:
    """
    Save the results to a CSV file.
    
    Args:
        results: List of (passenger_id, flight_count) tuples
        output_file: Path to the output CSV file
    """
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Passenger ID', 'Number of Flights'])
        for passenger_id, count in results:
            writer.writerow([passenger_id, count])

def main():
    """Main function to run the MapReduce job."""
    parser = argparse.ArgumentParser(description='MapReduce for finding passengers with most flights')
    parser.add_argument('--input', default='AComp_Passenger_data_no_error.csv',
                        help='Input CSV file containing flight data')
    parser.add_argument('--output', default='passenger_flight_counts.csv',
                        help='Output CSV file for the results')
    parser.add_argument('--mappers', type=int, default=4,
                        help='Number of mapper threads')
    parser.add_argument('--reducers', type=int, default=2,
                        help='Number of reducer threads')
    args = parser.parse_args()

    # Create and execute the MapReduce framework
    mr = MapReduceFramework(num_mappers=args.mappers, num_reducers=args.reducers)
    mr.execute(args.input)
    
    # Find the passenger(s) with the most flights
    top_passengers = mr.find_passengers_with_max_flights()
    
    # Save all flight counts to a CSV file
    print(f"Saving all passenger flight counts to {args.output}")
    save_results_to_csv(sorted(mr.final_results, key=lambda x: x[1], reverse=True), args.output)
    
    # Print the passenger(s) with the most flights
    print("\nPassenger(s) with the most flights:")
    for passenger_id, count in top_passengers:
        print(f"Passenger ID: {passenger_id}, Flights: {count}")

if __name__ == "__main__":
    main()