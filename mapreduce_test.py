#!/usr/bin/env python3
"""
Test script for the MapReduce passenger flight analysis.
This script creates a small sample dataset and demonstrates the MapReduce functionality.
"""

import csv
import os
from passenger_flight_mapreduce import MapReduceFramework, save_results_to_csv

def create_sample_data(filename, num_records=30):
    """
    Create a sample dataset for testing.
    
    Args:
        filename: Name of the file to create
        num_records: Number of records to generate
    """
    # Sample passenger IDs
    passenger_ids = ["ABC1234DE5", "FGH5678IJ9", "KLM9012NO3", "PQR3456ST7", "UVW7890XY1"]
    
    # Sample flight IDs
    flight_ids = ["ABC1234X", "DEF5678Y", "GHI9012Z", "JKL3456A", "MNO7890B"]
    
    # Sample airports
    airports = ["DEN", "FRA", "LHR", "JFK", "LAX", "SFO", "ORD"]
    
    # Create sample data
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Generate records
        for i in range(num_records):
            # Create an uneven distribution of flights per passenger
            passenger_idx = i % len(passenger_ids)
            # Make one passenger have more flights
            if i > num_records // 2:
                passenger_idx = 0
                
            flight_idx = i % len(flight_ids)
            source_idx = i % len(airports)
            dest_idx = (i + 1) % len(airports)
            
            # Create a record
            writer.writerow([
                passenger_ids[passenger_idx],
                flight_ids[flight_idx],
                airports[source_idx],
                airports[dest_idx],
                1420564460 + i * 3600,  # Unix timestamp
                60 + i % 120  # Flight duration
            ])
    
    print(f"Created sample data file: {filename}")
    return filename

def run_test():
    """
    Run a test of the MapReduce framework using sample data.
    """
    # Create sample data file
    sample_file = "sample_passenger_data.csv"
    create_sample_data(sample_file)
    
    # Create and execute the MapReduce framework
    mr = MapReduceFramework(num_mappers=2, num_reducers=2)
    mr.execute(sample_file)
    
    # Find the passenger(s) with the most flights
    top_passengers = mr.find_passengers_with_max_flights()
    
    # Save the results
    output_file = "sample_passenger_flight_counts.csv"
    save_results_to_csv(sorted(mr.final_results, key=lambda x: x[1], reverse=True), output_file)
    
    # Print the passenger(s) with the most flights
    print("\nPassenger(s) with the most flights:")
    for passenger_id, count in top_passengers:
        print(f"Passenger ID: {passenger_id}, Flights: {count}")
    
    # Print all passenger flight counts
    print("\nAll passenger flight counts:")
    for passenger_id, count in sorted(mr.final_results, key=lambda x: x[1], reverse=True):
        print(f"Passenger ID: {passenger_id}, Flights: {count}")
    
    # Clean up
    if os.path.exists(sample_file):
        os.remove(sample_file)
    if os.path.exists(output_file):
        os.remove(output_file)

if __name__ == "__main__":
    run_test()