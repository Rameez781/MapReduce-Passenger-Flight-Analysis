#!/usr/bin/env python3
"""
Unit tests for the MapReduce passenger flight analysis.
"""

import unittest
import os
import csv
from tempfile import NamedTemporaryFile
from passenger_flight_mapreduce import MapReduceFramework

class TestMapReduceFramework(unittest.TestCase):
    """Test cases for the MapReduce framework."""
    
    def setUp(self):
        """Set up test environment."""
        # Create a temporary CSV file with test data
        self.temp_file = NamedTemporaryFile(delete=False, suffix='.csv')
        self.temp_file.close()
        
        # Create test data
        with open(self.temp_file.name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Format: passenger_id, flight_id, source, destination, departure_time, flight_time
            writer.writerow(["ABC1234DE5", "FLT001", "DEN", "JFK", "1420564460", "120"])
            writer.writerow(["ABC1234DE5", "FLT002", "JFK", "LAX", "1420567460", "240"])
            writer.writerow(["FGH5678IJ9", "FLT003", "LAX", "SFO", "1420571060", "60"])
            writer.writerow(["KLM9012NO3", "FLT004", "SFO", "DEN", "1420574660", "150"])
            writer.writerow(["FGH5678IJ9", "FLT005", "DEN", "ORD", "1420578260", "90"])
    
    def tearDown(self):
        """Clean up after tests."""
        if os.path.exists(self.temp_file.name):
            os.remove(self.temp_file.name)
    
    def test_read_csv_data(self):
        """Test reading CSV data."""
        mr = MapReduceFramework()
        data = mr.read_csv_data(self.temp_file.name)
        
        # Check that all rows were read
        self.assertEqual(len(data), 5)
        
        # Check that columns were parsed correctly
        self.assertEqual(data[0]['passenger_id'], "ABC1234DE5")
        self.assertEqual(data[0]['flight_id'], "FLT001")
        self.assertEqual(data[0]['source'], "DEN")
        self.assertEqual(data[0]['destination'], "JFK")
        self.assertEqual(data[0]['departure_time_unix'], "1420564460")
        self.assertEqual(data[0]['flight_time'], "120")
    
    def test_split_data(self):
        """Test splitting data into chunks."""
        mr = MapReduceFramework()
        data = [{'id': i} for i in range(10)]
    
        # Split into 2 chunks
        chunks = mr.split_data(data, 2)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(len(chunks[0]), 5)
        self.assertEqual(len(chunks[1]), 5)
    
        # Split into 3 chunks (uneven)
        chunks = mr.split_data(data, 3)
        self.assertEqual(len(chunks), 4) 
        self.assertEqual(sum(len(chunk) for chunk in chunks), 10)

    def test_map_function(self):
        """Test the map function."""
        mr = MapReduceFramework()
        record = {'passenger_id': 'ABC1234DE5', 'flight_id': 'FLT001'}
        key, value = mr.map_function(record)
        
        self.assertEqual(key, 'ABC1234DE5')
        self.assertEqual(value, 1)
    
    def test_reduce_function(self):
        """Test the reduce function."""
        mr = MapReduceFramework()
        key = 'ABC1234DE5'
        values = [1, 1, 1]
        reduced_key, reduced_value = mr.reduce_function(key, values)
        
        self.assertEqual(reduced_key, 'ABC1234DE5')
        self.assertEqual(reduced_value, 3)
    
    def test_end_to_end(self):
        """Test the complete MapReduce process."""
        mr = MapReduceFramework(num_mappers=2, num_reducers=2)
        mr.execute(self.temp_file.name)
        
        # Get passenger with most flights
        top_passengers = mr.find_passengers_with_max_flights()
        
        # Check that the correct passenger was identified
        self.assertEqual(len(top_passengers), 2)
        passenger_ids = [p[0] for p in top_passengers]
        self.assertIn('ABC1234DE5', passenger_ids)
        self.assertIn('FGH5678IJ9', passenger_ids)
        
        # Check the flight counts
        flight_counts = {p[0]: p[1] for p in mr.final_results}
        self.assertEqual(flight_counts['ABC1234DE5'], 2)
        self.assertEqual(flight_counts['FGH5678IJ9'], 2)
        self.assertEqual(flight_counts['KLM9012NO3'], 1)

if __name__ == "__main__":
    unittest.main()