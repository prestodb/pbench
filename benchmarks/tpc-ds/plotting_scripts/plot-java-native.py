import pandas as pd
import matplotlib.pyplot as plt
import re
import sys

def extract_query_number(filename):
    """Extract the query number from the filename."""
    match = re.search(r'query_(\d+).sql', filename)
    return int(match.group(1)) if match else None

def load_and_process_csv(file_path):
    """Load CSV, extract query number, and aggregate data."""
    data = pd.read_csv(file_path)
    data['query_number'] = data['query_file'].apply(extract_query_number)
    aggregated_data = data.groupby('query_number')['duration_in_seconds'].mean()
    return aggregated_data

def main(java_data_path, native_data_path):
    # Load and process the data
    java_data = load_and_process_csv(java_data_path)
    native_data = load_and_process_csv(native_data_path)

    # Merge the datasets and calculate speedup
    comparison_data = pd.DataFrame({'Java': java_data, 'Native': native_data})
    comparison_data['Speedup'] = comparison_data['Java'] / comparison_data['Native']
    average_speedup = comparison_data['Speedup'].mean()

    # Plotting
    plt.figure(figsize=(15, 8))
    plt.bar(comparison_data.index, comparison_data['Speedup'], color='orange')
    plt.xlabel('Query Number')
    plt.ylabel('Speedup (Java Duration / Native Duration)')
    plt.title('Speedup of Native Engine Over Java Engine for Each Query')

    # Display query numbers at regular intervals on the x-axis
    interval = 5  # Change this value as needed for better spacing
    plt.xticks([i for i in comparison_data.index if i % interval == 0])

    plt.axhline(y=1, color='r', linestyle='-', label='Speedup = 1')
    plt.axhline(y=average_speedup, color='blue', linestyle='--', label=f'Average Speedup: {average_speedup:.2f}')
    plt.legend()
    plt.show()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python script.py <java_csv_path> <native_csv_path>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
