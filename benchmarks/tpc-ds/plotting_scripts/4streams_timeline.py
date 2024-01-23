import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import re
import sys
from datetime import datetime

# Constant for the threshold duration for long-running queries (in seconds)
THRESHOLD_DURATION = 600  # You can adjust this value as needed

def extract_query_number(filename):
    """Extracts the query number from the filename."""
    match = re.search(r'query_(\d+).sql', filename)
    return int(match.group(1)) if match else None

def convert_to_datetime(time_str):
    """Converts ISO format string to datetime object."""
    return datetime.fromisoformat(time_str)

def generate_query_colors(data):
    """Generates a color palette for unique queries."""
    unique_queries = data['query_number'].unique()
    palette = sns.color_palette("hsv", len(unique_queries))
    return {query: palette[i] for i, query in enumerate(unique_queries)}

def main(java_data_path, native_data_path):
    java_data = pd.read_csv(java_data_path)
    native_data = pd.read_csv(native_data_path)

    # Extract query numbers and convert time strings to datetime
    java_data['query_number'] = java_data['query_file'].apply(extract_query_number)
    native_data['query_number'] = native_data['query_file'].apply(extract_query_number)
    java_data['start_time'] = java_data['start_time'].apply(convert_to_datetime)
    native_data['end_time'] = native_data['end_time'].apply(convert_to_datetime)

    # Generating color palettes
    java_query_colors = generate_query_colors(java_data)
    native_query_colors = generate_query_colors(native_data)

    # Plotting
    fig, axs = plt.subplots(2, 1, figsize=(20, 15), sharex=True, gridspec_kw={'hspace': 0.3})
    stages = list(set(java_data['stage_id'].unique()) | set(native_data['stage_id'].unique()))
    stage_dict = {stage: i for i, stage in enumerate(stages)}

    def plot_timeline(data, ax, query_colors, engine_name):
        for _, row in data.iterrows():
            stage_num = stage_dict[row['stage_id']]
            color = query_colors.get(row['query_number'], 'black')
            ax.plot([mdates.date2num(row['start_time']), mdates.date2num(row['end_time'])], [stage_num, stage_num], color=color, linewidth=5)

            if row['duration_in_seconds'] >= THRESHOLD_DURATION:
                mid_time = mdates.date2num(row['start_time']) + (mdates.date2num(row['end_time']) - mdates.date2num(row['start_time'])) / 2
                ax.text(mid_time, stage_num + 0.1, f'Q{row["query_number"]}', verticalalignment='bottom', horizontalalignment='center', color='black', fontsize=10)

    plot_timeline(java_data, axs[0], java_query_colors, 'Java')
    plot_timeline(native_data, axs[1], native_query_colors, 'Native')

    axs[0].set_title('Java Engine Execution Timeline', pad=20)
    axs[1].set_title('Native Engine Execution Timeline', pad=20)
    for ax in axs:
        ax.set_yticks(range(len(stages)))
        ax.set_yticklabels(stages)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))

    plt.xticks(rotation=45)
    plt.xlabel('Time')
    plt.ylabel('Thread (Stage ID)')
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python script.py <java_csv_path> <native_csv_path>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
