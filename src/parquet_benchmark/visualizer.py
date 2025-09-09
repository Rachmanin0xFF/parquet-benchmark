import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from collections import defaultdict

def create_time_based_gantt_with_lanes(df, color_by='file_partition'):
    """
    Create a Gantt chart with proper time-based x-axis and non-overlapping lanes
    color_by: 'file_partition' (default), 'query_type', or 'engine'
    """
    
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])
    
    # Sort by start time for proper lane assignment
    df = df.sort_values('start_time').reset_index(drop=True)
    
    # Lane assignment algorithm
    method_lanes = defaultdict(list)
    lane_assignments = []
    
    for _, row in df.iterrows():
        method = row['method']
        start = row['start_time']
        end = row['end_time']
        
        # Find available lane
        assigned_lane = None
        for i, lane_end in enumerate(method_lanes[method]):
            if lane_end <= start:
                assigned_lane = i
                method_lanes[method][i] = end
                break
        
        if assigned_lane is None:
            assigned_lane = len(method_lanes[method])
            method_lanes[method].append(end)
        
        lane_assignments.append(assigned_lane)
    
    df['lane'] = lane_assignments
    df['y_category'] = df['method'] + ' - Lane ' + (df['lane'] + 1).astype(str)

    import re
    def extract_part_number(url):
        # Pattern to match 'part-' followed by digits
        pattern = r"part-(\d+)"
        match = re.search(pattern, url)
        if match:
            return match.group(1)  # Returns just the number portion
        else:
            return None
    
    df['file_partition'] = df['url'].apply(extract_part_number)

    # Find the Range header column (case-insensitive)
    range_col = None
    for col in df.columns:
        if col.lower() == 'range':
            range_col = col
            break

    # Classify query type
    def classify_query_type(row):
        if row['method'] == 'HEAD':
            return 'HEAD'
        elif row['method'] == 'GET':
            if range_col and pd.notnull(row[range_col]):
                return 'RANGED GET'
            else:
                return 'GET'
        else:
            return row['method']
    df['query_type'] = df.apply(classify_query_type, axis=1)

    # Choose color column based on what's available and what's requested
    if color_by == 'engine' and 'engine' in df.columns:
        color_col = 'engine'
        color_label = 'Engine'
        color_seq = px.colors.qualitative.Set3
    elif color_by == 'query_type':
        color_col = 'query_type'
        color_label = 'Query Type'
        color_seq = px.colors.qualitative.Set1
    else:  # default to file_partition
        color_col = 'file_partition'
        color_label = 'File Partition'
        color_seq = px.colors.qualitative.Set2

    fig = px.timeline(
        df,
        x_start='start_time',
        x_end='end_time',
        y='y_category',
        color=color_col,
        hover_data=['status_code', 'response_size', 'method', 'response_time_ms', 'file_partition', 'query_type'],
        title='HTTP Requests Timeline - Non-Overlapping Layout',
        labels={
            'y_category': 'HTTP Method & Lane',
            color_col: color_label,
            'response_time_ms': 'Response Time (ms)'
        },
        color_discrete_sequence=color_seq
    )
    
    fig.update_layout(
        height=max(400, df['y_category'].nunique() * 40),
        xaxis_title='Time',
        yaxis_title='HTTP Method',
        yaxis={'categoryorder': 'category ascending'}
    )
    
    return fig