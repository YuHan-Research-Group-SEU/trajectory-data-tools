#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   data_tools.py
@Time    :   2026/01/05
@Author  :   XinkaiJi
@Contact :   xinkaiji@hotmail.com
@Version :   1.0
@Software:   VS Code
@Desc    :   Data tools for trajectory data.
    - read_parquet: read Parquet file, extract embedded metadata, and convert trajectory data back to dictionary format.
    - plot_trajectory_spacetime_diagram: plot trajectory spacetime diagram from Parquet file.
'''
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
import os
import argparse

def read_parquet(parquet_path):
    '''
    Read Parquet file, extract embedded metadata, and convert trajectory data back to dictionary format.
    
    :param parquet_path: the path to the Parquet file
    :return: restored_tracks (Dict), restored_meta (Dict)
    '''
    print("\n--- Reading back from Parquet ---")
    
    # 1. Read Parquet file
    table = pq.read_table(parquet_path)
    
    # 2. Extract and parse Metadata
    file_meta = table.schema.metadata
    
    if b'dataset_meta' in file_meta:
        restored_meta_json = file_meta[b'dataset_meta'].decode('utf-8')
        restored_meta = json.loads(restored_meta_json)
        
        print("\n[Success] Meta embedded in Parquet found:")
        # Print all meta info
        print(restored_meta)
        print(f"  - Location: {restored_meta.get('location_name')}")
        print(f"  - Lane Map (Dict): {restored_meta.get('lane_sequence_to_movement_map')}")
    else:
        print("\n[Warning] 'dataset_meta' key not found in Parquet header.")
    
    # 3. Convert back to DataFrame to view data
    df_read = table.to_pandas()
    
    print("\n[Success] Trajectory Data loaded:")
    print(f"  - Shape: {df_read.shape}")
    print(f"  - Columns: {list(df_read.columns)}")
    
    # Verify complex structure (pixel_corners)
    sample_corners = df_read.iloc[0]['pixel_corners']
    print(f"  - Sample pixel_corners type: {type(sample_corners)}")
    print(f"  - Sample pixel_corners shape (len): {len(sample_corners)} (should be 5)")

      # 4. Convert back to Dict 
    print("\n--- Converting DataFrame back to Dict ---")
    restored_tracks = {}
    # Convert DataFrame to record list
    records = df_read.to_dict(orient='records')
    
    for record in records:
        # Assume vehicle_id exists and is unique
        if 'vehicle_id' in record:
            vid = record['vehicle_id']
            del record['vehicle_id']
            restored_tracks[vid] = record
            
    print(f"[Success] Converted back to Dict. Total tracks: {len(restored_tracks)}")
    if restored_tracks:
        sample_vid = list(restored_tracks.keys())[0]
        print(f"  - Sample Vehicle ID: {sample_vid}")
        print(f"  - Sample Keys in Track Dict: {list(restored_tracks[sample_vid].keys())[:5]} ...")
    return restored_tracks, restored_meta

def plot_trajectory_spacetime_diagram(trajectory_data, meta_data):
    '''
    Visualize trajectory data using Matplotlib.
    Plot time-space diagram for vehicles.
    Read unique_lane_ids from meta data and plot trajectory for each lane.
    Convert frenet_s to vehicle head center coordinate during plotting.
    Use frame_index and frame_interval from meta data to calculate time.
    Color the trajectory based on frenet_s_speed.
    Determine lane change positions based on lane_id.
    
    :param trajectory_data: the trajectory data in dictionary format
    :param meta_data: the meta data in dictionary format
    :return: None
    '''
    print("\n--- Plotting Spacetime Diagrams ---")
    
    unique_lane_ids = meta_data.get('unique_lane_ids', [])
    # Prioritize frame_interval, fallback to time_step (default 0.1)
    frame_interval = meta_data['frame_interval']
    
    # Create folder for saving figures
    save_folder = "fig"
    os.makedirs(save_folder, exist_ok=True)
    
    for target_lane_id in unique_lane_ids:
        # Skip invalid lane ID
        if target_lane_id == -1:
            continue
            
        print(f"Processing Lane {target_lane_id}...")
        
        fig, ax = plt.subplots(figsize=(20, 8))
        
        lines = []
        speeds = []
        # Store lane change points: LC (Lane Change)
        lc_points_x = []
        lc_points_y = []
        
        for vid, track in trajectory_data.items():
            # Extract data
            frames = np.array(track['frame_index'])
            s_coords = np.array(track['frenet_s'])
            s_speeds = np.array(track['frenet_s_speed'])
            lane_ids = np.array(track['lane_id'])
            
           
            vehicle_length = track.get('vehicle_length', 5.0)
            
            # Determine trajectory direction: increasing or decreasing
            # Use start and end points to determine overall trend
            if len(s_coords) > 1 and s_coords[-1] < s_coords[0]:
                direction_sign = -1
            else:
                direction_sign = 1

            # Calculate vehicle head coordinate
            # frenet_s is the center point
            # If increasing (direction_sign=1) add length, if decreasing (direction_sign=-1) subtract length
            head_s = s_coords + direction_sign * (vehicle_length / 2.0)
            
            # Calculate time: frame_index * frame_interval
            times = frames * frame_interval
            
            # Iterate through all frames of the vehicle to find segments in target_lane_id
            for i in range(len(frames) - 1):
                curr_lane = lane_ids[i]
                next_lane = lane_ids[i+1]
                
                # Case 1: Driving within the target lane
                if curr_lane == target_lane_id and next_lane == target_lane_id:
                    p1 = (times[i], head_s[i])
                    p2 = (times[i+1], head_s[i+1])
                    lines.append([p1, p2])
                    speeds.append(abs(s_speeds[i])) # Use absolute speed
                
                # Case 2: Lane change points (Cut-in or Cut-out)
                # Logic: Either this point or the next involves the target lane, and a lane change occurred
                
                # Cut-out: Currently in target, next frame not
                elif curr_lane == target_lane_id and next_lane != target_lane_id:
                    lc_points_x.append(times[i])
                    lc_points_y.append(head_s[i])
                    
                # Cut-in: Currently not in target, next frame is
                elif curr_lane != target_lane_id and next_lane == target_lane_id:
                    lc_points_x.append(times[i+1])
                    lc_points_y.append(head_s[i+1])

        # Skip plotting if no data for this lane
        if not lines:
            print(f"  No data for Lane {target_lane_id}")
            plt.close(fig)
            continue

        # Create LineCollection
        # Speed typically 0-35m/s (0-120km/h), use jet colormap
        lc = LineCollection(lines, array=np.array(speeds), cmap="jet", linewidths=1.0)
        lc.set_clim(vmin=0, vmax=35) # Set speed color range 0-35 m/s
        ax.add_collection(lc)
        
        # Add Colorbar
        cb = fig.colorbar(lc, ax=ax)
        cb.set_label('Speed [m/s]')
        
        # Plot lane change points
        if lc_points_x:
            ax.scatter(lc_points_x, lc_points_y, 
                       marker='o', s=20, 
                       color='k', linewidths=0.8, 
                       label='Lane Change', zorder=3)
            ax.legend(loc='upper right')
            
        ax.autoscale()
        ax.set_title(f'Lane {target_lane_id} Time-Space Diagram')
        ax.set_xlabel("Time [s]")
        ax.set_ylabel("Car Head Location (Frenet S) [m]")
        
        # Save figure
        save_path = os.path.join(save_folder, f'lane_{target_lane_id}_spacetime.png')
        plt.tight_layout()
        plt.savefig(save_path, dpi=300)
        plt.close(fig)
        print(f"  Saved: {save_path}")


# intersection
def analysis_movement_data(trajectory_data, meta_data):
    '''
    Analyze movement data from trajectory data.
    '''
    print("\n--- Analyzing Movement Data ---")
    
    lane_sequence_to_movement_map = meta_data.get('lane_sequence_to_movement_map', {})
    
    # 1. Calculate global frame range to filter partial trajectories
    all_start_frames = []
    all_end_frames = []
    for track in trajectory_data.values():
        if 'frame_index' in track and track['frame_index'] is not None and len(track['frame_index']) > 0:
            all_start_frames.append(track['frame_index'][0])
            all_end_frames.append(track['frame_index'][-1])
            
    if not all_start_frames:
        print("No valid frames found.")
        return {}

    global_min_frame = min(all_start_frames)
    global_max_frame = max(all_end_frames)
    
    print(f"Global Frame Range: {global_min_frame} - {global_max_frame}")
    
    # Initialize counters
    movement_counts = {}
    od_counts = {}
    od_vehicles = {} # Store vehicle IDs for each OD pair
    
    total_vehicles = len(trajectory_data)
    valid_movement_vehicles = 0
    undefined_filtered_count = 0

    for vid, track in trajectory_data.items():
        # lane_sequence might be a list or numpy array
        lane_seq = track.get('lane_sequence')
        
        if lane_seq is None or len(lane_seq) == 0:
            continue
            
        # Get start and end lane
        try:
            start_lane = int(lane_seq[0])
            end_lane = int(lane_seq[-1])
        except Exception:
            continue
            
        # Create OD Key
        od_key = f"{start_lane}-{end_lane}"
        
        # Determine movement name
        if od_key in lane_sequence_to_movement_map:
            movement_name = lane_sequence_to_movement_map[od_key]
        else:
            movement_name = "Undefined"
            
        # Check if we need to filter (only for Undefined movements)
        if movement_name == "Undefined":
            vehicle_frames = track.get('frame_index')
            # Ensure vehicle_frames is valid and not empty
            if vehicle_frames is None or len(vehicle_frames) == 0:
                continue

            v_start = vehicle_frames[0]
            v_end = vehicle_frames[-1]
            
            # Exclusion condition: exists at global start or global end
            if v_start < global_min_frame+3 or v_end > global_max_frame-3:
                undefined_filtered_count += 1
                continue
        
        # Count
        movement_counts[movement_name] = movement_counts.get(movement_name, 0) + 1
        od_counts[od_key] = od_counts.get(od_key, 0) + 1
        
        # Record Vehicle ID
        if od_key not in od_vehicles:
            od_vehicles[od_key] = []
        od_vehicles[od_key].append(vid)
        
        if movement_name != "Undefined":
            valid_movement_vehicles += 1

    print(f"Total Vehicles: {total_vehicles}")
    print(f"Identified Movements: {valid_movement_vehicles}")
    print(f"Filtered Undefined Vehicles (Time Boundary): {undefined_filtered_count}")
    
    print("\n[Movement Statistics]")
    for name, count in sorted(movement_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  - {name}: {count}")
        
    print("\n[OD Pair Statistics (Top 10)]")
    for od, count in sorted(od_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        mapped_name = lane_sequence_to_movement_map.get(od, "Undefined")
        print(f"  - {od} ({mapped_name}): {count}")
        if mapped_name == "Undefined":
             print(f"    -> Vehicle IDs: {od_vehicles.get(od, [])}")

    return movement_counts


def main():
    parser = argparse.ArgumentParser(
        description="Plot trajectory spacetime diagram from Parquet file."
    )
    parser.add_argument('--parquet',default='data/Hurong_20220617_B3_F1/Hurong_20220617_B3_F1.parquet', help="Path to the Parquet file")
    args = parser.parse_args()
    trajectory_data, meta_data = read_parquet(args.parquet)
    # if trajectory_data and meta_data:
    #     plot_trajectory_spacetime_diagram(trajectory_data, meta_data)
    analysis_movement_data(trajectory_data, meta_data)
if __name__ == "__main__":
    main()