#!/usr/bin/python3

import os
import subprocess

# Directory with video files
input_dir = '/home/ubuntu/stream-download/files'
output_dir = os.path.join(input_dir, 'transcoded')

# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Supported file extensions
video_extensions = ['.mov', '.mp4']

# Get list of video files
video_files = [f for f in os.listdir(input_dir) if os.path.splitext(f)[1].lower() in video_extensions]

# Transcode each video file using HandBrakeCLI
for video_file in video_files:
    input_file = os.path.join(input_dir, video_file)
    output_file = os.path.join(output_dir, os.path.splitext(video_file)[0] + '.mp4')
    
    # HandBrakeCLI command for transcoding
    command = [
        'HandBrakeCLI',
        '-i', input_file,
        '-o', output_file,
        '--preset', 'Very Fast 720p30',
        '-e', 'x264',
        '-q', '26',
        '-r', '29.97',
        '-R', '44.1',
        '--ab', '128'
    ]
    
    print(f"Transcoding {video_file}...")
    try:
        # Run the command
        subprocess.run(command, check=True)
        print(f"Transcoded {video_file} successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to transcode {video_file}. Error: {e}")

print("Transcoding completed.")
