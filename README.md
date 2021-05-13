# Stream Download

Downloads ABR video files from CDNs based on the M3U8 playlist. 

Combines all downloaded video \*.ts video files into a single \*.ts file. 

## Dependency

sudo apt-get install python-pycurl

## Usage

Get an \*.m3u8 playlist file for importing streaming assets.

Import the streaming media assets from the playlist file into the database.

./stream.py -i playlist.m3u8

View imported streaming asset details in database. (-l as in list)

./stream.py -l

Download all streaming media assets from CDN and store locally in "video" folder.

./stream.py

Combine all streaming media files into one video file. Specify the output filename.

./stream.py -s output.ts

Delete all downloaded files.

./stream.py -d

Purge the database to start over.

./stream.py -p

## Transcoding

Transcode the combined stream file from TS to MP4 using Handbrake.
