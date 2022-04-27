# Stream Download

Purpose: To archive or scrape video from Dlive.tv. 

Description: The tool downloads non-DRM ABR video files from CDNs based on the M3U8 playlist. It can combine all downloaded video \*.ts files into a single specified \*.ts file. 

## Dependencies

Python 3.x

Package: python-pycurl

## Usage

Get an \*.m3u8 playlist file from the streaming website. (example link)

```bash
wget -L https://url.com/playlist.m3u8
```

Import the streaming media assets from the playlist file into the database.

```bash
./stream.py -i playlist.m3u8
```

View imported streaming asset details in database. (-l as in list)

```bash
./stream.py -l
```

Download all streaming media assets from CDN and store locally in "video" folder. If any assets fail to download, just run the script again.

```bash
./stream.py
```

Combine all streaming media files into one video file. Specify the output filename.

```bash
./stream.py -s output.ts
```

Delete all downloaded files and assets from database.

```bash
./stream.py -d
```

Purge the database to start over.

```bash
./stream.py -p
```

Clear the database and remove all downloaded video files. (faster than -d option)
```bash
./reset.sh
```

## Transcoding

Transcode the combined stream file from TS to MP4 using Handbrake.
