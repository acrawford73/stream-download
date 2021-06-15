#!/bin/bash
./stream.py -p
FILE=playback.m3u8
if test -f "$FILE"; then
	rm $FILE
	echo "$FILE deleted."
else
	echo "$FILE not found."
fi
echo ""

if test -d "video"; then
	rm -rf video
	echo "Video folder deleted."
else
	echo "Video folder not found."
fi
echo ""
