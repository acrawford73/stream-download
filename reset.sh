#!/bin/bash
./stream.py -p

if test -d "video"; then
	rm -rf video
	echo "Video folder deleted."
else
	echo "Video folder not found."
fi
echo ""
