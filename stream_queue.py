#!/usr/bin/python3
# Author: Anthony Crawford
# Target Operating System: Ubuntu
# Python Version: 3
# Package: pycurl (https://pypi.org/project/pycurl/)
# sudo apt install python3-pycurl
# sudo apt install python-configparser
# Purpose: Based on a list of assets, download video content from
#  DLIVE.TV using multiprocessing queues.
# -----------------------------------------------------------------------------

import os
import sys
import errno
import multiprocessing
from multiprocessing import Pool, Queue
from distutils.util import strtobool
from random import randint
import shutil
# Logging
import logging
import logging.config
import logging.handlers
# Configuration
import configparser
# Third-party
import sqlite3
import pycurl


# Functions

def make_sure_path_exists(path):
	try:
		os.makedirs(path)
	except OSError as exception:
		if exception.errno != errno.EEXIST:
			raise

# Check if field is empty
def is_empty(any_structure):
	if any_structure:
		#print('Structure is not empty.')
		return False
	else:
		#print('Structure is empty.')
		return True

# check if asset list inputfile is present
def file_check_exists(inputfile):
	exists = os.path.isfile(inputfile)
	if exists:
		if debug:
			print('Asset file ' + inputfile + ' found.')
		return True
	else:
		if debug:
			print('Asset file ' + inputfile + ' not found!')
		return False

def db_purge(database):
	sql = """ CREATE TABLE "assets" (
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		asset TEXT NOT NULL DEFAULT "",
		asset_uri TEXT NOT NULL DEFAULT "",
		status INTEGER NOT NULL DEFAULT 0
	); """
	conn = sqlite3.connect(database)
	c = conn.cursor()
	c.execute("DROP TABLE assets")
	c.execute(sql)
	conn.commit()
	conn.close()
	print();print('Database ' + database + ' purged.');print()

def print_assets(assets):
	for asset in assets:
		print("[" + str(asset[0]) + "] " + asset[1] + " | " + asset[2] + " | " + str(asset[3]))

def print_help():
	print()
	print("Usage:")
	print("./stream.py -i <input-file>, where -i means 'ingest'. Imports the assets CSV file into the tool database.")
	print("./stream.py -p, where -p means 'purge'. Purges all assets from the assets database.")
	print("./stream.py -d, where -d means 'delete'. Deletes Completed and Failed assets from the tool database and NAS directory.")
	print("./stream.py -l, where -l means 'list'. Prints all assets in the tool database.")
	print("./stream.py -s <output-file>, where -s means 'stream'. Combines all video files into single transport stream.")
	print("./stream.py -h, where -h means 'help'. Prints this help information.")
	print("./stream.py, runs the ingest script.")
	print()

# create the database if not present
def db_check_exists(database):
	sql = """ CREATE TABLE "assets" (
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		asset TEXT NOT NULL DEFAULT "",
		asset_uri TEXT NOT NULL DEFAULT "",
		status INTEGER NOT NULL DEFAULT 0
	); """
	exists = os.path.isfile(database)
	if exists:
		#print('Database ' + database + ' already created.')
		return True
	else:
		print();print('Database ' + database + ' missing, creating new database.')
		conn = sqlite3.connect(database)
		c = conn.cursor()
		c.execute(sql)
		conn.commit()
		conn.close()
		print();print('Database ' + database + ' created.')
		return True

def db_asset_importer(database,inputfile):
	asset_count = 0
	conn = sqlite3.connect(database)
	c = conn.cursor()
	#log.info('Asset file: ' + inputfile)
	f=open(inputfile,'r')
	for line in f.readlines():
		if (not line in ['\n','\r\n']):
			if line.startswith('http') or line.startswith('https'):
				asset = (line.split('/')[-1]).strip();print("[" + str(asset_count+1) + "] " + asset)
				asset_uri = line.strip();print(asset_uri)
				c.execute("INSERT INTO assets (asset,asset_uri) \
					VALUES (?,?)", (asset,asset_uri))
				asset_count+=1
	f.close()
	conn.commit()
	conn.close()
	print();print('There were '+str(asset_count)+' assets imported from file ' + inputfile + '.');print()

def db_get_inventory(database):
	#assets_queued = []
	conn = sqlite3.connect(database)
	c = conn.cursor()
	c.execute("SELECT * FROM assets")
	assets_queued = c.fetchall()
	conn.close()
	return assets_queued

def download_target(url,ingest_count,assets_total):
	hostname = url.split('/')[2]
	headers = [
		'Accept:application/octet-stream',
		'Connection:keep-alive',
		'Content-Type:application/gzip',
		'Host:'+hostname,
		'User-Agent:Python'
	]

	c = pycurl.Curl()
	c.fp = None
	c.setopt(c.FOLLOWLOCATION, False)
	c.setopt(c.CONNECTTIMEOUT, 10)
	c.setopt(c.NOSIGNAL, True)
	c.setopt(c.FAILONERROR, True)
	c.setopt(c.HTTPHEADER, headers)
	c.setopt(c.NOPROGRESS, False)
	#if debug == True:
	#	c.setopt(c.VERBOSE, True)
	c.setopt(c.URL, url)
	filename = url.split('/')[-1]
	local_filename = os.path.join(storage_path, filename)
	with open(local_filename, 'wb') as f:
		print("Downloading: ["+str(ingest_count)+"/"+str(assets_total)+"] " + filename)
		c.setopt(c.WRITEFUNCTION, f.write)
		try:
			c.perform()
		except pycurl.error as e:
			#print('Status Code: %d' % c.getinfo(c.RESPONSE_CODE))
			if e.args[0] == pycurl.E_COULDNT_CONNECT and c.exception:
				print(c.exception)
			else:
				print(e)
			f.close()
			c.close()	
			return False
		print('Asset ' + filename + ' download completed in %0.3f seconds' % c.getinfo(c.TOTAL_TIME))
	f.close()
	c.close()
	return True

	# if failed add to fail queue


# Process download queue
def process_main(dq):
	while true:
		n = q.get();print(n)
		if n == 0:
			return True
		else:
			if download_target(n[0],n[1],n[2]) == False:
				fq.put(n)
			else:
				ingest_count+=1
				print("Downloading: ["+str(ingest_count)+"/"+str(assets_total)+"] " + filename)


# 
if __name__ == "__main__":

	### Load Configuration Parameters
	config = configparser.ConfigParser()
	config.read('etc/config.conf')
	#
	debug = strtobool(config.get('tool', 'debug_enabled'))
	database = config.get('tool', 'database')
	queue_limit = int(config.get('tool', 'queue_limit'))
	storage_path = config.get('tool', 'storage_path')
	http_timeout = int(config.get('tool', 'http_timeout'))

	# CLI Input
	inputfile = ""
	argv = sys.argv[1:]
	try:
		opts, args = getopt.getopt(argv,"hpdlfs:o:i:o:",["ifile="])
	except getopt.GetoptError:
		print_help()
		sys.exit(2)

	for opt, arg in opts:

		# Help
		if opt == '-h':
			print_help()
			sys.exit()

		# Purge
		elif opt == '-p':
			db_purge(database)
			sys.exit()

		# Delete
		elif opt == '-d':
			# remove all Completed/Failed assets from database and NAS
			inventory = db_get_inventory(database)
			assets_completed = inventory[3]
			assets_failed = inventory[4]
			if len(assets_completed) > 0:
				print();print('There are ' + str(len(assets_completed)) + ' completed assets that will be deleted.')
				for asset in assets_completed:
					time.sleep(0.2)
					delete_asset_db(asset[0])
					filename = os.path.join(storage_path, asset[2].split('/')[-1])
					deleted = delete_asset(filename)
					if deleted == True:
						print('Asset [' + str(asset[0]) + '] ' + filename + ' deleted.')
					elif deleted == False:
						print('Asset [' + str(asset[0]) + '] ' + filename + ' not deleted.')
				deleted = delete_asset('playback.m3u8')
				if deleted == True:
					print('playback.m3u8 file deleted.')

			# Failed asset means the download failed, so remove the downloaded file
			if len(assets_failed) > 0:
				print();print('There are ' + str(len(assets_failed)) + ' failed assets that will be deleted.')
				for asset in assets_failed:
					time.sleep(0.2)
					delete_asset_db(asset[0])
					filename = os.path.join(storage_path, asset[2].split('/')[-1])
					deleted = delete_asset(filename)	
					if deleted == True:
						print('Asset [' + str(asset[0]) + '] ' + filename + ' deleted.')
					elif deleted == False:
						print('Asset [' + str(asset[0]) + '] ' + filename + ' not deleted.')
			if (len(assets_completed) == 0) and (len(assets_failed) == 0):
				print();print("There are no assets ready to be deleted.")
			print();sys.exit()

		# List
		elif opt == '-l':
			assets_queued = db_get_inventory(database)
			if len(assets_queued) > 0:
				print();print("Queued Assets = " + str(len(assets_queued)))
				print_assets(assets_queued)
			if (len(assets_new) == 0) and (len(assets_queued) == 0) and (len(assets_active) == 0) and (len(assets_completed) == 0) and (len(assets_failed) == 0):
				print();print("The database contains no assets.")
			get_inventory_print(database)
			print();sys.exit()
		
		# Combine video files to a single stream file
		elif opt == '-s':
			output_file = arg
			counter = 1
			if output_file:
				asset_list = []
				f=open('playback.m3u8','r')
				for line in f.readlines():
					if not line in ['\n','\r\n']:
						if line.startswith('http') or line.startswith('https'):
							asset = (line.split('/')[-1]).strip()
							asset_list.append(asset)
				f.close()

				out_data = b''
				for file in asset_list:
					with open(storage_path + file, 'rb') as fp:
						print('[' + str(counter) + '] ' + file)
						out_data += fp.read()
					counter += 1	
				fp.close()
				print();print("Combining all *.ts files into single stream file " + output_file + "...")
				with open(output_file, 'wb') as fp:
					fp.write(out_data)
				fp.close()
				print();print('Done.')
			else:
				print();print("Stream filename not specified.")
			print();sys.exit()

		# Import assets list file or HLS playlist
		elif opt in ("-i", "--ifile"):
			inputfile = arg

	#----------------------------------------#
	# Run asset importer
	#----------------------------------------#
	print()

	# Get assets list from database
	if inputfile:
		if file_check_exists(inputfile):
			if db_check_exists(database):
				print();print('Importing assets file ' + inputfile + ' to database ' + database + '...');print()
				db_asset_importer(database,inputfile)
				#db_get_inventory_log(database)
				sys.exit()
			else:
				print();print("Database file doesn't exist.");print()
				sys.exit()
		else:
			print();print("Import file doesn't exist.");print()
			sys.exit()

	# Initialize Logging
	log_file = strftime('stream_%Y%m%d_%H%M%S.log')
	log_path = config.get('tool', 'log_path')
	logfile = os.path.join(log_path, log_file)
	make_sure_path_exists(log_path)
	if debug:
		logging.basicConfig(level=logging.DEBUG) # log to stdout
	else:
		logging.basicConfig(level=logging.INFO) # log to stdout
	log = logging.getLogger('Tool')
	handler = logging.FileHandler(logfile)
	if debug:
		handler.setLevel(logging.DEBUG)  # log to console
	else:
		handler.setLevel(logging.INFO)  # log to console
	formatter = logging.Formatter('%(asctime)s.%(msecs)03d:%(levelname)s:%(message)s','%Y-%m-%d %H:%M:%S')
	handler.setFormatter(formatter)
	log.addHandler(handler)

	# Create storage path if it doesn't exist
	make_sure_path_exists(storage_path)

	print()
	log.info('--------------------------------')
	log.info('Stream Downloader Tool')

	# Determine if we need to continue processing assets from the last time the script was run
	if (len(assets_new) > 0) or (len(assets_queued) > 0) or (len(assets_active) > 0) or (len(assets_failed) > 0):
		log.info('... Ingesting ...')
		ingesting = True
	else:
		log.info('There are no assets available to ingest. Exiting...')
		sys.exit(0)


	# Download Queue
	dq = Queue()
	
	# Failed Download Queue
	fq = Queue()

	p = Process(target=process_main, args(dq,fq,))
	p.start()

	# Pull assets from database and add them to the queue
	# loop
	dq.put([url,ingest_count,assets_total])

	# get length of fq, if > 0 process failed downloads

	# Stop processing queue
	dq.put(0)
	fq.put(0)
	p.join()


	#----------------------------------------#
	# Exit Summary
	log.info('Assets Ingested = ' + str(ingest_count))
	log.info('--------------------------------')
	log.info('Completed')

	dur = time.time() - stream_start_time
	pos = abs( int(dur) )
	day = int(pos / (3600*24))
	rem = pos % (3600*24)
	hour = int(rem / 3600)
	rem = rem % 3600
	mins = int(rem / 60)
	secs = int(rem % 60)

	if day == 1:
		dtxt = "day"
	else:
		dtxt = "days"

	if hour == 1:
		htxt = "hour"
	else:
		htxt = "hours"

	if mins == 1:
		mtxt = "minute"
	else:
		mtxt = "minutes"

	if secs == 1:
		stxt = "second"
	else:
		stxt = "seconds"

	duration = str(day)+' '+dtxt+' '+str(hour)+' '+htxt+' '+str(mins)+' '+mtxt+' '+str(secs)+' '+stxt
	log.info('Runtime = ' + duration)

	log.info('--------------------------------')

	#----------------------------------------#
	#red;white.blue()
	print();sys.exit()

