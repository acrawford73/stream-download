#!/usr/bin/python3
# Here is an example of a Python script that can add multiple .mp4 URLs to a queue, 
# download multiple .mp4 files from the queue in parallel, based on the number of CPU cores.
# Using the concurrent.futures, queue and multiprocessing library.

# This script uses Queue module to create a queue and add the URLs to it. 
# Then it uses cpu_count() function from multiprocessing library to determine the number of CPU cores available. 
# It then uses min function to take the minimum of number of urls and number of CPU cores.
# It creates a concurrent.futures.ProcessPoolExecutor object with the number of cores as the number of processes.
# Then it uses the submit function to submit the download_file function with each url in the urls list, 
# also passing the queue as a second argument. The download_file function will download the file and put the 
# filename in the queue. After all the urls have been processed, the script uses a while loop to 
# print the filenames from the queue.

### Rumble Downloader
# Grab the master M3U8 file from the video page using the browser Web Inspector.
# The master M3U8 file will include a list of multiple video sizes, so pick one.
# The sized M3U8 file will include a list of all video segments. Place this into a text file.
# Grab the request URL to a segment and add it to each line of the text file, 'urls.txt'.
###

import os
import sys
import errno
import concurrent.futures
#import urllib.request
from multiprocessing import cpu_count
from queue import Queue
from time import strftime
from random import randint
import datetime,time
import shutil
import re, urllib.parse
# Logging
import logging
import logging.config
import logging.handlers
# Configuration
import configparser
import getopt
# Third-Party
import sqlite3
import pycurl


### Functions

def str_to_bool(s):
	if s == "True":
		return True
	elif s == "False":
		return False
	else:
		return None

def make_sure_path_exists(path):
	try:
		os.makedirs(path)
	except OSError as exception:
		if exception.errno != errno.EEXIST:
			raise

def delete_asset(path):
	if os.path.exists(path):
		try:
			os.remove(path)
		except OSError:
			print("Error occurred deleting file: " + path)
			return False
	else:
		print("File does not exist: " + path)
		return False
	return True

def delete_asset_db(asset):
	conn = sqlite3.connect(database)
	c = conn.cursor()
	c.execute("DELETE FROM assets WHERE id=?", (int(asset),))
	conn.commit()
	conn.close()

def duration_msg(duration,message):
	duration = round(duration,3)
	if duration < 1:
		duration = str(duration * 1000)
		duration = duration.split('.')[0]
		msg = message + ' ' + duration + ' msec'
	elif duration >= 1 and duration < 60:
		msg = message + ' ' + str(duration) + ' sec'
	elif duration >= 60: # minutes
		minutes, seconds = divmod(duration, 60)
		if seconds > 0:
			msg = message + ' ' + str(minutes) + ' min ' + str(seconds) + ' sec'
		else:
			msg = message + ' ' + str(minutes) + ' min'
	return msg

# Log duration of event (minutes seconds)
def duration_log(duration,message):
	duration = round(duration,3)
	log.debug(duration_msg(duration,message))

# Calculate duration of event (minutes seconds)
def duration_dld(duration):
	duration = round(duration,3)
	if duration < 1:
		duration = str(duration * 1000)
		duration = duration.split('.')[0]
		dur = duration + ' msec'
	elif duration >= 1 and duration < 60: # sec
		dur = str(duration) + ' sec'
	elif duration >= 60: # minutes
		minutes, seconds = divmod(duration, 60)
		if seconds > 0:
			dur = str(minutes) + ' min ' + str(seconds) + ' sec'
		else:
			dur = str(minutes) + ' min'
	return dur

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
			print('Assets file ' + inputfile + ' found.')
		return True
	else:
		if debug:
			print('Assets file ' + inputfile + ' not found!')
		return False

# Any asset that failed/error is written to CSV log
def csv_asset_failed(pid_aid,csvfile,error):
	if not file_check_exists(csvfile):
		with open(csvfile, 'w') as f:
			f.write('asset,error\n')
		f.close()
	with open(csvfile, 'a') as f:
		f.write(pid_aid + ',' + error +'\n')
	f.close()

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

				m = re.search(r"[?&]r_file=([^&]+)", line)
				if m:
					asset = urllib.parse.unquote(m.group(1))

				print("[" + str(asset_count+1) + "] " + asset)
				asset_uri = line.strip();print(asset_uri)
				c.execute("INSERT INTO assets (asset,asset_uri) \
					VALUES (?,?)", (asset,asset_uri))
				asset_count+=1
	f.close()
	conn.commit()
	conn.close()
	print();print('There were '+str(asset_count)+' assets imported from file ' + inputfile + '.');print()


def get_inventory_print(database):
	print('--------------------------------')
	print('Assets Database:')

	assets_new = []
	assets_queued = []
	assets_completed = []
	assets_failed = []

	conn = sqlite3.connect(database)
	# New
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=0")
	assets_new = c.fetchall()
	print('New       = ' + str(len(assets_new)))
	# Queued
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=1")
	assets_queued = c.fetchall()
	print('Queued    = ' + str(len(assets_queued)))
	# Failed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=6")
	assets_failed = c.fetchall()
	print('Failed    = ' + str(len(assets_failed)))
	# Completed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=7")
	assets_completed = c.fetchall()
	print('Completed = ' + str(len(assets_completed)))

	conn.close()
	print('--------------------------------')
#   return [assets_new, assets_queued, assets_completed, assets_failed]


def delete_asset(path):
	if os.path.exists(path):
		try:
			os.remove(path)
		except OSError:
			print("Error occurred deleting file: " + path)
			return False
	else:
		print("File does not exist: " + path)
		return False
	return True

def delete_asset_db(asset):
	conn = sqlite3.connect(database)
	c = conn.cursor()
	c.execute("DELETE FROM assets WHERE id=?", (int(asset),))
	conn.commit()
	conn.close()

def duration_msg(duration,message):
	duration = round(duration,3)
	if duration < 1:
		duration = str(duration * 1000)
		duration = duration.split('.')[0]
		msg = message + ' ' + duration + ' msec'
	elif duration >= 1 and duration < 60:
		msg = message + ' ' + str(duration) + ' sec'
	elif duration >= 60: # minutes
		minutes, seconds = divmod(duration, 60)
		if seconds > 0:
			msg = message + ' ' + str(minutes) + ' min ' + str(seconds) + ' sec'
		else:
			msg = message + ' ' + str(minutes) + ' min'
	return msg

# Log duration of event (minutes seconds)
def duration_log(duration,message):
	duration = round(duration,3)
	log.debug(duration_msg(duration,message))

# Calculate duration of event (minutes seconds)
def duration_dld(duration):
	duration = round(duration,3)
	if duration < 1:
		duration = str(duration * 1000)
		duration = duration.split('.')[0]
		dur = duration + ' msec'
	elif duration >= 1 and duration < 60: # sec
		dur = str(duration) + ' sec'
	elif duration >= 60: # minutes
		minutes, seconds = divmod(duration, 60)
		if seconds > 0:
			dur = str(minutes) + ' min ' + str(seconds) + ' sec'
		else:
			dur = str(minutes) + ' min'
	return dur

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
			print('Assets file ' + inputfile + ' found.')
		return True
	else:
		if debug:
			print('Assets file ' + inputfile + ' not found!')
		return False

def print_assets(assets):
	for asset in assets:
		print("[" + str(asset[0]) + "] " + asset[1] + " | " + asset[2] + " | " + str(asset[3]))

def print_help():
	print()
	print("Usage:")
	print("./stream.py -i <input-file>, where -i means 'import'. Imports the m3u8 playlist file into the tool database.")
	print("./stream.py -p, where -p means 'purge'. Purges all assets from the assets database.")
	print("./stream.py -d, where -d means 'delete'. Deletes Completed and Failed assets from the tool database.")
	print("./stream.py -l, where -l means 'list'. Prints all assets in the tool database.")
	print("./stream.py -s <output-file>, where -s means 'save'. Combines all video files and saves as a single transport stream.")
	print("./stream.py -h, where -h means 'help'. Prints this help information.")
	print("./stream.py, runs the download script.")
	print()

def db_get_inventory_log(database):
	log.info('--------------------------------')
	log.info('Assets Database:')

	assets_new = []
	assets_queued = []
	assets_completed = []
	assets_failed = []

	conn = sqlite3.connect(database)
	# New
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=0")
	assets_new = c.fetchall()
	log.info('New       = ' + str(len(assets_new)))
	# Queued
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=1")
	assets_queued = c.fetchall()
	log.info('Queued    = ' + str(len(assets_queued)))
	# Failed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=6")
	assets_failed = c.fetchall()
	log.info('Failed    = ' + str(len(assets_failed)))
	# Completed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=7")
	assets_completed = c.fetchall()
	log.info('Completed = ' + str(len(assets_completed)))

	conn.close()
	log.info('--------------------------------')
	return [assets_new, assets_queued, assets_completed, assets_failed]


def db_get_inventory(database):
	assets_new = []
	assets_queued = []
	assets_completed = []
	assets_failed = []

	conn = sqlite3.connect(database)
	# New
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=0")
	assets_new = c.fetchall()
	# Queued
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=1")
	assets_queued = c.fetchall()
	# Failed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=6")
	assets_failed = c.fetchall()
	# Completed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=7")
	assets_completed = c.fetchall()

	conn.close()
	return [assets_new, assets_queued, assets_completed, assets_failed]


def db_update_asset_status(database,aid,status):
	conn = sqlite3.connect(database)
	c = conn.cursor()
	c.execute("UPDATE assets SET status=? WHERE id=?", (status,aid))
	conn.commit()
	conn.close()

def db_update_asset_status_asset(database,asset,status):
	conn = sqlite3.connect(database)
	c = conn.cursor()
	c.execute("UPDATE assets SET status=? WHERE asset=?", (status,asset))
	conn.commit()
	conn.close()

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


# Download asset from target

# def download_file(url, q):
#     filename = os.path.join(".", url.split("/")[-1])
#     urllib.request.urlretrieve(url, filename)
#     q.put(filename)

def download_target(url,assets_total):
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
	# these keepalive options are not available on CentOS.
	# these options are kept here for reference.
	#c.setopt(c.TCP_KEEPALIVE, True)
	#c.setopt(c.TCP_KEEPINTVL, 30L)
	#c.setopt(c.TCP_KEEPIDLE, 120L)
	c.setopt(c.FOLLOWLOCATION, False)
	c.setopt(c.CONNECTTIMEOUT, 10)
	# The connection is dropped if the asset isn't downloaded within the c.TIMEOUT window.
	#c.setopt(c.TIMEOUT, 60L)  # DO NOT set this timeout! 
	c.setopt(c.NOSIGNAL, True)
	#c.setopt(c.FORBID_REUSE, True)  # Disabled, reuse same TCP socket
	c.setopt(c.FAILONERROR, True)
	c.setopt(c.HTTPHEADER, headers)
	c.setopt(c.NOPROGRESS, False)
	if debug == True:
		c.setopt(c.VERBOSE, True)
	c.setopt(c.URL, url)
	filename = url.split('/')[-1]
	local_filename = os.path.join(storage_path, filename)
	with open(local_filename, 'wb') as f:
		#print();log.info("Downloading: " + filename)
		#log.info("Downloading: ["+str(ingest_count)+"/"+str(assets_total)+"] " + filename)
		c.setopt(c.WRITEFUNCTION, f.write)
		try:
			c.perform()
			#log.info('Asset download  ' + filename + ' completed in %0.3f seconds' % c.getinfo(c.TOTAL_TIME))
			db_update_asset_status_asset(database,filename,3)
		except pycurl.error as e:
			#log.info('Asset failed to download  ' + url)
			db_update_asset_status_asset(database,filename,4)
			#print('Status Code: %d' % c.getinfo(c.RESPONSE_CODE))
			if e.args[0] == pycurl.E_COULDNT_CONNECT and c.exception:
				log.error(c.exception)
			else:
				log.error(e)
			f.close()
			c.close()   
			return False
		finally:
			f.close()
			c.close()
	return True

### End of Functions



##### Main #####

if __name__ == "__main__":

	### Load Configuration Parameters
	config = configparser.ConfigParser()
	config.read('etc/config.conf')
	debug = str_to_bool(config.get('tool', 'debug_enabled'))
	database = config.get('tool', 'database')
	storage_path = config.get('tool', 'storage_path')
	http_timeout = int(config.get('tool', 'http_timeout'))
	ingest_count = 0
	ingesting = False

	### Initialize Logging
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

	### Create file storage path if !exist
	make_sure_path_exists(storage_path)

	### CLI Command Options
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
			assets_completed = inventory[2]
			assets_failed = inventory[3]
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
			inventory = db_get_inventory(database)
			assets_new = inventory[0]
			if len(assets_new) > 0:
				print();print("New Assets = " + str(len(assets_new)))
				print_assets(assets_new)
			assets_queued = inventory[1]
			if len(assets_queued) > 0:
				print();print("Queued Assets = " + str(len(assets_queued)))
				print_assets(assets_queued)
			assets_completed = inventory[2]
			if len(assets_completed) > 0:
				print();print("Completed Assets = " + str(len(assets_completed)))
				print_assets(assets_completed)
			assets_failed = inventory[3]
			if len(assets_failed) > 0:
				print();print("Failed Assets = " + str(len(assets_failed)))
				print_assets(assets_failed)
			if (len(assets_new) == 0) and (len(assets_queued) == 0) and (len(assets_completed) == 0) and (len(assets_failed) == 0):
				print();print("The database contains no assets.")
			get_inventory_print(database)
			print();sys.exit()
		
		# Combine video files to a single stream file
		elif opt == '-s':
			output_file = arg
			counter = 1
			if output_file:
				inventory = db_get_inventory(database)
				assets_completed = inventory[2]
				out_data = b''
				for asset in assets_completed:
					with open(storage_path + asset[1], 'rb') as fp:
						print('[' + str(counter) + '] ' + asset[1])
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

		# Import assets list file or .m3u8 playlist
		# All media assets in the .m3u8 playlist must provide the full url
		elif opt in ("-i", "--ifile"):
			inputfile = arg
			print()
			if inputfile:
				if file_check_exists(inputfile):
					if db_check_exists(database):
						print();print('Importing playlist file ' + inputfile + ' to database ' + database + '...');print()
						db_asset_importer(database,inputfile)
						#db_get_inventory_log(database)
						sys.exit()
					else:
						print();print("Database file doesn't exist.");print()
						sys.exit()
				else:
					print();print("Import file doesn't exist.");print()
					sys.exit()
		# else:
		#     print_help()
		#     sys.exit()


	### Run Downloader using multi-processing and queues ###

	# def download_file(url, q):
	#     filename = os.path.join(".", url.split("/")[-1])
	#     urllib.request.urlretrieve(url, filename)
	#     q.put(filename)

	status_new = 0
	status_queued = 1
	status_failed = 6
	status_completed = 7

	### Get the latest asset statuses
	if db_check_exists(database):
		inventory = db_get_inventory_log(database)
		assets_new = inventory[0]
		assets_queued = inventory[1]
		assets_completed = inventory[2]
		assets_failed = inventory[3]
		# Total number of assets for the download queue
		assets_total = len(assets_new) + len(assets_queued) + len(assets_failed)

	# Exit if no assets available or ingest completed
	if assets_total > 0:
		ingesting = True
		log.info('... Downloading ...')
		# Prepare the queue
		q = Queue()
		# Assets fresh in the database
		for asset in assets_new:
			q.put(asset[2])
		# Assets that were previously queued
		for asset in assets_queued:
			q.put(asset[2])
		# Assets that failed to download previously
		for asset in assets_failed:
			q.put(asset[2])
	else:
		ingesting = False
		log.info('There are no assets ready to download, or script has completed. Exiting...')
		db_get_inventory_log(database)
		sys.exit()

	num_cores = min(q.qsize(),cpu_count())

	# Start the download timer
	stream_start_time = time.time()

	with concurrent.futures.ProcessPoolExecutor(num_cores) as executor:
		while not q.empty():
			url = q.get()
			executor.submit(download_target, url, assets_total)

	while not q.empty():
		print(q.get())


#    sys.exit()



	# while ingesting:

	#     #----------------------------------------#
	#     # Exit if no assets available or ingest completed
	#     if (len(assets_new) == 0) and (len(assets_queued) == 0) and (len(assets_failed) == 0):
	#         ingesting = False
	#         log.info('There are no assets ready to download, or script has completed. Exiting...')
	#         db_get_inventory_log(database)

	#     # Download continues ...
	#     else:

	#         # Get current asset inventory for processing
	#         inventory = db_get_inventory(database)
	#         assets_new = inventory[0]
	#         assets_queued = inventory[1]
	#         assets_completed = inventory[2]
	#         assets_failed = inventory[3]

	#         log.debug('----------------')
	#         log.debug('Asset Inventory:')
	#         for asset in assets_new:
	#             log.debug(asset)
	#         log.debug('New       = ' + str(len(assets_new)))
	#         for asset in assets_queued:
	#             log.debug(asset)
	#         log.debug('Queued    = ' + str(len(assets_queued)))
	#         for asset in assets_completed:
	#             log.debug(asset)
	#         log.debug('Completed = ' + str(len(assets_completed)))
	#         for asset in assets_failed:
	#             log.debug(asset)
	#         log.debug('Failed    = ' + str(len(assets_failed)))


	#     #----------------------------------------#
	#     # Process Queued Assets
	#     # Check if the asset is ingested already
	#     if len(assets_queued) > 0:

	#         downloaded = False

	#         # slack off a bit before each asset download
	#         # time.sleep(sleep_timer)

	#         # Download the asset file if not downloaded already
	#         # Allows resume from last downloaded file
	#         if not file_check_exists(os.path.join('video/', asset[1])):
	#             downloaded = download_target(asset[2],ingest_count+1,assets_total)
	#         else:
	#             continue

	#         # Update database only if download was successful.
	#         if downloaded == True:
	#             db_update_asset_status(database,asset[0],status_completed)
	#             log.debug('Asset [' + str(asset[0]) + '] ' + asset[1] + ' was successfully downloaded.')
	#             ingest_count+=1
	#         else:
	#             db_update_asset_status(database,asset[0],status_failed)
	#             log.error('Failed to download asset [' + str(asset[0]) + '] ' + asset[1])
	#             csvfn_errors = log_file.rsplit('.',1)[0] + '_failed.csv'
	#             csvfile_errors = os.path.join(log_path, csvfn_errors)
	#             if os.path.exists(csvfile_errors) == False:
	#                 with open(csvfile_errors, 'w') as f:
	#                     f.write('asset,error\n')
	#                 f.close()
	#             csv_asset_failed(asset[1],csvfile_errors,"Failed to download asset from CDN")

	#     #-------------------------------------- --#
	#     # Process New Assets
	#     # Move New assets to Queued status
	#     if len(assets_new) > 0:
	#         log.info('There are ' + str(len(assets_new)) + ' new assets to download.' )
	#         for asset in assets_new:
	#             db_update_asset_status(database,asset[0],status_queued)
	#             log.debug('Moved New asset [' + str(asset[0]) + '] ' + asset[1] + ' to Queue status.')
	#         log.info('There are ' + str(len(assets_new)) + ' new assets moved to download queue.')

	#     #----------------------------------------#
	#     # Process Failed Assets
	#     # Move Failed assets to Queued status
	#     if len(assets_failed) > 0:
	#         log.info('There are ' + str(len(assets_failed)) + ' failed assets to re-download.' )
	#         for asset in assets_failed:
	#             db_update_asset_status(database,asset[0],status_queued)
	#             log.debug('Moved Failed asset [' + str(asset[0]) + '] ' + asset[1] + ' to Queue status.')
	#             filename = os.path.join(storage_path, asset[1])
	#             deleted = delete_asset(filename)
	#             if deleted == True:
	#                 log.info('Asset [' + str(asset[0]) + '] ' + filename + ' deleted.')
	#             elif deleted == False:
	#                 log.error('Asset [' + str(asset[0]) + '] ' + filename + ' not deleted.')
	#         log.info('There are ' + str(len(assets_failed)) + ' failed assets moved to download queue.')        



	### Exit Summary ###

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
	log.info('Assets Downloaded = ' + str(ingest_count))
	log.info('--------------------------------')
	log.info('Completed')
	log.info('Runtime = ' + str(day)+"d:"+str(hour)+"h:"+str(mins)+"m:"+str(secs)+"s, " + duration)
	log.info('--------------------------------')

	print();sys.exit()