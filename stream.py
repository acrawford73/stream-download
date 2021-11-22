#!/usr/bin/python
# Author: Anthony Crawford
# Target Operating System: RedHat/CentOS 6/7
# Python Version: 2.x
# Package: pycurl (https://pypi.org/project/pycurl/)
# sudo apt install python-pycurl
# Purpose: Based on a list of assets, download video content.
# -----------------------------------------------------------------------------
#
### Packages
import os
import re
import sys
import errno
import datetime,time
from time import strftime
from distutils.util import strtobool
from random import randint
import shutil
# Logging
import logging
import logging.config
import logging.handlers
# Configuration
from ConfigParser import SafeConfigParser
# Third-Party
import sqlite3
import getopt
import pycurl

### Load Configuration Parameters
config = SafeConfigParser()
config.read('etc/config.conf')
#
debug = strtobool(config.get('tool', 'debug_enabled'))
database = config.get('tool', 'database')
queue_limit = int(config.get('tool', 'queue_limit'))
storage_path = config.get('tool', 'storage_path')
http_timeout = int(config.get('tool', 'http_timeout'))

ingest_count = 0
ingesting = False

#-----------------------------------------------------------------------#
# Functions
#-----------------------------------------------------------------------#
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
		print;print('Database ' + database + ' missing, creating new database.')
		conn = sqlite3.connect(database)
		c = conn.cursor()
		c.execute(sql)
		conn.commit()
		conn.close()
		print;print('Database ' + database + ' created.')
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
				asset = (line.split('/')[-1]).strip();print "[" + str(asset_count+1) + "] " + asset
				asset_uri = line.strip();print asset_uri
				c.execute("INSERT INTO assets (asset,asset_uri) \
					VALUES (?,?)", (asset,asset_uri))
				asset_count+=1
	f.close()
	conn.commit()
	conn.close()
	print;print('There were '+str(asset_count)+' assets imported from file ' + inputfile + '.');print


def get_inventory_print(database):
	print;print('--------------------------------')
	print('Assets Database:')

	assets_new = []
	assets_queued = []
	assets_active = []
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
	# Active
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=2")
	assets_active = c.fetchall()
	print('Active    = ' + str(len(assets_active)))
	# Completed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=3")
	assets_completed = c.fetchall()
	print('Completed = ' + str(len(assets_completed)))
	# Failed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=5")
	assets_failed = c.fetchall()
	print('Failed    = ' + str(len(assets_failed)))

	conn.close()
	print('--------------------------------')
#	return [assets_new, assets_queued, assets_active, assets_completed, assets_failed]


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


def db_get_inventory_log(database):
	log.info('--------------------------------')
	log.info('Assets Database:')

	assets_new = []
	assets_queued = []
	assets_active = []
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
	# Active
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=2")
	assets_active = c.fetchall()
	log.info('Active    = ' + str(len(assets_active)))
	# Completed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=3")
	assets_completed = c.fetchall()
	log.info('Completed = ' + str(len(assets_completed)))
	# Failed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=5")
	assets_failed = c.fetchall()
	log.info('Failed    = ' + str(len(assets_failed)))

	conn.close()
	log.info('--------------------------------')
	return [assets_new, assets_queued, assets_active, assets_completed, assets_failed]


def db_get_inventory(database):
	assets_new = []
	assets_queued = []
	assets_active = []
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
	# Active
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=2")
	assets_active = c.fetchall()
	# Completed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=3")
	assets_completed = c.fetchall()
	# Failed
	c = conn.cursor()
	c.execute("SELECT * FROM assets WHERE status=5")
	assets_failed = c.fetchall()

	conn.close()
	return [assets_new, assets_queued, assets_active, assets_completed, assets_failed]


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
	print;print('Database ' + database + ' purged.');print


# Download asset from target
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
	# these keepalive options are not available on CentOS.
	# these options are kept here for reference.
	#c.setopt(c.TCP_KEEPALIVE, True)
	#c.setopt(c.TCP_KEEPINTVL, 30L)
	#c.setopt(c.TCP_KEEPIDLE, 120L)
	c.setopt(c.FOLLOWLOCATION, False)
	c.setopt(c.CONNECTTIMEOUT, 10L)
	# The connection is dropped if the asset isn't downloaded within the c.TIMEOUT window.
	#c.setopt(c.TIMEOUT, 60L)  # DO NOT set this timeout! 
	c.setopt(c.NOSIGNAL, True)
	#c.setopt(c.FORBID_REUSE, True)  # Disabled, it will reuse same TCP socket
	c.setopt(c.FAILONERROR, True)
	c.setopt(c.HTTPHEADER, headers)
	c.setopt(c.NOPROGRESS, False)
	if debug == True:
		c.setopt(c.VERBOSE, True)
	c.setopt(c.URL, url)
	filename = url.split('/')[-1]
	local_filename = os.path.join(storage_path, filename)
	with open(local_filename, 'wb') as f:
		log.info("Downloading: ["+str(ingest_count)+"/"+str(assets_total)+"] " + filename)
		c.setopt(c.WRITEFUNCTION, f.write)
		try:
			c.perform()
		except pycurl.error as e:
			#print('Status Code: %d' % c.getinfo(c.RESPONSE_CODE))
			if e.args[0] == pycurl.E_COULDNT_CONNECT and c.exception:
				log.error(c.exception)
			else:
				log.error(e)
			f.close()
			c.close()	
			return False
		log.info('Asset download  ' + filename + ' completed in %0.3f seconds' % c.getinfo(c.TOTAL_TIME))
	f.close()
	c.close()
	return True

def print_assets(assets):
	for asset in assets:
		print "[" + str(asset[0]) + "] " + asset[1] + " | " + asset[2] + " | " + str(asset[3])

def print_help():
	print
	print "Usage:"
	print "./stream.py -i <input-file>, where -i means 'ingest'. Imports the assets CSV file into the tool database."
	print "./stream.py -p, where -p means 'purge'. Purges all assets from the assets database."
	print "./stream.py -d, where -d means 'delete'. Deletes Completed and Failed assets from the tool database and NAS directory."
	print "./stream.py -l, where -l means 'list'. Prints all assets in the tool database."
	print "./stream.py -s <output-file>, where -s means 'stream'. Combines all video files into single transport stream."
	print "./stream.py -h, where -h means 'help'. Prints this help information."
	print "./stream.py, runs the ingest script."
	print

#-----------------------------------------------------------------------#
# End of Functions
#-----------------------------------------------------------------------#


### Start of script ###

#----------------------------------------#
# CLI Command Options
#----------------------------------------#
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
			print;print('There are ' + str(len(assets_completed)) + ' completed assets that will be deleted.')
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
			print;print('There are ' + str(len(assets_failed)) + ' failed assets that will be deleted.')
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
			print;print "There are no assets ready to be deleted."
		print;sys.exit()

	# List
	elif opt == '-l':
		inventory = db_get_inventory(database)
		assets_new = inventory[0]
		if len(assets_new) > 0:
			print;print "New Assets = " + str(len(assets_new))
			print_assets(assets_new)
		assets_queued = inventory[1]
		if len(assets_queued) > 0:
			print;print "Queued Assets = " + str(len(assets_queued))
			print_assets(assets_queued)
		assets_active = inventory[2]
		if len(assets_active) > 0:
			print;print "Active Assets = " + str(len(assets_active))
			print_assets(assets_active)
		assets_completed = inventory[3]
		if len(assets_completed) > 0:
			print;print "Completed Assets = " + str(len(assets_completed))
			print_assets(assets_completed)
		assets_failed = inventory[4]
		if len(assets_failed) > 0:
			print;print "Failed Assets = " + str(len(assets_failed))
			print_assets(assets_failed)
		if (len(assets_new) == 0) and (len(assets_queued) == 0) and (len(assets_active) == 0) and (len(assets_completed) == 0) and (len(assets_failed) == 0):
			print;print("The database contains no assets.")
		get_inventory_print(database)
		print;sys.exit()
	
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
			print;print("Combining all *.ts files into single stream file " + output_file + "...")
			with open(output_file, 'wb') as fp:
				fp.write(out_data)
			fp.close()
			print;print('Done.')
		else:
			print;print("Stream filename not specified.")
		print;sys.exit()

	# Import assets list file or HLS playlist
	elif opt in ("-i", "--ifile"):
		inputfile = arg

#----------------------------------------#
# Run asset importer
#----------------------------------------#
print
if inputfile:
	if file_check_exists(inputfile):
		if db_check_exists(database):
			print;print('Importing assets file ' + inputfile + ' to database ' + database + '...');print
			db_asset_importer(database,inputfile)
			#db_get_inventory_log(database)
			sys.exit()
		else:
			print;print("Database file doesn't exist.");print
			sys.exit()
	else:
		print;print("Import file doesn't exist.");print
		sys.exit()


#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
# Bussit!  Start of ./stream.py ingest with logging
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

stream_start_time = time.time()

#----------------------------------------#
# Initialize Logging
#----------------------------------------#
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

# if debug:
# 	logging.getLogger('urllib3').setLevel(logging.DEBUG)

# Create storage path if it doesn't exist
make_sure_path_exists(storage_path)

print
log.info('--------------------------------')
log.info('Stream Downloader Tool')
#log.info('--------------------------------')


#----------------------------------------#
# Initialize Asset Inventory from DB
#----------------------------------------#
# [New, Queued, Active, Completed, Not Found, Failed]
# Ingest Status:
# 0 New
# 1 Queued
# 2 Active
# 3 Completed
# 4 Not Found
# 5 Failed
status_new = 0
status_queued = 1
status_active = 2
status_completed = 3
status_failed = 5

#----------------------------------------#
if db_check_exists(database):
	inventory = db_get_inventory_log(database)
	assets_new = inventory[0]
	assets_total = len(assets_new)
	assets_queued = inventory[1]
	assets_active = inventory[2]
	assets_completed = inventory[3]
	assets_failed = inventory[4]

# Determine if we need to continue processing assets from the last time the script was run
if (len(assets_new) > 0) or (len(assets_queued) > 0) or (len(assets_active) > 0) or (len(assets_failed) > 0):
	log.info('... Ingesting ...')
	ingesting = True
else:
	log.info('There are no assets available to ingest. Exiting...')


#----------------------------------------#
# Main Ingest Processing Loop
#----------------------------------------#

while ingesting:

	#----------------------------------------#
	# Exit if no assets available or ingest completed
	if (len(assets_new) == 0) and (len(assets_queued) == 0) and (len(assets_active) == 0) and (len(assets_failed) == 0):
		ingesting = False
		log.info('There are no assets ready to ingest, or ingest has completed. Exiting...')
		db_get_inventory_log(database)

	# Ingest continues ...
	else:
		# Wait a bit before downloading next file
		#time.sleep(ingest_interval)

		# Get current asset inventory for processing
		inventory = db_get_inventory(database)
		assets_new = inventory[0]
		assets_queued = inventory[1]
		assets_active = inventory[2]
		assets_completed = inventory[3]
		assets_failed = inventory[4]

		log.debug('----------------')
		log.debug('Asset Inventory:')
		for asset in assets_new:
			log.debug(asset)
		log.debug('New       = ' + str(len(assets_new)))
		for asset in assets_queued:
			log.debug(asset)
		log.debug('Queued    = ' + str(len(assets_queued)))
		for asset in assets_active:
			log.debug(asset)
		log.debug('Active    = ' + str(len(assets_active)))
		for asset in assets_completed:
			log.debug(asset)
		log.debug('Completed = ' + str(len(assets_completed)))
		for asset in assets_failed:
			log.debug(asset)
		log.debug('Failed    = ' + str(len(assets_failed)))


	#----------------------------------------#
	# Process Queued Assets
	# Check if the asset is ingested already
	# Move Queued assets to Active status based on available active spot availability based on queue limit
	# Upgrade Asset to Active status if createCBRasset call was successful
	if len(assets_queued) > 0:

		# check if active queue has an empty spot, if yes move queued asset(s) to active
		if len(assets_active) < queue_limit:

			active_slots = queue_limit - len(assets_active)
			count = 0

			for asset in assets_queued:
				if active_slots > 0:
					downloaded = False

					# slack off a bit before each asset download
					time.sleep(0.05)

					# Download the asset file if not downloaded already
					# Allows resume from last downloaded file
					if not file_check_exists(os.path.join('video/', asset[1])):
						downloaded = download_target(asset[2],ingest_count+1,assets_total)
					else:
						continue

					# Update database only if download was successful.
					if downloaded == True:
						db_update_asset_status(database,asset[0],status_completed)
						log.debug('Asset [' + str(asset[0]) + '] ' + asset[1] + ' was successfully downloaded.')
						count+=1
						active_slots-=1	
						ingest_count+=1
					else:
						db_update_asset_status(database,asset[0],status_failed)
						log.error('Failed to download asset [' + str(asset[0]) + '] ' + asset[1])
						csvfn_errors = log_file.rsplit('.',1)[0] + '_failed.csv'
						csvfile_errors = os.path.join(log_path, csvfn_errors)
						if os.path.exists(csvfile_errors) == False:
							with open(csvfile_errors, 'w') as f:
								f.write('asset,error\n')
							f.close()
						csv_asset_failed(asset[1],csvfile_errors,"Failed to download asset from CDN")


	#--------------------------------------	--#
	# Process New Assets
	# Move New assets to Queued status
	if len(assets_new) > 0:
		log.info('There are ' + str(len(assets_new)) + ' new assets to ingest.' )
		count = 0
		for asset in assets_new:
			db_update_asset_status(database,asset[0],status_queued)
			log.debug('Moved New asset [' + str(asset[0]) + '] ' + asset[1] + ' to Queue status.')
			count+=1
		log.info('There are ' + str(count) + ' new assets moved to download queue.')

	#----------------------------------------#
	# Process Failed Assets
	# Move Failed assets to Queued status
	if len(assets_failed) > 0:
		log.info('There are ' + str(len(assets_new)) + ' failed assets to reingest.' )
		count = 0
		for asset in assets_failed:
			db_update_asset_status(database,asset[0],status_queued)
			log.info('Moved failed asset [' + str(asset[0]) + '] ' + asset[1] + ' to download queue.')
			count+=1
			filename = os.path.join(storage_path, asset[1])
			deleted = delete_asset(filename)
			if deleted == True:
				log.info('Asset [' + str(asset[0]) + '] ' + filename + ' deleted.')
			elif deleted == False:
				log.error('Asset [' + str(asset[0]) + '] ' + filename + ' not deleted.')
		log.info('There are ' + str(count) + ' failed assets moved to download queue.')		


#----------------------------------------#
# Exit Summary
log.info('Assets Ingested = ' + str(ingest_count))
log.info('--------------------------------')
log.info('Completed')

dur = time.time() - stream_start_time
pos = abs( int(dur) )
day = pos / (3600*24)
rem = pos % (3600*24)
hour = rem / 3600
rem = rem % 3600
mins = rem / 60
secs = rem % 60

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
print;sys.exit()
