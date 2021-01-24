import sqlite3
import os
import time
from PIL import Image

def refresh_db():
	picture_dir = "/home/pi/Pictures"
	db_file = "pictureframe.db3"

	# create the db file if it doesn't yet exist
	db = create_open_db(db_file)

	# update the db info with for any added or modified folders since last db refresh
	modified_folders = update_modified_folders(db, picture_dir)

	# update the db with info for any added or modified files since the last db refresh
	modified_files = update_modified_files(db, modified_folders)

	# update the exif data for any added or modified files since the last db refresh
	update_exif_info(db, modified_files)

	# remove any files or folders from the db that are no longer on disk
	remove_missing_files_and_folders(db, picture_dir)

def create_open_db(db_file):

	sql_folder_table = """
		CREATE TABLE IF NOT EXISTS folder (
			id INTEGER NOT NULL PRIMARY KEY,
			name TEXT UNIQUE NOT NULL,
			last_modified REAL DEFAULT 0 NOT NULL
		)"""

	sql_file_table = """
		CREATE TABLE IF NOT EXISTS file (
			id INTEGER NOT NULL PRIMARY KEY,
			folder TEXT NOT NULL,
			name TEXT NOT NULL,
			type TEXT NOT NULL,
			orientation INTEGER DEFAULT 1 NOT NULL,
			last_modified REAL DEFAULT 0 NOT NULL,
			exif_datetime REAL DEFAULT 0 NOT NULL,
			location TEXT,
			width INTEGER DEFAULT 0 NOT NULL,
			height INTEGER DEFAULT 0 NOT NULL,
			UNIQUE(folder, name)
		)"""

	db = sqlite3.connect(db_file)
	db.row_factory = sqlite3.Row
	db.execute(sql_folder_table)
	db.execute(sql_file_table)

	return db

def update_modified_folders(db, picture_dir):
	out_of_date_folders = []
	insert_data = []
	sql_select = "SELECT * FROM folder WHERE name = ?"
	sql_update = "INSERT OR REPLACE INTO folder(name, last_modified) VALUES(?, ?)"
	for dirpath, dirnames, filenames in os.walk(picture_dir):
		mod_tm = int(os.stat(dirpath).st_mtime)
		found = db.execute(sql_select, (dirpath,)).fetchone()
		if not found or found['last_modified'] < mod_tm:
			out_of_date_folders.append(dirpath)
			insert_data.append([dirpath, mod_tm])

	if len(insert_data):
		db.executemany(sql_update, insert_data)
		db.commit()

	return out_of_date_folders

def update_modified_files(db, modified_folders):
	out_of_date_files = []
	insert_data = []
	sql_select = "SELECT folder, name, last_modified FROM file WHERE folder = ? AND name = ?"
	sql_update = "INSERT OR REPLACE INTO file(folder, name, type, last_modified) VALUES(?, ?, ?, ?)"
	extensions = ['.png','.jpg','.jpeg','.heif','.heic']
	for dir in modified_folders:
		for file in os.listdir(dir):
			base, extension = os.path.splitext(file)
			if extension.lower() in extensions:
				type = extension[1:]
				full_file = os.path.join(dir, file)
				mod_tm =  os.path.getmtime(full_file)
				found = db.execute(sql_select, (dir, file)).fetchone()
				if not found or found['last_modified'] < mod_tm:
					out_of_date_files.append(full_file)
					insert_data.append([dir, file, type, mod_tm])

	if len(insert_data):
		db.executemany(sql_update, insert_data)
		db.commit()

	return out_of_date_files

def update_exif_info(db, modified_files):
	sql_update = "UPDATE file SET orientation = ?, exif_datetime = ?, width = ?, height = ? WHERE folder = ? AND name = ?"
	insert_data = []
	for file in modified_files:
		(orientation, date, width, height) = get_exif_info(file)
		dir = os.path.dirname(file)
		base = os.path.basename(file)
		insert_data.append([orientation, date, width, height, dir, base])

	if len(insert_data):
		db.executemany(sql_update, insert_data)
		db.commit()

def remove_missing_files_and_folders(db, picture_dir):
	files = []
	folders = []
	# Get a list of all files and folders in the defined picture folder
	for (dirpath, dirnames, filenames) in os.walk(picture_dir):
		for f in filenames:
			files.append(os.path.join(dirpath, f))
		for d in dirnames:
			folders.append(os.path.join(dirpath, d))

	# Find files in the db that are no longer on disk
	file_id_list = []
	cur = db.cursor()
	cur.execute('SELECT id, folder, name from file')
	for row in cur:
		file = os.path.join(row['folder'], row['name'])
		if not os.path.exists(file):
			file_id_list.append([row['id']])

	# Find folders in the db that are no longer on disk
	folder_id_list = []
	cur.execute('SELECT id, name from folder')
	for row in cur:
		if not os.path.exists(row['name']):
			folder_id_list.append([row['id']])

	# Delete any non-existent files from the db
	if len(file_id_list):
		db.executemany('DELETE FROM file WHERE id = ?', file_id_list)

	# Delete any non-existent folders from the db
	if len(folder_id_list):
		db.executemany('DELETE FROM folder WHERE id = ?', folder_id_list)

	db.commit()

def get_exif_info(file_path_name):
	EXIF_DATETIME = 36867
	EXIF_ORIENTATION = 274
	dt = os.path.getmtime(file_path_name) # so use file last modified date
	orientation = 1
	width = 0
	height = 0
	try:
		im = Image.open(file_path_name) # lazy operation so shouldn't load (better test though)
		width = im.width
		height = im.height
		exif_data = im._getexif() # TODO check if/when this becomes proper function
		if EXIF_DATETIME in exif_data:
			exif_dt = time.strptime(exif_data[EXIF_DATETIME], '%Y:%m:%d %H:%M:%S')
			dt = time.mktime(exif_dt)
		if EXIF_ORIENTATION in exif_data:
			orientation = int(exif_data[EXIF_ORIENTATION])
			if orientation < 1 or orientation > 8:
				orientation = 1
			if orientation in [5, 6, 7, 8]:
				width, height = height, width # swap values
	except Exception as e: # NB should really check error here but it's almost certainly due to lack of exif data
		#print('trying to read exif', e)
		pass

	return (orientation, dt, width, height)

refresh_db()