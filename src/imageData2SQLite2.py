import sqlite3
import os
import time
import logging
from get_image_meta import GetImageMeta

class ImageCache:

	EXTENSIONS = ['.png','.jpg','.jpeg','.heif','.heic']

	def __init__(self, picture_dir='/home/pi/Pictures'):
		self.__picture_dir = picture_dir
		self.__db_file = "pictureframe2.db3"
		self.__db = self.__create_open_db(self.__db_file)

	def update_cache(self):

		t0 = time.time()

		# update the db with info for any added or modified folders since last db refresh
		modified_folders = self.__update_modified_folders()

		# update the db with info for any added or modified files since the last db refresh
		modified_files = self.__update_modified_files(modified_folders)

		# update the meta data for any added or modified files since the last db refresh
		self.__update_meta_data(modified_files)

		# remove any files or folders from the db that are no longer on disk
		self.__purge_missing_files_and_folders()

		t1 = time.time()
		print("Total: ", t1-t0)

		self.__db.commit()

	def query_cache(self, where_clause, sort_clause = 'exif_datetime asc'):
		cursor = self.__db.cursor()
		cursor.row_factory = None # we don't want the "sqlite3.Row" setting from the db here...
		sql = """
			SELECT file_id
			FROM all_data
			WHERE {0}
			ORDER BY {1}
		""".format(where_clause, sort_clause)

		return cursor.execute(sql).fetchall()

	def get_file_info(self, file_id):
		sql = "SELECT * FROM all_data where file_id = {0}".format(file_id)
		result = self.__db.execute(sql).fetchone()
		dict = {}
		for key in result.keys():
			dict[key] = result[key]

		return dict

	def __create_open_db(self, db_file):

		sql_folder_table = """
			CREATE TABLE IF NOT EXISTS folder (
				folder_id INTEGER NOT NULL PRIMARY KEY,
				name TEXT UNIQUE NOT NULL,
				last_modified REAL DEFAULT 0 NOT NULL
			)"""

		sql_file_table = """
			CREATE TABLE IF NOT EXISTS file (
				file_id INTEGER NOT NULL PRIMARY KEY,
				folder_id INTEGER NOT NULL,
				basename  TEXT NOT NULL,
				extension TEXT NOT NULL,
				last_modified REAL DEFAULT 0 NOT NULL,
				UNIQUE(folder_id, basename, extension)
			)"""

		sql_meta_table = """
			CREATE TABLE IF NOT EXISTS meta (
				file_id INTEGER NOT NULL PRIMARY KEY,
				orientation INTEGER DEFAULT 1 NOT NULL,
				exif_datetime REAL DEFAULT 0 NOT NULL,
				f_number REAL DEFAULT 0 NOT NULL,
				exposure_time TEXT,
				iso REAL DEFAULT 0 NOT NULL,
				focal_length TEXT,
				make TEXT,
				model TEXT,
				lens TEXT,
				rating INTEGER,
				latitude TEXT,
				longitude TEXT,
				width INTEGER DEFAULT 0 NOT NULL,
				height INTEGER DEFAULT 0 NOT NULL
			)"""

		# Combine all important data in a single view for easy accesss
		# Although we can't control the layout of the view when using 'meta.*', we want it
		# all and that seems better than enumerating (and maintaining) each column here.
		sql_all_data_view = """
			CREATE VIEW IF NOT EXISTS all_data
			AS
			SELECT
				folder.name	|| "/" || file.basename || "." || file.extension AS file,
				file.last_modified,
				meta.*,
				meta.height > meta.width as is_portrait
			FROM file
				INNER JOIN folder
					ON folder.folder_id = file.folder_id
				LEFT JOIN meta
					ON file.file_id = meta.file_id
			"""

		# trigger to automatically delete file records when associated folder records are deleted
		sql_clean_file_trigger = """
			CREATE TRIGGER IF NOT EXISTS Clean_File_Trigger
			AFTER DELETE ON folder
			FOR EACH ROW
			BEGIN
				DELETE FROM file WHERE folder_id = OLD.folder_id;
			END"""

		# trigger to automatically delete meta records when associated file records are deleted
		sql_clean_meta_trigger = """
			CREATE TRIGGER IF NOT EXISTS Clean_Meta_Trigger
			AFTER DELETE ON file
			FOR EACH ROW
			BEGIN
				DELETE FROM meta WHERE file_id = OLD.file_id;
			END"""

		db = sqlite3.connect(db_file)
		db.row_factory = sqlite3.Row # make results accessible by field name
		for item in (sql_folder_table, sql_file_table, sql_meta_table, sql_all_data_view,
					sql_clean_file_trigger, sql_clean_meta_trigger):
			db.execute(item)

		return db

	def __update_modified_folders(self):
		out_of_date_folders = []
		insert_data = []
		sql_select = "SELECT * FROM folder WHERE name = ?"
		# Note, we must use INSERT OR IGNORE here, as INSERT OR REPLACE will modify
		# the record id upon conflict. Since the id is linked in other tables/views
		# it can't be allowed to change. We'll follow that up with an UPDATE, which
		# should ensure that both new and updated records are handled correctly. Even
		# though this is redundant in some cases, it seems to be the fastest method.
		sql_insert = "INSERT OR IGNORE INTO folder(last_modified, name) VALUES(?, ?)"
		sql_update = "UPDATE folder SET last_modified = ? WHERE name = ?"
		for dir in [d[0] for d in os.walk(self.__picture_dir)]:
			mod_tm = int(os.stat(dir).st_mtime)
			found = self.__db.execute(sql_select, (dir,)).fetchone()
			if not found or found['last_modified'] < mod_tm:
				out_of_date_folders.append(dir)
				insert_data.append([mod_tm, dir])

		if len(insert_data):
			self.__db.executemany(sql_insert, insert_data)
			self.__db.executemany(sql_update, insert_data)

		return out_of_date_folders

	def __update_modified_files(self, modified_folders):
		out_of_date_files = []
		insert_data = []
		# Here, we can get away with INSERT OR REPLACE as a change to the file's db id
		# won't cause problems as the linked records will naturally be updated anyway.
		sql_select = "SELECT file, last_modified FROM all_data WHERE file = ?"
		sql_update = "INSERT OR REPLACE INTO file(folder_id, basename, extension, last_modified) VALUES((SELECT folder_id from folder where name = ?), ?, ?, ?)"
		for dir in modified_folders:
			for file in os.listdir(dir):
				base, extension = os.path.splitext(file)
				if extension.lower() in ImageCache.EXTENSIONS:
					full_file = os.path.join(dir, file)
					mod_tm =  os.path.getmtime(full_file)
					found = self.__db.execute(sql_select, (full_file,)).fetchone()
					if not found or found['last_modified'] < mod_tm:
						out_of_date_files.append(full_file)
						insert_data.append([dir, base, extension.lstrip("."), mod_tm])

		if len(insert_data):
			self.__db.executemany(sql_update, insert_data)

		return out_of_date_files

	def __get_meta_sql_from_dict(self, dict):
		columns = ', '.join(dict.keys())
		ques = ', '.join('?' * len(dict.keys()))
		return 'INSERT OR REPLACE INTO meta(file_id, {0}) VALUES((SELECT file_id from all_data where file = ?), {1})'.format(columns, ques)

	def __update_meta_data(self, modified_files):
		sql_insert = None
		insert_data = []
		for file in modified_files:
			meta = self.__get_exif_info(file)
			if sql_insert == None:
				sql_insert = self.__get_meta_sql_from_dict(meta)
			vals = list(meta.values())
			vals.insert(0, file)
			insert_data.append(vals)

		if len(insert_data):
			self.__db.executemany(sql_insert, insert_data)

	def __purge_missing_files_and_folders(self):
		# Find folders in the db that are no longer on disk
		folder_id_list = []
		for row in self.__db.execute('SELECT folder_id, name from folder'):
			if not os.path.exists(row['name']):
				folder_id_list.append([row['folder_id']])

		# Delete any non-existent folders from the db. Note, this will automatically
		# remove orphaned records from the 'file' and 'meta' tables
		if len(folder_id_list):
			self.__db.executemany('DELETE FROM folder WHERE folder_id = ?', folder_id_list)

		# Find files in the db that are no longer on disk
		file_id_list = []
		for row in self.__db.execute('SELECT file_id, file from all_data'):
			if not os.path.exists(row['file']):
				file_id_list.append([row['file_id']])

		# Delete any non-existent files from the db. Note, this will automatically
		# remove matching records from the 'meta' table as well.
		if len(file_id_list):
			self.__db.executemany('DELETE FROM file WHERE file_id = ?', file_id_list)

	def __get_exif_info(self, file_path_name):
		exifs = GetImageMeta(file_path_name)
		# Dict to store interesting EXIF data
		# Note, the 'key' must match a field in the 'meta' table
		e = {}

		e['orientation'] = exifs.get_orientation()

		width, height = exifs.get_size()
		if e['orientation'] in (5, 6, 7, 8):
			width, height = height, width # swap values
		e['width'] = width
		e['height'] = height


		e['f_number'] = exifs.get_exif('EXIF FNumber')
		e['make'] = exifs.get_exif('EXIF Make')
		e['model'] = exifs.get_exif('EXIF Model')
		e['exposure_time'] = exifs.get_exif('EXIF ExposureTime')
		e['iso'] =  exifs.get_exif('EXIF ISOSpeedRatings')
		e['focal_length'] =  exifs.get_exif('EXIF FocalLength')
		e['rating'] = exifs.get_exif('EXIF Rating')
		e['lens'] = exifs.get_exif('EXIF LensModel')
		val = exifs.get_exif('EXIF DateTimeOriginal')
		if val != None:
			e['exif_datetime'] = time.mktime(time.strptime(val, '%Y:%m:%d %H:%M:%S'))
		else:
			e['exif_datetime'] = os.path.getmtime(file_path_name)

		gps = exifs.get_location()
		lat = gps['latitude']
		lon = gps['longitude']
		e['latitude'] = round(lat, 4) if lat else lat
		e['longitude'] = round(lon, 4) if lon else lon

		return e

# If being executed (instead of imported), kick it off...
if __name__ == "__main__":
	cache = ImageCache(picture_dir='/home/pi/Pictures')
	cache.update_cache()
	# items = cache.query_cache("make like '%google%'", "exif_datetime asc")
	# info = cache.get_file_info(12)