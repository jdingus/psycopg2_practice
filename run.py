import csv
import psycopg2
import psycopg2.extras
import sys

def db_connect():
  """
  Connect to dbase and report if not connected
  """
  try:
      conn = None
      conn = psycopg2.connect("dbname='pets'")  # user='jdingus'")
      print "Connected to pets dbase!"

      return conn
  except:
      print "I am unable to connect to the database"
      quit()

def load_dict(csv_file):
    """
    Will take .csv file and use header to create a list of dictionaries of each row
    """
    delimiter = ','
    quote_character = '"'

    csv_fp = open(csv_file, 'rb')
    csv_reader = csv.DictReader(csv_fp, fieldnames=[], 
      restkey='undefined-fieldnames', delimiter=delimiter,
      quotechar=quote_character)

    current_row = 0
    csv_list=[] 
    for row in csv_reader:
        current_row += 1

        # Use heading rows as field names for all other rows.
        if current_row == 1:
            csv_reader.fieldnames = row['undefined-fieldnames']
            continue
        csv_list.append(row)

    insert_nulls(csv_list)  # Checks all blank entries in CSV and inserts NULL
    
    csv_list[0] = strip_dict(csv_list[0])
    
    # print csv_list[0]
    
    return csv_list

def insert_nulls(csv_list_dict):
    """
    Will take any '' value in this list and set to NULL
    """
    for item in csv_list_dict:
        for key in item:
            if item[key] == '':
                item[key] = None

def strip_dict(d):
    return { key : strip_dict(value)
             if isinstance(value, dict)
             else value.strip()
             for key, value in d.items() }

def in_dbase(cursor_obj,table_name,col_name,find_string):
  """
  Checks to see if a find_string entry is in passed table_name and col_name
  If present will return 1, not present will return 0, Note this comparison is case insensitive
  """
  if find_string == None:
    return False # Don't add a None field to the dbase
  find_string = "'" + find_string + "'"
  conn = cursor_obj
  cursor = conn.cursor()
  cursor.execute(
    'SELECT * FROM %s WHERE %s ilike %s;' % (table_name, col_name, find_string)) 
  cursor_list = []
  cursor_list = cursor.fetchall()
  if len(cursor_list) > 0:
    return True
  else: return False

def insert_line_csv(cursor_obj,list_insert):
	conn = cursor_obj
	cursor = conn.cursor()
	SQL = "INSERT INTO pet (name,age,adopted) VALUES (%s);"
	data = list_insert
	cursor.execute(SQL,data)
	conn.commit()
	return
  
  
def csv_insert_db(cursor_obj,csv_list_dict):
  """
  Loop Thru each csv_list of dicts, check if species,breed,shelter in pets db; 
  if not create db entry in respective tables, Then add each csv entry into db
  """
  conn = cursor_obj
  cursor = conn.cursor()

  for dicts in csv_list_dict:
    """ Each dict check to see if breed, species, or shelter already in dbase"""
    breed_ck = dicts['breed name']
    breed_ck = "'" + breed_ck + "'"
    species_ck = dicts['species name']
    species_ck = "'" + species_ck + "'"
    shelter_ck = dicts['shelter name']
    shelter_ck = "'" + shelter_ck + "'"
    print dicts['breed name']
    if in_dbase(conn, 'breed', 'breed.name', breed_ck) == 0:
      insert_breed(conn,'breed','name',breed_ck)

def insert_record(cursor_obj,table_name,col_name,in_string):
	conn = cursor_obj
	cursor = conn.cursor()

	if table_name == 'shelter':
		SQL = "INSERT INTO shelter (name) VALUES (%s);"
		data = (in_string, )

	if table_name == 'breed':
		SQL = "INSERT INTO breed (name) VALUES (%s);"
		data = (in_string, )

	if table_name == 'species':
		SQL = "INSERT INTO species (name) VALUES (%s);"
		data = (in_string, )

	if table_name == 'pet':
		if col_name == 'name':
			SQL = "INSERT INTO pet (name) VALUES (%s);"
			data = (in_string, )
			print 'found one!'
		if col_name == 'age':
			SQL = "INSERT INTO pet (age) VALUES (%s);"
			data = (in_string, )
			print 'found one!'
		if col_name == 'adopted':
			SQL = "INSERT INTO pet (adopted) VALUES (%s);"
			data = (in_string, )
			print 'found one!'

	cursor.execute(SQL,data)
	conn.commit()
	return

def main():
	""" Initialize pets dbase """
	import os
	os.system("psql -d pets < create_pets.sql")

	""" Connect to pets dabase """
	conn = db_connect()
	""" Create list of dicts with csv header as keys in dicts
	"""
	csv_list = load_dict('pets_to_add.csv') 

	print csv_list

	""" Add species, breed or shelter if not already in dbase """
	for item in csv_list:
		for key in item:
			if key=='shelter name' and item[key] != None:
				if not in_dbase(conn,'shelter','name',item[key]):
					print "I need to add this shelter name!",item[key] 
					insert_record(conn,'shelter','name',item[key])

			if key=='breed name' and item[key] != None:
				if not in_dbase(conn,'breed','name',item[key]):
					print "I need to add this breed name!",item[key] 
					insert_record(conn,'breed','name',item[key])

			if key=='species name' and item[key] != None:
				if not in_dbase(conn,'species','name',item[key]):
					print "I need to add this species name!",item[key] 
					insert_record(conn,'species','name',item[key]) 	

	""" Add all entries from csv file into dbase
	Name,age,breed name,species name,shelter name,adopted
	"""
	print type(csv_list)
  
# 	for item in csv_list:
# 		insert_line_csv(conn,item)

    


if __name__ == "__main__":
    main()