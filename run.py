import csv
import psycopg2

def db_connect():
  """
  Connect to dbase and report if not connected
  """
  try:
      conn = psycopg2.connect("dbname='pets' host='localhost' password='dbpass'")
      print "Connect to dbase!"
      return conn
  except:
      print "I am unable to connect to the database"
      quit()

def load_dict(csv_file):
	"""
	Will take .csv file and use header to create a list of dictionaries of each row
	"""
	# file_name = csv_file
	delimiter = ','
	quote_character = '"'

	csv_fp = open(csv_file, 'rb')
	csv_reader = csv.DictReader(csv_fp, fieldnames=[], restkey='undefined-fieldnames', delimiter=delimiter, quotechar=quote_character)

	current_row = 0
	csv_list=[]	
	for row in csv_reader:
		current_row += 1

		# Use heading rows as field names for all other rows.
		if current_row == 1:
			csv_reader.fieldnames = row['undefined-fieldnames']
			continue
		csv_list.append(row)
	return csv_list

def insert_nulls(csv_list_dict):
	"""
	Will take any '' value in this list and set to NULL
	"""
	for item in csv_list_dict:
		for key in item:
			if item[key] == '':
				item[key] = 'NULL'

def in_dbase(cursor_obj):
  pass
  conn = cursor_obj
  cursor = conn.cursor()
#   cursor.execute('SELECT COUNT(*) FROM species WHERE species.name = Cat')  
#     cursor.execute('SELECT * FROM species')  


def main():
  """ Main function """
  conn = db_connect()
  csv_list = load_dict('pets_to_add.csv') # Create list of dictionaries as csv_header[data]
  insert_nulls(csv_list)  # Checks all blank entries in CSV and inserts NULL
  
  """ Check to see if entries in dbase if not add, normalize for caps """
  in_dbase(conn) # conn,'species','dog'
#   in_dbase(conn,'breed')
#   in_dbase(conn,'shelter')


if __name__ == "__main__":
    main()