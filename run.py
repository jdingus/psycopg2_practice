import csv
import psycopg2

def db_connect():
  """
  Connect to dbase and report if not connected
  """
  try:
      conn = None
      conn = psycopg2.connect("dbname='pets' user='jdingus'")
      print "Connected to pets dbase!"
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
    print csv_list
    for item in csv_list:
      for k, v in item.iteritems():
        item[k] = v.lstrip()
    print csv_list
    return csv_list

def insert_nulls(csv_list_dict):
    """
    Will take any '' value in this list and set to NULL
    """
    for item in csv_list_dict:
        for key in item:
            if item[key] == '':
                item[key] = 'NULL'

def in_dbase(cursor_obj,table_name,col_name,find_string):
  """
  Checks to see if a find_string entry is in passed table_name and col_name
  If present will return 1, not present will return 0, Note this comparison is case insensitive
  """
  conn = cursor_obj
  cursor = conn.cursor()
  # cursor.execute('SELECT * FROM %s;' % (table_name))
  # print cursor.fetchall()
  cursor.execute(
    'SELECT * FROM %s WHERE %s ilike %s;' % (table_name, col_name, find_string)) 
  cursor_list = []
  cursor_list = cursor.fetchall()
  if len(cursor_list) > 0:
    return 1
  else: return 0

def csv_insert_db(cursor_obj,csv_list_dict):
  """
  Loop Thru each csv_list of dicts, check if species,breed,shelter in pets db; if not create db entry in respective tables,
  Then add each csv entry into db
  """
  conn = cursor_obj
  cursor = conn.cursor()

  for dicts in csv_list_dict:
    ck_species = dicts[" species name"]
    print ck_species
    # for key in dicts:
    #   if item[key] == '':
    #     item[key] = 'NULL'





def main():
  """ Main function """
  conn = db_connect() # Connect to pets dbase
  csv_list = load_dict('pets_to_add.csv') # Create list of dictionaries with csv header as keys in dicts
  insert_nulls(csv_list)  # Checks all blank entries in CSV and inserts NULL
  

  csv_insert_db(conn,csv_list)


  # """ Check to see if entries in dbase if not add, normalize for caps """
  print in_dbase(conn,'species','species.name',"' DOG'")  # Make sure to ESCAPE find_string >> "'string'"
  # print in_dbase(conn,'breed','breed.name',"'Mixed'")




if __name__ == "__main__":
    main()