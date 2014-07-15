import csv

def load_dict(dict_csv):
	"""
	Will take .csv file and use header to create a list of dictionaries of each row
	"""
	file_name = dict_csv
	delimiter = ','
	quote_character = '"'

	csv_fp = open(file_name, 'rb')
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


def main():
	""" Main function """
	csv_list = load_dict('pets_to_add.csv')

	insert_nulls(csv_list)


if __name__ == "__main__":
    main()