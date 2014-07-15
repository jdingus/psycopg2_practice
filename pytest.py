#!/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#

import psycopg2

try:
    conn = psycopg2.connect("dbname='pets' host='localhost' password='dbpass'")
except:
    print "I am unable to connect to the database"
    quit()
cur = conn.cursor()
cur.execute("""SELECT * FROM species""")
print cur.fetchall()
cur.execute('SELECT version()')
ver = cur.fetchone()
print ver
cur.execute("""SELECT name,species_id from breed""")
rows = cur.fetchall()
for row in rows:
  print "    ", row[0]

import psycopg2.extras  
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
try:
    cur.execute("""SELECT * from shelter;""")
except:
    print "I can't SELECT from pet"

#
# Note that below we are accessing the row via the column name.

rows = cur.fetchall()
for row in rows:
    print "   ", row['name']
  


#     conn = psycopg2.connect("dbname='pets' user='dbuser' host='localhost' password='dbpass'")