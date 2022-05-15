import mysql.connector
import json
from flask import Flask, request, render_template

app = Flask(__name__)

# Helper Functions

def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False


# Routes

@app.route('/')
def hello_world():
  return 'Hello, Docker!'

@app.route('/parcels')
def get_parcels():
  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()


  cursor.execute("SELECT * FROM parcels")

  row_headers=[x[0] for x in cursor.description] #this will extract row headers

  results = cursor.fetchall()
#  json_data=[]
#  for result in results:
#    json_data.append(dict(zip(row_headers,result)))
#
#  cursor.close()
#
#  return json.dumps(json_data)
  result_string = ' '.join([str(item) for item in row_headers]) + '<br>'
  for row in results:
    result_string += ' '.join([str(item) for item in row]) + '<br>'
  
  return result_string

@app.route('/initdb')
def db_init():
  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret"
  )
  cursor = mydb.cursor()

  cursor.execute("DROP DATABASE IF EXISTS inventory")
  cursor.execute("CREATE DATABASE inventory")
  cursor.close()

  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()

  cursor.execute("DROP TABLE IF EXISTS parcels")
  cursor.execute("CREATE TABLE parcels (parcel_id VARCHAR(255), first_name VARCHAR(255), last_name VARCHAR(255))")
  cursor.close()

  return 'init database'


@app.route('/addtest2')
def add_test2():
  return render_template('text-input.html')
  
@app.route('/addtest2', methods=['POST'])
def add_test2_post():
  parcel_id = request.form['parcel_id']
  first_name = request.form['first_name']
  last_name = request.form['last_name']

  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()

  if not checkTableExists(mydb, "parcels"):
      return f'ERROR: table "parcels" does not exist!'

  sql_cmd = f'INSERT INTO parcels (parcel_id, first_name, last_name) VALUES ("{parcel_id}", "{first_name}", "{last_name}")'

  cursor.execute(sql_cmd)

  mydb.commit()

  cursor.close()

  return f'added parcel_id: {parcel_id} FirstName:{first_name} LastName: {last_name}'

@app.route('/addtest')
def add_test():
  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()

  if not checkTableExists(mydb, "parcels"):
      return f'ERROR: table "parcels" does not exist!'

  parcel_id = "99.37.115923.22371774"
  first_name = "Johnny"
  last_name = "DropTables"

  sql_cmd = f'INSERT INTO parcels (parcel_id, first_name, last_name) VALUES ("{parcel_id}", "{first_name}", "{last_name}")'

  cursor.execute(sql_cmd)

  mydb.commit()

  cursor.close()

  return f'added parcel_id: {parcel_id} FirstName:{first_name} LastName: {last_name}'

if __name__ == "__main__":
  app.run(host ='0.0.0.0')