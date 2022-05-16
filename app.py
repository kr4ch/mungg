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
  return render_template('index.html')

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
  result_string = '<h1>Parcel Overview</h1>'
  result_string += '<table><tr>'+' '.join(['<th>'+str(item)+'</th>' for item in row_headers]) + '</tr><br>'
  for row in results:
    result_string += '<tr>'+' '.join(['<td>'+str(item)+'</td>' for item in row]) + f'<td><a href="edit?{row[0]}">Edit</a></td></tr><br>'
  result_string += '</table><br><br><a href="/">Back to start</a>'
  
  return result_string


@app.route('/initdb')
def db_init():
  """
  Initialize database.
  Creates tables and all their columns.
  If the db already existed, it will be dropped!
  """
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

  create_table = "CREATE TABLE parcels  (parcel_id VARCHAR(255), " \
                                        "first_name VARCHAR(255), " \
                                        "last_name VARCHAR(255), " \
                                        "einheit_id VARCHAR(255), "  \
                                        "shelf_proposed SMALLINT UNSIGNED," \
                                        "shelf_selected SMALLINT UNSIGNED, " \
                                        "width_cm TINYINT UNSIGNED, " \
                                        "length_cm TINYINT UNSIGNED, " \
                                        "height_cm TINYINT UNSIGNED, " \
                                        "weight_g SMALLINT UNSIGNED)"
  print(create_table)

  cursor.execute("DROP TABLE IF EXISTS parcels")
  cursor.execute(create_table)
  cursor.close()

  return 'init database'


@app.route('/newparcel')
def new_parcel():
  return render_template('new-parcel.html')
  
@app.route('/newparcel', methods=['POST'])
def new_parcel_post():
  # Variable        gets data from form                 or uses default value if form is empty
  parcel_id       = request.form.get('parcel_id')       or '99.01.234567.89012345'
  first_name      = request.form.get('first_name')      or 'Johnny'
  last_name       = request.form.get('last_name')       or 'DropTables'
  einheit_id      = request.form.get('einheit_id')      or '123ABC'
  shelf_proposed  = request.form.get('shelf_proposed')  or '10'
  shelf_selected  = request.form.get('shelf_selected')  or '0'
  width_cm        = request.form.get('width_cm')        or '5'
  length_cm       = request.form.get('length_cm')       or '8'
  height_cm       = request.form.get('height_cm')       or '3'
  weight_g        = request.form.get('weight_g')        or '500'

  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()

  if not checkTableExists(mydb, "parcels"):
      return f'ERROR: table "parcels" does not exist!'

  sql_cmd =  f'INSERT INTO '\
                'parcels '\
                  '(parcel_id, first_name, last_name, einheit_id, shelf_proposed, shelf_selected, width_cm, length_cm, height_cm, weight_g) '\
              'VALUES ('\
                f'"{parcel_id}", '\
                f'"{first_name}", '\
                f'"{last_name}", '\
                f'"{einheit_id}", '\
                f'{shelf_proposed}, '\
                f'{shelf_selected}, '\
                f'{width_cm}, '\
                f'{length_cm}, '\
                f'{height_cm}, '\
                f'{weight_g})'
  print(sql_cmd)

  cursor.execute(sql_cmd)

  mydb.commit()

  cursor.close()

  #return f'Added new parcel: parcel_id: {parcel_id} FirstName:{first_name} LastName: {last_name} einheit_id: {einheit_id} shelf_proposed: {shelf_proposed} shelf_selected: {shelf_selected} '\
  #              f'width_cm: {width_cm} length_cm: {length_cm} height_cm: {height_cm} weight_g: {weight_g}'
  return render_template('index.html')

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