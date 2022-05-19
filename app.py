import mysql.connector
import json
import re
from flask import Flask, request, render_template, url_for, redirect, jsonify, send_file
import pandas as pd
from io import BytesIO

app = Flask(__name__)

###############################################################################
# Helper Functions
###############################################################################

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

def test_parcel_id_valid(parcel_id):
  print(f'DBG: testing {parcel_id} for validity')
  # Test if we got an empty string for parcel_id
  if parcel_id == '' or parcel_id == 'None':
    return f'ERROR: Invalid parcel_id {parcel_id}'

  # Test if we got the correct format like "99.01.234567.89012345"
  matched = re.match("99\.[0-9]{2}\.[0-9]{6}\.[0-9]{8}", parcel_id)
  is_match = bool(matched)
  if not is_match:
    return f'ERROR: Invalid parcel_id {parcel_id}. Expected "99.01.234567.89012345"'

###############################################################################
# Routes
###############################################################################

@app.route('/')
def index():
  return render_template('index.html')

# List all known parcels
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
#  cursor.close()
#  return json.dumps(json_data)

  # Create table in HTML that lists all parcels
  parcel_table_html = '<h1>Parcel Overview</h1>'
  parcel_table_html += '<table><tr>'+' '.join(['<th>'+str(item)+'</th>' for item in row_headers]) + '</tr><br>'
  for row in results:
    this_parcel_id = row[0]
    parcel_table_html += '<tr>'+' '.join(['<td>'+str(item)+'</td>' for item in row]) + f'<td><a href="search/{this_parcel_id}">Edit</a></td></tr><br>'
  parcel_table_html += '</table><br><br><a href="/">Back to start</a>'
  
  return parcel_table_html


# Initialize database (Deletes all existing records!)
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

  create_table = """
    CREATE TABLE parcels
      (parcel_id VARCHAR(255),
       first_name VARCHAR(255),
       last_name VARCHAR(255),
       einheit_id VARCHAR(255),
       shelf_proposed SMALLINT UNSIGNED,
       shelf_selected SMALLINT UNSIGNED,
       width_cm TINYINT UNSIGNED,
       length_cm TINYINT UNSIGNED,
       height_cm TINYINT UNSIGNED,
       weight_g SMALLINT UNSIGNED)
  """
  print(create_table)

  cursor.execute("DROP TABLE IF EXISTS parcels")
  cursor.execute(create_table)
  cursor.close()

  return 'init database'


# Create new parcel
@app.route('/newparcel')
def new_parcel():
  return render_template('new-parcel.html')

# Create new parcel (after clicking SUBMIT)
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

  ret = test_parcel_id_valid(parcel_id)
  if ret: return ret

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
  return redirect(url_for('index'))


# Search for a parcel
@app.route('/search/<parcel_id>')
def search_parcel(parcel_id):
  return render_template('search.html', parcel_id=f'{parcel_id}')

# Search for a parcel (after clicking SUBMIT)
@app.route('/search/<parcel_id>', methods=['POST'])
def search_parcel_post(parcel_id):
  parcel_id = request.form.get('parcel_id')

  # Test if we got an empty string
  if parcel_id == '' or parcel_id == 'None':
    return f'ERROR: Invalid parcel_id {parcel_id}'

  # Test if we got the correct format like "99.01.234567.89012345"
  matched = re.match("99\.[0-9]{2}\.[0-9]{6}\.[0-9]{8}", parcel_id)
  is_match = bool(matched)
  if not is_match:
    return f'ERROR: Invalid parcel_id {parcel_id}. Expected "99.01.234567.89012345"'

  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()

  if not checkTableExists(mydb, "parcels"):
      return f'ERROR: table "parcels" does not exist!'

  # Check if we have a parcel in our table that matches parcel_id
  sql_cmd = f"SELECT * FROM parcels WHERE parcel_id = '{parcel_id}'"
  print(sql_cmd)
  cursor.execute(sql_cmd)

  print(f"DBG: cursor={cursor}")
  
  row = cursor.fetchone()
  if row == None:
    print(f'ERROR: Unable to find parcel with id {parcel_id}')
    return f'ERROR: Unable to find parcel with id {parcel_id}<br><a href="/search">go back</a>"'

  for row in cursor:
    print(f"* {row}")
    #TODO: Test if multiple parcels match the searched id!
  
  #row = cursor[0]
  parcel_id = row[0]
  first_name = row[1]
  last_name = row[2]
  einheit_id = row[3]
  shelf_proposed = row[4]
  shelf_selected = row[5]
  width_cm = row[6]
  length_cm = row[7]
  height_cm = row[8]
  weight_g = row[9]

  cursor.close()

  return redirect(url_for('edit_parcel',  parcel_id=f'{parcel_id}', first_name=f'{first_name}', last_name=f'{last_name}', \
                                          einheit_id=f'{einheit_id}', shelf_proposed=f'{shelf_proposed}', shelf_selected=f'{shelf_selected}', \
                                          width_cm=f'{width_cm}', length_cm=f'{length_cm}', height_cm=f'{height_cm}', weight_g=f'{weight_g}'))


# Edit a parcel
@app.route('/edit/<parcel_id>/<first_name>/<last_name>/<einheit_id>/<shelf_proposed>/<shelf_selected>/<width_cm>/<length_cm>/<height_cm>/<weight_g>')
def edit_parcel(parcel_id, first_name, last_name, einheit_id, shelf_proposed, shelf_selected, width_cm, length_cm, height_cm, weight_g):
  return render_template('edit.html',  parcel_id=f'{parcel_id}', first_name=f'{first_name}', last_name=f'{last_name}', \
                                          einheit_id=f'{einheit_id}', shelf_proposed=f'{shelf_proposed}', shelf_selected=f'{shelf_selected}', \
                                          width_cm=f'{width_cm}', length_cm=f'{length_cm}', height_cm=f'{height_cm}', weight_g=f'{weight_g}')

# Edit a parcel (after clicking SUBMIT)
@app.route('/edit/<parcel_id>/<first_name>/<last_name>/<einheit_id>/<shelf_proposed>/<shelf_selected>/<width_cm>/<length_cm>/<height_cm>/<weight_g>', methods=['POST'])
def edit_parcel_post(parcel_id, first_name, last_name, einheit_id, shelf_proposed, shelf_selected, width_cm, length_cm, height_cm, weight_g):
  parcel_id = request.form.get('parcel_id')
  first_name = request.form.get('first_name')
  last_name = request.form.get('last_name')
  einheit_id = request.form.get('einheit_id')
  shelf_proposed = request.form.get('shelf_proposed')
  shelf_selected = request.form.get('shelf_selected')
  width_cm = request.form.get('width_cm')
  length_cm = request.form.get('length_cm')
  height_cm = request.form.get('height_cm')
  weight_g = request.form.get('weight_g')

  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()

  if not checkTableExists(mydb, "parcels"):
      return f'ERROR: table "parcels" does not exist!'

  # Check if we have a parcel in our table that matches parcel_id
  sql_select_cmd = f"SELECT * FROM parcels WHERE parcel_id = '{parcel_id}'"
  print(sql_select_cmd)  
  cursor.execute(sql_select_cmd)
  record = cursor.fetchone()
  print(f"EDITING {record}")

  # Update this single record
  sql_update_cmd = f'UPDATE parcels SET '\
                      f'first_name = "{first_name}", '\
                      f'last_name = "{last_name}", '\
                      f'einheit_id = "{einheit_id}", '\
                      f'shelf_proposed = {shelf_proposed}, '\
                      f'shelf_selected = {shelf_selected}, '\
                      f'width_cm = {width_cm}, '\
                      f'length_cm = {length_cm}, '\
                      f'height_cm = {height_cm}, '\
                      f'weight_g = {weight_g} '\
                    f'WHERE parcel_id = "{parcel_id}"'
  print(sql_update_cmd)
  cursor.execute(sql_update_cmd)
  mydb.commit()

  # Test if it worked
  cursor.execute(sql_select_cmd)
  record = cursor.fetchone()
  print(record)
  
  cursor.close()
  return f'SUCCESS! Edited: {record}<br><br><a href="/">Home</a>'

#############################################
# Upload / Download / Export Functionality
#############################################

def import_parcels_to_db(parcel_dict):
  # We need all columns in the Excel sheet to be able to process it. Check and abort if not all are available
  required_keys = [False,False,False,False,False,False,False,False]
  for key in parcel_dict:
    if key   == 'parcel_id':  required_keys[0] = True
    elif key == 'first_name': required_keys[1] = True
    elif key == 'last_name':  required_keys[2] = True
    elif key == 'einheit_id': required_keys[3] = True
    elif key == 'width_cm':   required_keys[4] = True
    elif key == 'length_cm':  required_keys[5] = True
    elif key == 'height_cm':  required_keys[6] = True
    elif key == 'weight_g':   required_keys[7] = True
    else: print(f"WARNING: Unknown column in table: {key}")
  
  if not all(required_keys):
    return "<h1>ERROR: Missing column in Excel sheet!<h1>"

  print(parcel_dict)

  parcel_count = len(parcel_dict['parcel_id'])

  for i in range(parcel_count):
    parcel_id  = str(parcel_dict['parcel_id'][i])
    first_name = str(parcel_dict['first_name'][i])
    last_name  = str(parcel_dict['last_name'][i])
    einheit_id = str(parcel_dict['einheit_id'][i])
    width_cm   = str(parcel_dict['width_cm'][i])
    length_cm  = str(parcel_dict['length_cm'][i])
    height_cm  = str(parcel_dict['height_cm'][i])
    weight_g   = str(parcel_dict['weight_g'][i])
    
    # Test if data is valid. Eg. if parcel_id is correct format
    ret = test_parcel_id_valid(parcel_id)
    if ret: return ret

    # Test if parcel_id already exists, we dont want any duplicates
    mydb = mysql.connector.connect(
      host="mysqldb",
      user="root",
      password="secret",
      database="inventory"
    )
    cursor = mydb.cursor()

    if not checkTableExists(mydb, "parcels"):
        return f'ERROR: table "parcels" does not exist!'

    sql_cmd = f"SELECT * FROM parcels WHERE parcel_id = '{parcel_id}'"
    print(sql_cmd)
    cursor.execute(sql_cmd)    
    row = cursor.fetchone()
    if row != None:
      print(f'ERROR: There is already a parcel with id {parcel_id}')
      continue # skip inserting the parcel into the db

    else:
      print("No duplicate parcel ids found (this is good!)")

    # Now insert the new parcel into the db
    # Note: shelf_proposed and shelf_selected are empty after import!
    sql_cmd =  f'INSERT INTO '\
                  'parcels '\
                    '(parcel_id, first_name, last_name, einheit_id, shelf_proposed, shelf_selected, width_cm, length_cm, height_cm, weight_g) '\
                'VALUES ('\
                  f'"{parcel_id}", '\
                  f'"{first_name}", '\
                  f'"{last_name}", '\
                  f'"{einheit_id}", '\
                  f'0, '\
                  f'0, '\
                  f'{width_cm}, '\
                  f'{length_cm}, '\
                  f'{height_cm}, '\
                  f'{weight_g})'
    print(sql_cmd)
    cursor.execute(sql_cmd)
    mydb.commit()
    cursor.close()
  return

@app.route("/upload", methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        print(request.files['file'])
        f = request.files['file']
        data_xls = pd.read_excel(f)
        ret = import_parcels_to_db(data_xls.to_dict())
        if ret: return ret
        import_excel_html = '<h1>Imported Excel file:<h1>' + data_xls.to_html() + '<br><br><a href="/">Back to start</a>'
        return import_excel_html
    return '''
    <!doctype html>
    <title>Upload an excel file</title>
    <h1>Excel file upload (xls, xlsx, xlsm, xlsb, odf, ods or odt)</h1>
    <form action="" method=post enctype=multipart/form-data>
    <p><input type=file name=file><input type=submit value=Upload>
    </form>
    '''

@app.route("/export", methods=['GET'])
def export_records():
  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  df = pd.io.sql.read_sql('SELECT * FROM parcels', mydb)
  print(df)

  output = BytesIO()
  writer = pd.ExcelWriter(output, engine='xlsxwriter')
  df.to_excel(writer, sheet_name='Sheet1')
  writer.save()
  output.seek(0)
  
  return send_file(output, attachment_filename='bula_post_export.xlsx', as_attachment=True)



###############################################################################
# Deprecated
###############################################################################

# Old test for quickly adding a parcel
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