from cgitb import html
import mysql.connector
import json
import re
from flask import Flask, request, render_template, url_for, redirect, jsonify, send_file
import pandas as pd
from io import BytesIO
from math import isnan

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

  # Test if we got the correct format like "990123456789012345"
  matched = re.match("99[0-9]{16}", parcel_id)
  is_match = bool(matched)
  if not is_match:
    return f'ERROR: Invalid parcel_id {parcel_id}. Expected "990123456789012345"'

def db_select_from_table_where(table, where_col, where_val):
  """
  Select all elements in a table that fit a where column.
  User must execute cursor.close() !
  Returns:
    mydb    = mysql.connector.connect
    cursor  = mydb.cursor
    results = all results as list with rows.
  """
  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()

  if not checkTableExists(mydb, str(table)):
      print(f'ERROR: table f"{table}" does not exist!')
      return 0, 0, 0

  sql_cmd = f"SELECT * FROM {table} WHERE {where_col} = '{str(where_val)}'"
  print(sql_cmd)
  cursor.execute(sql_cmd)
  
  results = cursor.fetchall()

  cursor.close()

  return results

def db_test_if_value_exists_in_column_in_table(table, column, value):
  """
  Select all elements in a table that fit a where column.
  Returns: True if value exists and False if it does not exist
  """
  results = db_select_from_table_where(table, column, value)

  if results == []:
    value_exists = False
  else:
    value_exists = True

  return value_exists

def db_find_max_value_for_column_in_table(table, column):
  """
  Finds the maximum value for a column in a table
  Returns:
    mydb      = mysql.connector.connect
    cursor    = mydb.cursor
    max_value = Maximum value
  """
  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()

  if not checkTableExists(mydb, str(table)):
      print(f'ERROR: table f"{table}" does not exist!')
      return 0, 0, 0

  sql_cmd = f"SELECT MAX({column} AS maximum FROM {table}"
  print(sql_cmd)
  cursor.execute(sql_cmd)
  
  results = cursor.fetchall()
  max_value = results[0][0]

  cursor.close()

  return max_value

def db_update_column_for_record_where_column_has_value(table, col_set_name, col_set_val, col_where_name, col_where_val):
  """
  For one record that where a column has a given name, set another column to a value.
  Arguments:
    table:          The table to edit
    col_set_name:   Name of the column that shall be changed
    col_set_val:    Value of the column that shall be changed
    col_where_name: Name of the column to find our record
    col_where_val:  Value of the column to find our record
  Returns:
    Always True
  """
  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()
  sql_update_cmd = f'UPDATE parcels SET '\
                    f'{col_set_name} = {col_set_val} '\
                  f'WHERE {col_where_name} = "{col_where_val}"'
  print(sql_update_cmd)
  cursor.execute(sql_update_cmd)
  mydb.commit()
  cursor.close()

  # TODO: Add check if update worked

  return True


###############################################################################
# Data Processing
###############################################################################
  
def assign_shelf_to_new_parcels():
  """
  Find all parcels that have not yet been assigned to a shelf (shelf_proposed) 
  and assigns them.
  """
  assigned_count     = 0
  assigned_parcel_id = []
  assigned_shelf     = []
  failed_count       = 0
  failed_parcel_id   = []

  # Find all parcels that have not been assigned to a shelf yet  
  results = db_select_from_table_where('parcels', 'shelf_proposed', '0')

  for row in results:
    parcel_id_this  = row[0]
    einheit_id_this = row[3]

    # Determine which shelf this parcel should go on
    results_einheit = db_select_from_table_where('parcels', 'einheit_id', f'{einheit_id_this}')
    print(f"DBG: results_einheit={results_einheit}")

    parcel_needs_shelf = True
    
    # Find if there are other parcels for the same einheit_id that have already a shelf_proposed
    if not(results_einheit[0][0] == str(parcel_id_this) and len(results_einheit) == 1):
      # There are already parcels for this einheit_id, check them all out
      for row_einheit in results_einheit:
        parcel_id_einheit      = row_einheit[0]
        shelf_proposed_einheit = row_einheit[4]
        # TODO: Check if there is enough space in this shelf

        if parcel_id_einheit != parcel_id_this and shelf_proposed_einheit != 0 and parcel_needs_shelf:
            shelf_proposed = row_einheit[4]
            print(f'Parcel {parcel_id_this} has already parcels for einheit {einheit_id_this} in shelf {shelf_proposed}')
            parcel_needs_shelf = False

    # This is the only parcel for this einheit_id
    if parcel_needs_shelf:
      # Put it into the next empty shelf
      # TODO: Determine which shelf fits for this parcel size / weight

      # Iterate through all shelves starting at 0 to find the first empty one
      # TODO: Inefficient!
      shelf_proposed = 1
      while (db_test_if_value_exists_in_column_in_table('parcels', 'shelf_proposed', f'{shelf_proposed}')):
        shelf_proposed += 1
      print(f'Parcel {parcel_id_this} is the first for einheit {einheit_id_this} and was assigned to shelf {shelf_proposed}')

    if shelf_proposed == 0:
      print(f"ERROR: Unable to assign shelf to {parcel_id_this}")
      failed_count += 1
      failed_parcel_id.append(parcel_id_this)
    else:
      # Update this single record
      ret = db_update_column_for_record_where_column_has_value('parcels', 'shelf_proposed', shelf_proposed, 'parcel_id', parcel_id_this)
      if not ret:
        print(f"ERROR: Unable to change shelf_proposed for parcel_id {parcel_id_this}")

      assigned_count = assigned_count + 1
      assigned_parcel_id.append(str(parcel_id_this))
      assigned_shelf.append(str(shelf_proposed))
  
  # Generate overview of which shelfs have been assigned
  return_string = f'Assigned shelf to {assigned_count} parcels:<br><br>'
  for i in range(assigned_count):
    return_string += f'ID: {assigned_parcel_id[i]}: Shelf {assigned_shelf[i]}<br>'
  if failed_count > 0:
    return_string += f'<br><b>FAILED</b> to assign shelf to {failed_count} parcels:' + '<br>'.join(failed_parcel_id)
  return_string += '<br><br><a href="/">Back to start</a>'

  return return_string
    


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
  parcel_table_html = '<h1>Parcel Overview</h1><br>'
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
       dim_1 SMALLINT UNSIGNED,
       dim_2 SMALLINT UNSIGNED,
       dim_3 SMALLINT UNSIGNED,
       weight_g SMALLINT UNSIGNED)
  """
  print(create_table)

  cursor.execute("DROP TABLE IF EXISTS parcels")
  cursor.execute(create_table)
  cursor.close()

  return 'Re-initialized database<br><br><a href="/">Back to start</a>'


# Create new parcel
@app.route('/newparcel')
def new_parcel():
  return render_template('new-parcel.html')

# Create new parcel (after clicking SUBMIT)
@app.route('/newparcel', methods=['POST'])
def new_parcel_post():
  # Variable        gets data from form                 or uses default value if form is empty
  parcel_id       = request.form.get('parcel_id')       or '990123456789012345'
  first_name      = request.form.get('first_name')      or 'Johnny'
  last_name       = request.form.get('last_name')       or 'DropTables'
  einheit_id      = request.form.get('einheit_id')      or '123ABC'
  shelf_proposed  = request.form.get('shelf_proposed')  or '0'
  shelf_selected  = request.form.get('shelf_selected')  or '0'
  dim_1           = request.form.get('dim_1')           or '500'
  dim_2           = request.form.get('dim_2')           or '800'
  dim_3           = request.form.get('dim_3')           or '300'
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
                  '(parcel_id, first_name, last_name, einheit_id, shelf_proposed, shelf_selected, dim_1, dim_2, dim_3, weight_g) '\
              'VALUES ('\
                f'"{parcel_id}", '\
                f'"{first_name}", '\
                f'"{last_name}", '\
                f'"{einheit_id}", '\
                f'{shelf_proposed}, '\
                f'{shelf_selected}, '\
                f'{dim_1}, '\
                f'{dim_2}, '\
                f'{dim_3}, '\
                f'{weight_g})'
  print(sql_cmd)

  cursor.execute(sql_cmd)

  mydb.commit()

  cursor.close()

  #return f'Added new parcel: parcel_id: {parcel_id} FirstName:{first_name} LastName: {last_name} einheit_id: {einheit_id} shelf_proposed: {shelf_proposed} shelf_selected: {shelf_selected} '\
  #              f'dim_1: {dim_1} dim_2: {dim_2} dim_3: {dim_3} weight_g: {weight_g}'
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

  # Test if we got the correct format like "990123456789012345"
  matched = re.match("99[0-9]{16}", parcel_id)
  is_match = bool(matched)
  if not is_match:
    return f'ERROR: Invalid parcel_id {parcel_id}. Expected "990123456789012345"'

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
  dim_1 = row[6]
  dim_2 = row[7]
  dim_3 = row[8]
  weight_g = row[9]

  cursor.close()

  return redirect(url_for('edit_parcel',  parcel_id=f'{parcel_id}', first_name=f'{first_name}', last_name=f'{last_name}', \
                                          einheit_id=f'{einheit_id}', shelf_proposed=f'{shelf_proposed}', shelf_selected=f'{shelf_selected}', \
                                          dim_1=f'{dim_1}', dim_2=f'{dim_2}', dim_3=f'{dim_3}', weight_g=f'{weight_g}'))


# Edit a parcel
@app.route('/edit/<parcel_id>/<first_name>/<last_name>/<einheit_id>/<shelf_proposed>/<shelf_selected>/<dim_1>/<dim_2>/<dim_3>/<weight_g>')
def edit_parcel(parcel_id, first_name, last_name, einheit_id, shelf_proposed, shelf_selected, dim_1, dim_2, dim_3, weight_g):
  return render_template('edit.html',  parcel_id=f'{parcel_id}', first_name=f'{first_name}', last_name=f'{last_name}', \
                                          einheit_id=f'{einheit_id}', shelf_proposed=f'{shelf_proposed}', shelf_selected=f'{shelf_selected}', \
                                          dim_1=f'{dim_1}', dim_2=f'{dim_2}', dim_3=f'{dim_3}', weight_g=f'{weight_g}')

# Edit a parcel (after clicking SUBMIT)
@app.route('/edit/<parcel_id>/<first_name>/<last_name>/<einheit_id>/<shelf_proposed>/<shelf_selected>/<dim_1>/<dim_2>/<dim_3>/<weight_g>', methods=['POST'])
def edit_parcel_post(parcel_id, first_name, last_name, einheit_id, shelf_proposed, shelf_selected, dim_1, dim_2, dim_3, weight_g):
  parcel_id = request.form.get('parcel_id')
  first_name = request.form.get('first_name')
  last_name = request.form.get('last_name')
  einheit_id = request.form.get('einheit_id')
  shelf_proposed = request.form.get('shelf_proposed')
  shelf_selected = request.form.get('shelf_selected')
  dim_1 = request.form.get('dim_1')
  dim_2 = request.form.get('dim_2')
  dim_3 = request.form.get('dim_3')
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
                      f'dim_1 = {dim_1}, '\
                      f'dim_2 = {dim_2}, '\
                      f'dim_3 = {dim_3}, '\
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
  # Keep a list of which parcels where imported into the db and which were skipped
  parcels_imported_count = 0
  parcels_imported_list  = []
  parcels_skipped_count  = 0
  parcels_skipped_list   = []
  parcels_skipped_cause  = []

  # We need all columns in the Excel sheet to be able to process it. Check and abort if not all are available
  required_keys = [False,False,False,False,False,False,False,False]
  for key in parcel_dict:
    if key   == 'IC':         required_keys[0] = True # Parcel ID
    elif key == 'NAME3':      required_keys[1] = True # First Name
    elif key == 'STRASSE':    required_keys[2] = True # Last Name / Vulgo
    elif key == 'NAME2':      required_keys[3] = True # Einheit ID
    elif key == 'DIM_1':      required_keys[4] = True # Dimension 1 in mm
    elif key == 'DIM_2':      required_keys[5] = True # Dimension 2 in mm
    elif key == 'DIM_3':      required_keys[6] = True # Dimension 3 in mm
    elif key == 'GEWICHT':    required_keys[7] = True # Weight in gram
    else: print(f"WARNING: Unknown column in table: {key}")
  
  if not all(required_keys):
    return "<h1>ERROR: Missing column in Excel sheet!<h1>"

  print(parcel_dict)

  parcel_count = len(parcel_dict['IC'])

  for i in range(parcel_count):
    # If cell is empty, it will give us 'NaN'. Convert it to 0.
    # TODO: Generate warning if we get a NaN!
    parcel_id  = str(parcel_dict['IC'][i] if isinstance(parcel_dict['IC'][i], int) else 0)            # Expect int
    first_name = str(parcel_dict['NAME3'][i])                                                         # Expect string
    last_name  = str(parcel_dict['STRASSE'][i])                                                       # Expect string
    einheit_id = str(parcel_dict['NAME2'][i] if isinstance(parcel_dict['NAME2'][i], int) else 0)      # Expect string
    dim_1      = str(int(parcel_dict['DIM_1'][i]) if not isnan(parcel_dict['DIM_1'][i]) else 0)       # Expect float
    dim_2      = str(int(parcel_dict['DIM_2'][i]) if not isnan(parcel_dict['DIM_2'][i]) else 0)       # Expect float
    dim_3      = str(int(parcel_dict['DIM_3'][i]) if not isnan(parcel_dict['DIM_3'][i]) else 0)       # Expect float
    weight_g   = str(parcel_dict['GEWICHT'][i] if isinstance(parcel_dict['GEWICHT'][i], int) else 0)  # Expect int
    
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
      parcels_skipped_count += 1
      parcels_skipped_list.append(str(parcel_id))
      parcels_skipped_cause.append("Duplicate Parcel ID")
      continue # skip inserting the parcel into the db

    else:
      print("No duplicate parcel ids found (this is good!)")

    # Now insert the new parcel into the db
    # Note: shelf_proposed and shelf_selected are empty after import!
    sql_cmd =  f'INSERT INTO '\
                  'parcels '\
                    '(parcel_id, first_name, last_name, einheit_id, shelf_proposed, shelf_selected, dim_1, dim_2, dim_3, weight_g) '\
                'VALUES ('\
                  f'"{parcel_id}", '\
                  f'"{first_name}", '\
                  f'"{last_name}", '\
                  f'"{einheit_id}", '\
                  f'0, '\
                  f'0, '\
                  f'{dim_1}, '\
                  f'{dim_2}, '\
                  f'{dim_3}, '\
                  f'{weight_g})'
    print(sql_cmd)
    cursor.execute(sql_cmd)
    mydb.commit()
    cursor.close()

    parcels_imported_count += 1
    parcels_imported_list.append(str(parcel_id))

  html_imported_parcels = "<h1>DONE importing parcels from Excel upload</h1><br>"
  html_imported_parcels += f"TOTAL \t({parcel_count}) parcels found in Excel file<br>"
  html_imported_parcels += f"SUCCESS \t({parcels_imported_count}) have been imported<br>"
  html_imported_parcels += f"FAIL \t({parcels_skipped_count}) have been skipped (eg. because of duplicate parcel id)<br><br>List of fails:"
  for i in range(len(parcels_skipped_list)):
    html_imported_parcels += f'<br>\t{parcels_skipped_list[i]} (Cause: {parcels_skipped_cause[i]})'
  html_imported_parcels += f"<br><br>List of successes:<br>" + '<br>\t'.join(parcels_imported_list) + '<br><br><a href="/">Back to start</a>'

  return html_imported_parcels

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
# Processing
###############################################################################

@app.route('/assign')
def assign_shelf():
  return assign_shelf_to_new_parcels()

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

  parcel_id = "993711592322371774"
  first_name = "Johnny"
  last_name = "DropTables"

  sql_cmd = f'INSERT INTO parcels (parcel_id, first_name, last_name) VALUES ("{parcel_id}", "{first_name}", "{last_name}")'

  cursor.execute(sql_cmd)

  mydb.commit()

  cursor.close()

  return f'added parcel_id: {parcel_id} FirstName:{first_name} LastName: {last_name}'


if __name__ == "__main__":
  app.run(host ='0.0.0.0')