from cgitb import html
from flask import Flask, request, render_template, url_for, redirect, jsonify, send_file
import pandas as pd
from io import BytesIO

from db import *
from processing import *

from datetime import datetime

from urllib.parse import quote_plus, unquote_plus


app = Flask(__name__)


# Global Variables
last_change = '-'

###############################################################################
# Routes
###############################################################################

@app.route('/')
def index():
  global last_change
  no_parcels_total, no_parcels_tobeassigned, no_parcels_tobesorted, no_parcels_sorted = count_parcels()

  return render_template('index.html', 
      last_change=last_change,
      no_parcels_total=no_parcels_total, no_parcels_tobeassigned=no_parcels_tobeassigned, no_parcels_tobesorted=no_parcels_tobesorted, no_parcels_sorted=no_parcels_sorted)

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
  parcel_table_html += '<table><tr>'+' '.join(['<th>'+str(item)+'</th>' for item in row_headers]) + '</tr>'
  for row in results:
    this_parcel_id = row[0]
    parcel_table_html += '<tr>'+' '.join(['<td>'+str(item)+'</td>' for item in row]) + f'<td><a href="search/{this_parcel_id}">Edit</a></td></tr>'
  parcel_table_html += '</table><br><br><a href="/">Back to start</a>'
  
  return parcel_table_html


# Initialize database (Deletes all existing records!)
@app.route('/initdb')
def initdb():
  global last_change
  db_init()
  db_init_table_parcels()
  db_init_table_client_log()
  last_change = "Initialized database!"
  return 'Re-initialized database<br><br><a href="/">Back to start</a>'

# Create new parcel by entering all data by hand
@app.route('/newparcel')
def new_parcel():
  return render_template('new-parcel.html')

# Create new parcel (after clicking SUBMIT)
@app.route('/newparcel', methods=['POST'])
def new_parcel_post():
  global last_change
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

  ret = db_insert_into_table('parcels',
          ['parcel_id', 'first_name', 'last_name', 'einheit_id', 'shelf_proposed', 'shelf_selected', 'dim_1', 'dim_2', 'dim_3', 'weight_g'],
          [f'"{parcel_id}"', f'"{first_name}"', f'"{last_name}"', f'"{einheit_id}"', f'{shelf_proposed}', f'{shelf_selected}', f'{dim_1}', f'{dim_2}', f'{dim_3}', f'{weight_g}'])
  if ret:
    last_change = f"ERROR: Unable to manually add parcel {parcel_id}"

  last_change = f"SUCCESS manually adding parcel {parcel_id}"

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

  # Test if data is valid. Eg. if parcel_id is correct format
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
  
  # Get the values for the different columns. Make them safe for a URL with quote_plus. For example "/" can not be passed!
  parcel_id       = quote_plus(str(row[0]))
  first_name      = quote_plus(str(row[1]))
  last_name       = quote_plus(str(row[2]))
  einheit_id      = quote_plus(str(row[3]))
  shelf_proposed  = quote_plus(str(row[4]))
  shelf_selected  = quote_plus(str(row[5]))
  dim_1           = quote_plus(str(row[6]))
  dim_2           = quote_plus(str(row[7]))
  dim_3           = quote_plus(str(row[8]))
  weight_g        = quote_plus(str(row[9]))

  cursor.close()

  return redirect(url_for('edit_parcel',  parcel_id=f'{parcel_id}', first_name=f'{first_name}', last_name=f'{last_name}', \
                                          einheit_id=f'{einheit_id}', shelf_proposed=f'{shelf_proposed}', shelf_selected=f'{shelf_selected}', \
                                          dim_1=f'{dim_1}', dim_2=f'{dim_2}', dim_3=f'{dim_3}', weight_g=f'{weight_g}'))


# Edit a parcel
@app.route('/edit/<parcel_id>/<first_name>/<last_name>/<einheit_id>/<shelf_proposed>/<shelf_selected>/<dim_1>/<dim_2>/<dim_3>/<weight_g>')
def edit_parcel(parcel_id, first_name, last_name, einheit_id, shelf_proposed, shelf_selected, dim_1, dim_2, dim_3, weight_g):
  # Remove quotes from making strings URL safe:
  parcel_id_uq       = unquote_plus(str(parcel_id))
  first_name_uq      = unquote_plus(str(first_name))
  last_name_uq       = unquote_plus(str(last_name))
  einheit_id_uq      = unquote_plus(str(einheit_id))
  shelf_proposed_uq  = unquote_plus(str(shelf_proposed))
  shelf_selected_uq  = unquote_plus(str(shelf_selected))
  dim_1_uq           = unquote_plus(str(dim_1))
  dim_2_uq           = unquote_plus(str(dim_2))
  dim_3_uq           = unquote_plus(str(dim_3))
  weight_g_uq        = unquote_plus(str(weight_g))

  return render_template('edit.html', parcel_id = parcel_id_uq, first_name = first_name_uq, last_name = last_name_uq,
                                      einheit_id = einheit_id_uq, shelf_proposed = shelf_proposed_uq, shelf_selected = shelf_selected_uq,
                                      dim_1 = dim_1_uq, dim_2 = dim_2_uq, dim_3 = dim_3_uq, weight_g = weight_g_uq)

# Edit a parcel (after clicking SUBMIT)
@app.route('/edit/<parcel_id>/<first_name>/<last_name>/<einheit_id>/<shelf_proposed>/<shelf_selected>/<dim_1>/<dim_2>/<dim_3>/<weight_g>', methods=['POST'])
def edit_parcel_post(parcel_id, first_name, last_name, einheit_id, shelf_proposed, shelf_selected, dim_1, dim_2, dim_3, weight_g):
  parcel_id       = request.form.get('parcel_id')
  first_name      = request.form.get('first_name')
  last_name       = request.form.get('last_name')
  einheit_id      = request.form.get('einheit_id')
  shelf_proposed  = request.form.get('shelf_proposed')
  shelf_selected  = request.form.get('shelf_selected')
  dim_1           = request.form.get('dim_1')
  dim_2           = request.form.get('dim_2')
  dim_3           = request.form.get('dim_3')
  weight_g        = request.form.get('weight_g')

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

@app.route("/upload", methods=['GET', 'POST'])
def upload_file():
  global last_change
  html = 'ERROR: Unable to upload file'
  if request.method == 'POST':
    print(request.files['file'])
    f = request.files['file']
    data_xls = pd.read_excel(f)
    html, string = import_parcels_to_db(data_xls.to_dict())
    last_change = string
    return html
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
  return download_tables_as_xlsx(['parcels', 'client_log'], 'bula_post_parcels.xlsx')

###############################################################################
# Processing
###############################################################################

@app.route('/assign')
def assign_shelf():
  global last_change
  html_string, summary_string = assign_shelf_to_new_parcels()
  last_change = summary_string
  return html_string

# Sort a parcel - search it
@app.route('/sort_search')
def sort_search():
  return render_template('sort-search.html')

# Sort a parcel - search it (after clicking SUBMIT)
@app.route('/sort_search', methods=['POST'])
def sort_search_post():
  parcel_id = request.form.get('parcel_id')

  # Test if data is valid. Eg. if parcel_id is correct format
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

  # Check if we have a parcel in our table that matches parcel_id
  sql_cmd = f"SELECT * FROM parcels WHERE parcel_id = '{parcel_id}'"
  print(sql_cmd)
  cursor.execute(sql_cmd)

  print(f"DBG: cursor={cursor}")
  
  row = cursor.fetchone()
  if row == None:
    print(f'ERROR: Unable to find parcel with id {parcel_id}')
    return f'ERROR: Unable to find parcel with id {parcel_id}<br><a href="/sort_search">go back</a>"'

  for row in cursor:
    print(f"* {row}")
    #TODO: Test if multiple parcels match the searched id!
  
  # Get the values for the different columns. Make them safe for a URL with quote_plus. For example "/" can not be passed!
  parcel_id       = quote_plus(str(row[0]))
  shelf_proposed  = quote_plus(str(row[4]))
  shelf_selected  = quote_plus(str(row[5]))

  cursor.close()

  return redirect(url_for('sort_edit',  parcel_id=f'{parcel_id}', shelf_proposed=f'{shelf_proposed}', shelf_selected=f'{shelf_selected}'))

# Sort a parcel - edit it
@app.route('/sort_edit/<parcel_id>/<shelf_proposed>/<shelf_selected>')
def sort_edit(parcel_id, shelf_proposed, shelf_selected):
  # Remove quotes from making strings URL safe:
  parcel_id_uq       = unquote_plus(str(parcel_id))
  shelf_proposed_uq  = unquote_plus(str(shelf_proposed))
  shelf_selected_uq  = unquote_plus(str(shelf_selected))

  return render_template('sort-edit.html', parcel_id = parcel_id_uq, shelf_proposed = shelf_proposed_uq, shelf_selected = shelf_selected_uq)

# Sort a parcel - edit it (after clicking SUBMIT)
@app.route('/sort_edit/<parcel_id>/<shelf_proposed>/<shelf_selected>', methods=['POST'])
def sort_edit_post(parcel_id, shelf_proposed, shelf_selected):
  global last_change
  parcel_id       = request.form.get('parcel_id')
  shelf_proposed  = request.form.get('shelf_proposed')
  shelf_selected  = request.form.get('shelf_selected')

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
                      f'shelf_selected = {shelf_selected} '\
                    f'WHERE parcel_id = "{parcel_id}"'
  print(sql_update_cmd)
  cursor.execute(sql_update_cmd)
  mydb.commit()

  # Test if it worked
  cursor.execute(sql_select_cmd)
  record = cursor.fetchone()
  print(record)
  cursor.close()

  last_change = f"Sorted parcel {record} into shelf {shelf_selected}"

  #return f'SUCCESS! Sorted parcel {record} to shelf {shelf_selected}. Proposed shelf was {shelf_proposed}<br><br><a href="/">Home</a>'
  return redirect(url_for('index'))

###############################################################################
# Client access
###############################################################################

# Check-In client
@app.route('/checkin')
def checkin():
  return render_template('checkin.html')

# Check-In client (after clicking SUBMIT)
@app.route('/checkin', methods=['POST'])
def checkin_post():
  client_id = request.form.get('client_id')
  # TODO: Check if client id is valid  

  checkin_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

  db_insert_into_table('client_log', ['client_id', 'checkin_time'], [f'{client_id}', f'"{checkin_time}"'])

  return redirect(url_for('index'))



if __name__ == "__main__":
  app.run(host ='0.0.0.0')