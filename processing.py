import mysql.connector
from math import isnan

from db import *

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

  import_parcels_string = f"Imported parcels from Excel Sheet. Of a total {parcel_count} parcels succesfully imported {parcels_imported_count}."
  if parcels_skipped_count > 0:
    import_parcels_string += f" {parcels_skipped_count} failed to import!"

  return html_imported_parcels, import_parcels_string

def count_parcels():
    no_parcels_total        = db_count_entries('parcels')
    no_parcels_tobeassigned = db_count_entries_where_and('parcels', 'shelf_selected', '0', 'shelf_proposed', '0')
    no_parcels_tobesorted   = db_count_entries_where_and_not('parcels', 'shelf_selected', '0', 'shelf_proposed', '0')
    no_parcels_sorted       = db_count_entries_where_not('parcels', 'shelf_selected', '0')
    return no_parcels_total, no_parcels_tobeassigned, no_parcels_tobesorted, no_parcels_sorted