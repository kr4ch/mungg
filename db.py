import mysql.connector
import json
import re


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

###############################################################################
# DB Access
###############################################################################
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

  return

def db_init_table_parcels():
  """
  Initialize table "parcels" in database.
  Creates table and all its columns.
  If the table already existed, it will be deleted!
  """
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
  
  return

def db_init_table_client_log():
  """
  Initialize table "client_log" in database.
  Creates table and all its columns.
  If the table already existed, it will be deleted!
  """
  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()

  create_table = """
    CREATE TABLE client_log
      (client_id VARCHAR(255),
       checkin_time DATETIME,
       checkout_time DATETIME,
       store_time DATETIME,
       parcel_id VARCHAR(255))
  """
  print(create_table)

  cursor.execute("DROP TABLE IF EXISTS client_log")
  cursor.execute(create_table)
  cursor.close()

  return

def db_insert_into_table(table, col_name_list, col_val_list):
  """
  Insert an entry into table. Specify all columns and values to insert.
  Parameters:
    * table         = name of table
    * col_name_list = list with names of all columns
    * col_val_list  = list with values to into the col_name_list
  Returns:
    nothing
  """
  mydb = mysql.connector.connect(
    host="mysqldb",
    user="root",
    password="secret",
    database="inventory"
  )
  cursor = mydb.cursor()

  if not checkTableExists(mydb, str(table)):
      return f'ERROR: table {str(table)} does not exist!'

  sql_cmd =  f'INSERT INTO '\
                f'{str(table)} '\
                '( ' + ', '.join(col_name for col_name in col_name_list) + ' )'\
              'VALUES '\
                '( ' + ', '.join(col_val for col_val in col_val_list) + ' )'
               
  print(sql_cmd)
  cursor.execute(sql_cmd)
  mydb.commit()
  cursor.close()

  # TODO: test if insert worked and return success/fail

  return

def db_select_from_table_where(table, where_col, where_val):
  """
  Select all elements in a table that fit a where column.
  Returns:
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

def db_count_entries(table):
  """
  Counts the number of entries in a table
  Returns: number of entries or 0 if unable to find table
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
      return 0

  sql_cmd = f"SELECT * FROM {table}"
  cursor.execute(sql_cmd)
  
  results = cursor.fetchall()
  count = len(results)

  cursor.close()

  return count

def db_count_entries_where(table, where_col, where_val):
  """
  Counts the number of entries that fit a certain condition (where_col has value where_val).
  Returns: number of entries that fit condition or 0 if unable to find table
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
      return 0

  sql_cmd = f"SELECT * FROM {table} WHERE {where_col} = '{str(where_val)}'"
  cursor.execute(sql_cmd)
  
  results = cursor.fetchall()
  count = len(results)

  cursor.close()

  return count

def db_count_entries_where_and(table, where_col, where_val, where_col2, where_val2):
  """
  Counts the number of entries that fit a certain condition (where_col has value where_val)
  and a second contition (where_col2 has value where_val2).
  Returns: number of entries that fit condition or 0 if unable to find table
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
      return 0

  sql_cmd = f"SELECT * FROM {table} WHERE {where_col} = '{str(where_val)}' AND {where_col2} = '{str(where_val2)}'"
  cursor.execute(sql_cmd)
  
  results = cursor.fetchall()
  count = len(results)

  cursor.close()

  return count

def db_count_entries_where_not(table, where_col, where_val):
  """
  Counts the number of entries that DO NOT fit a certain condition (where_col has NOT value where_val).
  Returns: number of entries that fit condition or 0 if unable to find table
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
      return 0

  sql_cmd = f"SELECT * FROM {table} WHERE NOT {where_col} = '{str(where_val)}'"
  cursor.execute(sql_cmd)
  
  results = cursor.fetchall()
  count = len(results)

  cursor.close()

  return count

def db_count_entries_where_and_not(table, where_col, where_val, where_col2, where_val2):
  """
  Counts the number of entries that fit a certain condition (where_col has value where_val)
  and do NOT fit a second contition (where_col2 has value where_val2).
  Returns: number of entries that fit condition or 0 if unable to find table
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
      return 0

  sql_cmd = f"SELECT * FROM {table} WHERE {where_col} = '{str(where_val)}' AND NOT {where_col2} = '{str(where_val2)}'"
  cursor.execute(sql_cmd)
  
  results = cursor.fetchall()
  count = len(results)

  cursor.close()

  return count

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