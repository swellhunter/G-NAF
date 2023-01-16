"""
Load G-NAF files using Python. Assumes tables created with prefix GNAF_and empty in a database named TEST.
Easy tweaks if this is not the case.

To do
=====
- Help.
- Maintenance Log.
- Add source to Devops git.
- Gui to choose the folders.
- Drop the tables, find and modify the sql and re-create the tables.
- add index and referential integrity afterwards or rebuild.


https://stackoverflow.com/a/3964691/1734032
https://learn.microsoft.com/en-us/sql/connect/odbc/windows/system-requirements-installation-and-driver-files?view=sql-server-2017#installing-microsoft-odbc-driver-for-sql-server
https://towardsdatascience.com/use-python-and-bulk-insert-to-quickly-load-data-from-csv-files-into-sql-server-tables-ba381670d376

"""
import os
import sys
import pyodbc


# Yes, these could be in a class by themselves. Later maybe.

def connect_and_load(psv_file_nm, sql_server_nm, db_nm, db_table_nm):
    # Connect to SLQServer, perform the insert, close the connection.
    # Nested functions, I know. Consecutive connections help with log update?
    conn = connect_db(sql_server_nm, db_nm)
    success = insert_data(conn, psv_file_nm, db_table_nm)
    conn.close()
    return success


def connect_db(sql_server_nm, db_nm):
    # Connect to SQLServer with Windows authentication.
    # conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + sql_server_nm + ';DATABASE=' + db_nm + ';Trusted_Connection=yes;'
    # Connect with password.
    conn_string = ("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=TEST;"+
                   "UID=userid;PWD=xxxxxxxx")
    conn = pyodbc.connect(conn_string)
    return conn


def insert_data(conn, psv_file_nm, db_table_nm):
    # Cumulative. Manually validate that rows 1 and 2 are excluded, included, explicitly.
    qry = ("BULK INSERT " + db_table_nm + " FROM '" + psv_file_nm +
           "' WITH (FIRSTROW=2, FIELDTERMINATOR='|', ROWTERMINATOR = '0x0D0A')")
    print(qry[:-65]) # qry is not a parm, so doing it here. We could do similar for progress in main.
    # Execute the query
    cursor  = conn.cursor()
    success = cursor.execute(qry)
    conn.commit()
    cursor.close()
    return success


# see gitHub for G-NAF projects that use argparse if you want to improve this.
# Jan 2023 the only variable should be the folder name.

if len(sys.argv) > 1:
    folder = sys.argv[1]
else:
    sys.exit('Usage: %s subdir' % sys.argv[0])
if not os.path.isdir(folder):
    sys.exit('Could not find %s folder' % sys.argv[1])

# Recurse into the tree. No need to be terribly efficient.
for root, dirs, files in os.walk(folder):
    for file in files:
        if file.endswith("_psv.psv"):  # really specific, no false matches.

            table_data_file_name = os.path.join(os.getcwd(), root, file)

            # More than one way to skin a cat. Starts with, first under etc.
            # Australia is dependent upon 3 character state fields, and some are 2.
            # We will crash if we do not find the table anyway so this is self validating.

            if file[0:15] == "Authority_Code_":
                tb_name = "GNAF_" + file[15:-8]
            elif file[0:4] in "ACT_NSW_QLD_TAS_VIC_":
                tb_name = "GNAF_" + file[4:-8]
            else:
                tb_name = "GNAF_" + file[3:-8]

            # this is the money statement.
            connect_and_load(table_data_file_name, 'localhost', 'TEST', tb_name)
