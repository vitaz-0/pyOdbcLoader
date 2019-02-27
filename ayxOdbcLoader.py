import pyodbc
import datetime
import calendar


SRC_CONSTR = 'Driver={MySQL ODBC 5.3 Unicode Driver};DATABASE=employees;Server=localhost;Option=3;Port=3306;UID=vzak;PWD=Pra1234gue'
SRC_AUTOCOMMIT = True

TGT_CONSTR = 'DRIVER={PostgreSQL Unicode(x64)};DATABASE=ngp;UID=ngp;PWD=wombat-wookie-charleston;SERVER=localhost;PORT=5431'
TGT_AUTOCOMMIT = False

SERVER_NAME = 'localhost'

def init_cursor(constr, autocommit):
    try:
        conn = pyodbc.connect(constr,autocommit=autocommit)
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-16le')
        conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-16le')
        conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-16le')
        conn.setencoding(encoding='utf-16le')
    except Exception as e:
           print('Cannot connect to host. {}'.format(str(e)))
    try:
        cursor = conn.cursor()
        return cursor
    except Exception as e:
        print(e)
        return False

def get_record_info(cursor) -> list:
        print('Building_record_info_out_tables: ')
        recordInfo = list()
        #record_info_out = Sdk.RecordInfo(self.alteryx_engine)  # A fresh record info object for outgoing records.
        try:  # Add metadata info that is passed to tools downstream.
            for row in cursor.description:
                recordInfo.append(row[0])
        except Exception as e:
            print('Building_record_info_out_tables failed. {}}'.format(str(e)))
        return recordInfo               

def get_tables(cursor) -> list:
    start = datetime.datetime.now()
    table = list()
    dataOut = list()

    # Init tables cursor
    try:
        TABLES = cursor.tables(table=None, catalog=None, schema=None, tableType=None)
    except Exception as e:
        print('Cannot open cursor. {}'.format(str(e)))

    while table is not None:
        try:
            table = TABLES.fetchone()
            currentRow = list()
            if table is not None:
                for i in range(0,len(table)):
                    currentRow.append(str(table[i])) 
                dataOut.append(currentRow)    
                #print(dataOut)
                #currentRow.clear()
                
        except Exception as e:
            print('Load table failed. {}'.format(str(e)))
            return False

    end = datetime.datetime.now()
    print('Get all tables data took: @1', str(end - start))
    return dataOut

def getTableType(type) -> str:
    if type == 'TABLE':
        return 'T'
    if type == 'VIEW':
        return 'V'
    if type == 'PROCEDURE':
        return 'P'    
    return False    


##################################################
# MAIN
##################################################
ENGINE = 'mysql'
LOAD_ID = 'pyTest'+'_'+ENGINE
SRC_CURSOR = init_cursor(SRC_CONSTR, SRC_AUTOCOMMIT)
TGT_CURSOR = init_cursor(TGT_CONSTR, TGT_AUTOCOMMIT)

# Get tables meta field names
# fieldNames = get_record_info(CURSOR)

# Get All Tables Data
dataIn = get_tables(SRC_CURSOR)

"""
print('FIELD NAMES:')
print(fieldNames)
print('RESULT DATA:')
print(str(dataOut))
"""

TGT_CURSOR.execute("delete from rdbms_stage.db_tables where loadid = '{}'".format(LOAD_ID))

timestamp = calendar.timegm(datetime.datetime.now().timetuple())

for row in dataIn:
    sql = list()
    sql.append("insert into rdbms_stage.db_tables (loadid, tstamp, engine_name, server_name, table_name, table_type, table_comment, catalog_name, schema_name) values (")
    sql.append("'{0}',{1},'{2}','{3}',".format(LOAD_ID,str(timestamp),ENGINE,SERVER_NAME))
    sql.append("'{0}','{1}','{2}','{3}','{4}'".format(row[2],getTableType(row[3]),row[4],'def',row[0]))
    sql.append(")")
    
    sqlQuery = ''.join(sql)
    print(sqlQuery)

    TGT_CURSOR.execute(sqlQuery)

TGT_CURSOR.execute('commit')
asdasdasdasdasdasdsadadasdasdsadasdadsasdasdasdasdsasad
SRC_CURSOR.close()
TGT_CURSOR.close()






