import pyodbc
import datetime
import calendar


SRC_CONSTR = 'Driver={MySQL ODBC 5.3 Unicode Driver};DATABASE=employees;Server=localhost;Option=3;Port=3306;UID=vzak;PWD=Pra1234gue'
SRC_AUTOCOMMIT = True

TGT_CONSTR = 'DRIVER={PostgreSQL Unicode(x64)};DATABASE=ngp;UID=ngp;PWD=wombat-wookie-charleston;SERVER=localhost;PORT=5431'
TGT_AUTOCOMMIT = False

SERVER_NAME = 'localhost'

BATCH_SIZE = 2

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

def init_src_cursor_query(cursorMethod):    
    try:
        retCursor = cursorMethod
    except Exception as e:
        print('Cannot open SOURCE cursor. {}'.format(str(e))) 
    try:
        recordInfo = get_record_info(retCursor)
    except Exception as e:
        print('Cannot get record info. {}'.format(str(e)))     
    return retCursor, recordInfo

def get_record_info(cursor) -> dict:
    print('Building_record_info_out_tables: ')
    recordInfo = list()
    #record_info_out = Sdk.RecordInfo(self.alteryx_engine)  # A fresh record info object for outgoing records.
    try:  # Add metadata info that is passed to tools downstream.
        for row in cursor.description:
            recordInfo.append(row[0])
    except Exception as e:
        print('Building_record_info_out_tables failed. {}}'.format(str(e)))
    return dict(zip(recordInfo,range(len(recordInfo))))               

def get_tables(cursor) -> list:
    start = datetime.datetime.now()
    table = list()
    dataOut = list()

    # Init tables cursor
    """
    try:
        TABLES = cursor.tables(table=None, catalog=None, schema=None, tableType=None)
    except Exception as e:
        print('Cannot open cursor. {}'.format(str(e)))
    """
    init_src_cursor_query(cursor.tables(table=None, catalog=None, schema=None, tableType=None))

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


def get_tables_many(cursor, tgt_cursor):
    start = datetime.datetime.now()
    table = list()
    dataIn = list()
    dataOut = list()

    # Init tables cursor
    try:
        TABLES = cursor.tables(table=None, catalog=None, schema=None, tableType=None)
    except Exception as e:
        print('Cannot open SOURCE cursor. {}'.format(str(e))) 

    recordInfo = get_record_info(cursor)

    while True:
        try:
            dataIn = TABLES.fetchmany(BATCH_SIZE)
            if not dataIn:
                break
        except Exception as e:
            print('Load table MANY failed. {}'.format(str(e)))
            return False
    
        for row in dataIn:
            #dataOut = list()
            dataOutRow = list()
            # insert into rdbms_stage.db_tables (loadid, tstamp, engine_name, server_name, table_name, table_type, table_comment, catalog_name, schema_name)
            dataOutRow.extend([LOAD_ID, str(calendar.timegm(datetime.datetime.now().timetuple())), ENGINE, SERVER_NAME])
            dataOutRow.extend([row[recordInfo['table_name']], getTableType(row[recordInfo['table_type']]), row[recordInfo['remarks']], 'def', row[recordInfo['table_schem']]])
            dataOut.append(dataOutRow)
            print(dataOutRow)
        
    print('DATAOUT:')
    print(dataOut)
    tgt_cursor.executemany("insert into rdbms_stage.db_tables (loadid, tstamp, engine_name, server_name, table_name, table_type, table_comment, catalog_name, schema_name) values (?, ?, ?, ?, ?, ?, ?, ?, ?)", dataOut)


    end = datetime.datetime.now()
    print('Get many tables data took: {}'.format(str(end - start)))  
    #print(dataIn)





##################################################
# MAIN
##################################################
ENGINE = 'mysql'
LOAD_ID = 'pyTest'+'_'+ENGINE
SRC_CURSOR = init_cursor(SRC_CONSTR, SRC_AUTOCOMMIT) # add: TABLES
TGT_CURSOR = init_cursor(TGT_CONSTR, TGT_AUTOCOMMIT)

# Get tables meta field names
# fieldNames = get_record_info(CURSOR)

# Get All Tables Data
#dataIn = get_tables(SRC_CURSOR)
TGT_CURSOR.execute("delete from rdbms_stage.db_tables where loadid = '{}'".format(LOAD_ID)) # table name
get_tables_many(SRC_CURSOR, TGT_CURSOR) # cursor -> tables, columns
TGT_CURSOR.execute('commit')
SRC_CURSOR.close()
TGT_CURSOR.close()












"""
print('FIELD NAMES:')
print(fieldNames)
print('RESULT DATA:')
print(str(dataOut))
"""
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

SRC_CURSOR.close()
TGT_CURSOR.close()
"""





