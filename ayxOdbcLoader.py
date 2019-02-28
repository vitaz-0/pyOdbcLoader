import pyodbc
import datetime
import calendar


SRC_CONSTR = 'Driver={MySQL ODBC 5.3 Unicode Driver};DATABASE=employees;Server=localhost;Option=3;Port=3306;UID=vzak;PWD=Pra1234gue'
SRC_AUTOCOMMIT = True

TGT_CONSTR = 'DRIVER={PostgreSQL Unicode(x64)};DATABASE=ngp;UID=ngp;PWD=wombat-wookie-charleston;SERVER=localhost;PORT=5431'
TGT_AUTOCOMMIT = False

SERVER_NAME = 'localhost'

BATCH_SIZE = 2

INSERT_STATEMENTS = {
    "tables": "insert into rdbms_stage.db_tables (loadid, tstamp, engine_name, server_name, table_name, table_type, table_comment, catalog_name, schema_name) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    "columns": ""
}

RDBMS_STAGE_TABLES = {
    "tables": "db_tables",
    "columns": "db_columns"
}


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
    #print('Building_record_info_out_tables: ')
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
    tables = init_src_cursor_query(cursor.tables(table=None, catalog=None, schema=None, tableType=None))

    while table is not None:
        try:
            table = tables.fetchone()
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

def data_extract_transform(cursor):    
    try:
        dataIn = cursor.fetchmany(BATCH_SIZE)
        if not dataIn:
            return None
    except Exception as e:
        print('Method data_extract_transform FAILED. {}'.format(str(e)))
        return False
    return dataIn   

def data_transform_tables(dataIn, recordInfo):
    dataOut = list()
    try:
        for row in dataIn:
            dataOutRow = list()
            # insert into rdbms_stage.db_tables (loadid, tstamp, engine_name, server_name, table_name, table_type, table_comment, catalog_name, schema_name)
            dataOutRow.extend([LOAD_ID, str(calendar.timegm(datetime.datetime.now().timetuple())), ENGINE, SERVER_NAME])
            dataOutRow.extend([row[recordInfo['table_name']], getTableType(row[recordInfo['table_type']]), row[recordInfo['remarks']], 'def', row[recordInfo['table_schem']]])
            dataOut.append(dataOutRow)
            # print(dataOutRow)
    except Exception as e:
        print('Method data_transform_tables FAILED. {}'.format(str(e)))
        return False    

    return dataOut    

def data_transform_columns(dataIn, recordInfo):
    dataOut = list()
    return dataOut  

def data_load(cursor, dataOut:list, loadType:str):
    try:
        cursor.executemany(INSERT_STATEMENTS[loadType], dataOut)
    except Exception as e:
        print('Method data_transform_tables FAILED. {}'.format(str(e)))    

def data_etl_rollback(src_cursor, tgt_cursor):
    try:
        cursor.executemany(INSERT_STATEMENTS[loadType], dataOut)
    except Exception as e:
        print('Rollback FAILED. {}'.format(str(e)))

def data_etl(src_cursor, tgt_cursor, loadType:str):

    dataIn = list()
    dataOut = list()

    start = datetime.datetime.now()

    try:
        tables, recordInfo = init_src_cursor_query(src_cursor.tables(table=None, catalog=None, schema=None, tableType=None))
        
        # Cleanup target rdbms stage table
        tgt_cursor.execute("delete from rdbms_stage.{} where loadid = '{}'".format(RDBMS_STAGE_TABLES[loadType],LOAD_ID)) # table name
        
        while True:
            dataOut = list()

            dataIn = data_extract_transform(tables)
            if not dataIn:
                break
            
            if loadType == 'tables':
                dataOut = data_transform_tables(dataIn, recordInfo)    
            if loadType == 'columns':
                dataOut = data_transform_columns(dataIn, recordInfo)

            print('DATA OUT: ')
            print(dataOut)
            data_load(tgt_cursor, dataOut, loadType)

        tgt_cursor.execute('commit')
        
        end = datetime.datetime.now()
        print('process data_etl finished. Getting {} data took: {}'.format(loadType.upper(), str(end - start)))  
    
    except Exception as e:
        print('Process data_etl failed. Processing ROLLBACK')
        data_etl_rollback(tgt_cursor)
        src_cursor.close()
        tgt_cursor.close()


##################################################
# MAIN
##################################################
ENGINE = 'mysql'
LOAD_ID = 'pyTest'+'_'+ENGINE
SRC_CURSOR = init_cursor(SRC_CONSTR, SRC_AUTOCOMMIT) # add: TABLES
TGT_CURSOR = init_cursor(TGT_CONSTR, TGT_AUTOCOMMIT)

data_etl(SRC_CURSOR, TGT_CURSOR, 'tables') 
#data_etl(SRC_CURSOR, TGT_CURSOR, 'columns') 

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





