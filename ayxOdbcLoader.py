import pyodbc
import datetime
import calendar

INSERT_STATEMENTS = {
    "tables": "insert into rdbms_stage.db_tables (loadid, tstamp, engine_name, server_name, table_name, table_type, table_comment, catalog_name, schema_name) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    "columns": "INSERT INTO rdbms_stage.db_columns (loadid, tstamp, engine_name, server_name, catalog_name, schema_name, table_name, column_name, is_nullable, column_default, data_type) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
}

RDBMS_STAGE_TABLES = {
    "tables": "db_tables",
    "columns": "db_columns"
}


##SRC_CONSTR = 'Driver={MySQL ODBC 5.3 Unicode Driver};DATABASE=employees;Server=localhost;Option=3;Port=3306;UID=vzak;PWD=Pra1234gue'
#SRC_CONSTR = 'Driver={PostgreSQL Unicode(x64)};Servername=postgresql.alteryx-loaders.cloud;Port=5432;Database=postgres'
SRC_AUTOCOMMIT = True

TGT_CONSTR = 'DRIVER={PostgreSQL Unicode(x64)};DATABASE=ngp;UID=ngp;PWD=wombat-wookie-charleston;SERVER=localhost;PORT=5431'
TGT_AUTOCOMMIT = False
SERVER_NAME = 'localhost'
BATCH_SIZE = 100

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

def init_cursor_method(cursorMethod):    
    try:
        retCursor = cursorMethod
    except Exception as e:
        print('Cannot open cursor. {}'.format(str(e))) 
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

def get_table_type(type) -> str:
    if type == 'TABLE':
        return 'T'
    if type == 'VIEW':
        return 'V'
    if type == 'PROCEDURE':
        return 'P'    
    return False    

def get_is_nullable(isNullable) -> str:
    if isNullable.upper() == 'YES':
        return True
    if isNullable.upper() == '1':
        return True    
    if isNullable.upper() == 'Y':
        return True    
    if isNullable.upper() == 'NO':
        return False
    if isNullable.upper() == 'N':
        return False  
    if isNullable.upper() == '0':
        return False 
    return True   

def data_extract(cursor):    
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
            dataOutRow.extend([row[recordInfo['table_name']], get_table_type(row[recordInfo['table_type']]), row[recordInfo['remarks']], 'def', row[recordInfo['table_schem']]])
            dataOut.append(dataOutRow)
            #print(dataOutRow)
            #print(dataOut)
    except Exception as e:
        print('Method data_transform_tables FAILED. {}'.format(str(e)))
        return False    
    return dataOut    

def data_transform_columns(dataIn, recordInfo):
    dataOut = list()
    try:
        for row in dataIn:
            dataOutRow = list()
            #SELECT table_catalog, table_schema, table_name, column_name, column_default, is_nullable, data_type  FROM information_schema.columns
            # INSERT INTO rdbms_stage.db_columns (loadid, tstamp, engine_name, server_name, catalog_name, schema_name, table_name, column_name, is_nullable, column_default, data_type)
            dataOutRow.extend([LOAD_ID, str(calendar.timegm(datetime.datetime.now().timetuple())), ENGINE, SERVER_NAME])
            dataOutRow.extend([row[recordInfo['table_catalog']], row[recordInfo['table_schema']], row[recordInfo['table_name']], row[recordInfo['column_name']], get_is_nullable(row[recordInfo['is_nullable']]), row[recordInfo['column_default']], row[recordInfo['data_type']]])
            #dataOutRow.extend([row[recordInfo['ordinal_position']], row[recordInfo['num_prec_radix']], row[recordInfo['decimal_digits']], row[recordInfo['char_octet_length']], row[recordInfo['remarks']]])
            dataOut.append(dataOutRow)
            #print(dataOutRow)
            #print(dataOut)
            #print(row)
    except Exception as e:
        print('Method data_transform_columns FAILED. {}'.format(str(e)))
        return False    
    return dataOut     

def data_load(cursor, dataOut:list, loadType:str):
    try:
        cursor.executemany(INSERT_STATEMENTS[loadType], dataOut)
    except Exception as e:
        print('Method data_transform_columns FAILED. {}'.format(str(e)))    

def data_etl_rollback(src_cursor, tgt_cursor):
    try:
        src_cursor.close()
        if tgt_cursor == None:
            tgt_cursor.execute('rollback')
            tgt_cursor.close()
    except Exception as e:
        print('Rollback FAILED. {}'.format(str(e)))

def data_etl(src_cursor_method, tgt_cursor, loadType:str):

    dataIn = list()
    dataOut = list()

    start = datetime.datetime.now()

    try:
        dataInCursor, recordInfo = init_cursor_method(src_cursor_method)

        # Cleanup target rdbms stage table
        tgt_cursor.execute("delete from rdbms_stage.{} where loadid = '{}'".format(RDBMS_STAGE_TABLES[loadType],LOAD_ID)) # table name
        
        while True:
            dataOut = list()

            dataIn = data_extract(dataInCursor)
            if not dataIn:
                break
            
            if loadType == 'tables':
                dataOut = data_transform_tables(dataIn, recordInfo)    
            if loadType == 'columns':
                dataOut = data_transform_columns(dataIn, recordInfo)

            if dataOut == False:
                break

            #print('DATA OUT: ')
            #print(dataOut)
            data_load(tgt_cursor, dataOut, loadType)

        tgt_cursor.execute('commit')
        
        end = datetime.datetime.now()
        print('process data_etl finished. Getting {} data took: {}'.format(loadType.upper(), str(end - start)))  
    
    except Exception as e:
        print('Process data_etl failed. Processing ROLLBACK. {}'.format(str(e)))
        data_etl_rollback(src_cursor_method, tgt_cursor)


