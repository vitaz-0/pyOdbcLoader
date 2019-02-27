import pyodbc
import datetime

CONSTR = 'Driver={MySQL ODBC 5.3 Unicode Driver};DATABASE=employees;Server=localhost;Option=3;Port=3306;UID=vzak;PWD=Pra1234gue'
AUTOCOMMIT = True

CURSOR = None
TABLES = None


def init():
    try:
        conn = pyodbc.connect(CONSTR,autocommit=AUTOCOMMIT)
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

def build_record_info_out_tables(cursor) -> list:
        print('Building_record_info_out_tables: ')
        recordInfo = list()
        #record_info_out = Sdk.RecordInfo(self.alteryx_engine)  # A fresh record info object for outgoing records.
        try:  # Add metadata info that is passed to tools downstream.
            for row in cursor.description:
                recordInfo.append(row[0])
        except Exception as e:
            print('Building_record_info_out_tables failed. {}}'.format(str(e)))
        return recordInfo               

CURSOR = init()

fieldNames = build_record_info_out_tables(CURSOR)

# Get all tables
start = datetime.datetime.now()
try:
    TABLES = CURSOR.tables(table=None, catalog=None, schema=None, tableType=None)
except Exception as e:
    print('Cannot open cursor. {}'.format(str(e)))

end = datetime.datetime.now()
print('Read tables metadata took: @1', str(end - start))

start = datetime.datetime.now()

#loadedSchemas = list()
table_schema = list()
table = list()
currentRow = list()
dataOut = list()

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
        break

end = datetime.datetime.now()

print('RESULT')
print(fieldNames)
print(str(dataOut))
print('Read columns metadata took: @1', str(end - start))
