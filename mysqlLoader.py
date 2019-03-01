import configparser

SRC_CONSTR = 'Driver={MySQL ODBC 5.3 Unicode Driver};DATABASE=employees;Server=localhost;Option=3;Port=3306;UID=vzak;PWD=Pra1234gue'
#SRC_CONSTR = 'Driver={PostgreSQL Unicode(x64)};Servername=postgresql.alteryx-loaders.cloud;Port=5432;Database=postgres'
SRC_AUTOCOMMIT = True

TGT_CONSTR = 'DRIVER={PostgreSQL Unicode(x64)};DATABASE=ngp;UID=ngp;PWD=wombat-wookie-charleston;SERVER=localhost;PORT=5431'
TGT_AUTOCOMMIT = False
SERVER_NAME = 'localhost'
BATCH_SIZE = 100

ENGINE = 'mysql'
LOAD_ID = 'pyTest'+'_'+ENGINE

srcCursor = ayxOdbcLoader.init_cursor(SRC_CONSTR, SRC_AUTOCOMMIT) # add: TABLES
tgtCursor = ayxOdbcLoader.init_cursor(TGT_CONSTR, TGT_AUTOCOMMIT)

ayxOdbcLoader.data_etl(srcCursor.tables(table=None, catalog=None, schema=None, tableType=None), tgtCursor, 'tables') 
# Takhle by to bylo pres COLUMNS: data_etl(SRC_CURSOR.columns(table=None, catalog=None, schema=None, column=None), TGT_CURSOR, 'columns') 
ayxOdbcLoader.data_etl(srcCursor.execute('SELECT table_catalog, table_schema, table_name, column_name, column_default, is_nullable, data_type  FROM information_schema.columns'), tgtCursor, 'columns')

srcCursor.close()
tgtCursor.close()



