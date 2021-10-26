import pyodbc
import pandas as pd
from fast_to_sql import fast_to_sql as fts

def sql_insert(server_name,db_name,uname,pword,dataframe,table):
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server_name+';DATABASE='+db_name+';Uid='+uname+';Pwd='+pword+';Encrypt=no;Trusted_Connection=no;Connection Timeout=30; autocommit=True')
    cursor = cnxn.cursor()
    create_statement = fts.fast_to_sql(dataframe, table, cnxn , if_exists="append",  temp=False)
    cursor.close()
    cnxn.commit()
    cnxn.close()
    
def sql_read(server_name,db_name,uname,pword,query):
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server_name+';DATABASE='+db_name+';Uid='+uname+';Pwd='+pword+';Encrypt=no;Trusted_Connection=no;Connection Timeout=30; autocommit=True')
    cursor = cnxn.cursor()   
    df= pd.read_sql_query(query, cnxn)
    cursor.close()
    cnxn.commit()
    cnxn.close()
    return df



def sql_query(server_name,db_name,uname,pword,query):
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server_name+';DATABASE='+db_name+';Uid='+uname+';Pwd='+pword+';Encrypt=no;Trusted_Connection=no;Connection Timeout=30; autocommit=True')
    cursor = cnxn.cursor()   
    cursor.execute(query)
    cursor.close()
    cnxn.commit()
    cnxn.close()
    