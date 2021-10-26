import logging
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, BlobBlock, ContainerClient, PublicAccess
import pandas as pd
import datetime,time
import traceback
from pandas.core.frame import DataFrame
import sys

try:
    from SamsaraEcoDriving.etl import extractor,calc,etl
    import SamsaraEcoDriving.db as db
except:
    from etl import extractor,etl
    import db as db




def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")



    
    edwstoragefiles_connectionString   = os.environ['inputfiles_connectionString']
    #edwstoragefiles_connectionString   =  'DefaultEndpointsProtocol=https;AccountName=edwstoragefiles;AccountKey=kXlGjFmH3Fbj1/JeG9ucFRCrulW4ZKX4pN3do8VwC6oTup30dYKLSKMxk0x4aAkDcIw3yf820Jp9Cu7K9+SVcw==;EndpointSuffix=core.windows.net'

    inputfiles_container_name = 'samsaraecodriving'
    inputfiles_container = ContainerClient.from_connection_string(conn_str=edwstoragefiles_connectionString , container_name="samsaraecodriving")

    
    #ss
    link_service = 'https://edwstoragefiles.blob.core.windows.net/' 
    link_container = 'samsaraecodriving/'

    link_blob_name = link_service +  myblob.name    #blob.name
    blob_name = str(myblob.name).replace('samsaraecodriving/','').replace('.pdf','')
    blob_name_pdf = str(myblob.name).replace('samsaraecodriving/','')
    print('name of the file is: '+str(blob_name))
    print('name of the http blob name is: '+str(link_blob_name))
    print('name of pdf blob is: '+str(blob_name_pdf))
    #pd.read_csv(link_blob_name)

    edw_db_name = os.environ['edw_db_name']
    rls_server_name = os.environ['rls_server_name']
    af_uname = os.environ['af_uname']
    af_pword = os.environ['af_pword']

    startDate = (pd.to_datetime(str(datetime.datetime.strptime(extractor(myblob.name ,0), "%b").month)+'-'+extractor(myblob.name ,1)+'-'+extractor(myblob.name ,2))).date()
    finishDate = (pd.to_datetime(str(datetime.datetime.strptime(extractor(myblob.name ,4), "%b").month)+'-'+extractor(myblob.name,5)+'-'+extractor(myblob.name,6))).date()

    try:

    #TRANSFORMATION
    
        df = pd.read_csv(link_blob_name)
        df['startDate'] = startDate
        df['finishDate'] =  finishDate

        df = etl(df=df)
        datediff =((datetime.datetime.now()).date() -startDate).days


        #inputfiles_container.delete_blob(blob_name_pdf,delete_snapshots="include" )
    #INSERT TO SQL
        if datediff < 5:
            db.sql_insert(rls_server_name,edw_db_name,af_uname,af_pword,df = df,table = 'SamsaraEcoDriving')
            print('data inserted succcesfully')

    #INSERTO TO SQL IF OLDER THAN 5 DAYS
        else:

            db.sql_insert(rls_server_name,edw_db_name,af_uname,af_pword,df = df,table = 'SamsaraEcoDrivingCheck')
            print('older data inserted succcesfully')

    # DELETE INPUT BLOB
        inputfiles_container.delete_blob(blob_name_pdf,delete_snapshots="include" )

    except Exception as e:
        print('error delete')
        print('blob name: '+str(myblob.name))
        
        insertDt = datetime.datetime.now()
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        print("Exception type: ", exception_type)
        print("File name: ", filename)
        print("Line number: ", line_number)
        trackback_eoror_message = "blob name: "+str(myblob.name)+' error message: '+str("Exception type: ", exception_type, " File name: ", filename,"Line number: ", line_number)
        functionName = 'samsaraEcoDrivingReport'
        table = 'samsaraEcoDriving'
        error_row = [functionName,table,trackback_eoror_message,insertDt]
        error_row  = pd.DataFrame(error_row).T
        
        #SEND ERROR LOG TO SQL TABLE
        db.sql_insert(rls_server_name,edw_db_name,af_uname,af_pword,error_row, "azurefunctionLogs")

        #SEND not converted file to INPUTERROR CONTAIER
        

        #DELETE INPUT BLOB
        #inputfiles_container.delete_blob(blob_name_pdf,delete_snapshots="include" )

        print('process broken  - check azureFunctionLogs SQL talbe ')


    #DELETE BLOB
        inputfiles_container.delete_blob(blob_name_pdf,delete_snapshots="include" )
    time.sleep(1)
        


