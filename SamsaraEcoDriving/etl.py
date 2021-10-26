import datetime
import re
import pandas as pd
def calc(x):
    p1 = x.split(' ')[0]
    if 'm' in p1:
        p1_int = int(re.findall('[0-9]+', x)[0]) * 60
    elif 'h' in p1:
        p1_int = int(re.findall('[0-9]+', x)[0]) * 3600
    elif 's' in p1:
        p1_int = int(re.findall('[0-9]+', x)[0])      
    else:
        p1_int = 0   
        
    p2 = x.split(' ')[1]
    if 'm' in p2:
        p2_int = int(re.findall('[0-9]+', x)[1]) * 60
    elif 'h' in p2:
        p2_int = int(re.findall('[0-9]+', x)[1])  * 3600
    elif 's' in p2:
        p2_int = int(re.findall('[0-9]+', x)[1])      
    else:
        p2_int = 0

    return p1_int  + p2_int


def extractor(string,element):
    dt = re.sub(r'^.*?__', '',string.upper()).replace('.CSV','').split('_')[element]
    return dt



def etl(df):
    
    drop_cols = ['Driver','Anticipation','startDate','finishDate']
    calc_cols = df.columns.drop(drop_cols)
    for col in calc_cols:
    #col_name = col+'_sec'
    #print(col_name)
        df[col] = df[col].apply(calc)
    df.loc[:, df.columns != 'Driver'].sort_index(axis=1, inplace=True)

    list_col = sorted(df.columns)
    for col in drop_cols:
        list_col.remove(col)
    list_col_fin =  drop_cols + list_col

    df = df[list_col_fin]
    df.columns =  [  i.replace(' ','') for  i in list_col_fin ]

    df['insertDt'] =  datetime.datetime.now()
    #df['insertDiff'] =  (datetime.datetime.now() -startDate).days

    return df