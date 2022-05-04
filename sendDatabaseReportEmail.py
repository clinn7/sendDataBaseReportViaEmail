from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.sql import text
from sqlalchemy import select, and_
from envelope import Envelope
import configparser
import subprocess as sp
import pyodbc
import pandas as pd

def engineCreation(username, password1, server, database):
    dataConnect = pyodbc.connect("DRIVER={SQL Server}; SERVER=" +server+";DATABASE="+database+";UID="+username+";PWD="+ password1)
    return dataConnect

def createTableFiles(df1, df2, df3):
    print(df1.head(20))
    #tsv/csv file names and paths to edit below
    df1.to_csv('query1TableName.tsv', sep = '\t', index=False)
    df2.to_csv('query2TableName.tsv', sep='\t', index=False)
    df3.to_csv('query3TableName.csv')
    print('Files created')
    # sp.Popen([filePath1], shell=True)
    # sp.Popen([filePath2], shell=True)

def createQueryMessage(df3):
    space = '&nbsp;'
    tableDictionary = {}
    tableList = []
    messageTable = ""
    for index, row in df3.iterrows():
        key = row['columnHeader2']
        val = row['columnHeader3']
        tableDictionary[key] = val
        messageTable = str(messageTable) + str(key) + ": " + space*(40-int((len(str(key))))) + str(tableDictionary[key]) + "<br>"
    print(messageTable)
    return messageTable, tableDictionary, tableList

def message(filePath1, filePath2, host, port, user, password, recipient1, recipient2, messageTable):
    Envelope()\
        .from_(user)\
        .subject("Report Email Subject")\
        .to(recipient1).to(recipient2) \
        # Envelope package uses HTML for formatting messages, also used in createQueryMessage above
        .message("Tables meeting query3 requirements (columnHeader3 conditions specified met):"
                 + "<br>" +"<br>" + '<font face="Consolas" size="12px" color="000000">' + str(messageTable) + '</font>')\
        .attach(path=filePath1).attach(path=filePath2)\
        .smtp(host, port, user, password, "starttls")\
        .send()

def main():
    filePath1 = "filePath/Including/TsvFileForQuery1/query1TableName.tsv"
    filePath2 = "filePath/Including/TsvFileForQuery2/query2TableName.tsv"
    filePath3 = "filePath/Including/CsvFileForQuery3/query3TableName.csv"
    configFileName = "filePath/Including/configFile/sampleConfig.CONFIG"
    config = configparser.ConfigParser()
    config.read(configFileName)
    #config file name and path to edit below
    username = config['LOGIN']['user']
    password1 = config['LOGIN']['password']
    server = config['SERVER']['server']
    database = config['DATABASE']['database']
    host = config['SMTP']['host']
    port = config['SMTP']['port']
    user = config['MAILBOX']['user']
    password = config['MAILBOX']['password']
    recipient1 = config['ADDRESSES']['recipient1']
    recipient2 = config['ADDRESSES']['recipient2']
    dataConnect = engineCreation(username, password1, server, database)
    print('Data connection confirmed')
    #database queries converted to pandas dataframes below
    query1 = 'SELECT * FROM [dataBaseName].table1'
    df1 = pd.read_sql(query1, dataConnect)
    query2 = 'SELECT * FROM [dataBaseName].table2'
    df2 = pd.read_sql(query2, dataConnect)
    #change query below to callout rows meeting certain conditions/requirement, must also edit loop in createQueryMessage
    query3 = 'SELECT a.columnHeader1, a.columnHeader2, a.columnHeader3, a.columnHeader4  ' \
             'FROM [dataBaseName].table1 a WHERE a.columnHeader3 > 8000' #can change comparison operator/value here
    df3 = pd.read_sql(query3, dataConnect)
    print('Queries confirmed')
    messageTable, tableDictionary, tableList = createQueryMessage(df3=df3)
    tableInMessage = createQueryMessage(df3=df3)
    print('Defined table in message')
    createTableFiles(df1=df1, df2=df2, df3=df3)
    print('Created files')
    message(filePath1=filePath1, filePath2=filePath2, host=host, port=port, user=user, password=password, recipient1=recipient1,
            recipient2=recipient2, messageTable=messageTable)
    print(df3.to_string(index=False, col_space='20', justify='center'))
    print("Message sent")

main()
