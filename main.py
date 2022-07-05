import os
import json
import pymysql 
import csv
from fastapi import FastAPI,  File, UploadFile
from fastapi.responses import FileResponse
import pandas as pd


app = FastAPI() 

# ! CSV FILE DATA ADD IN DATABASE
db_name1="sql8502670"
db_user1="sql8502670"
db_password1="RtB3fuD9pJ"
db_host1="sql8.freesqldatabase.com"
db_port1="3306"

# ! database get data and convert into Csv
# db_name = 'digicides'
# db_user= 'kingadmin'
# db_password = '12345678'
# db_host = 'database-1.czaglb0mlalx.ap-south-1.rds.amazonaws.com'
# db_port = str(os.getenv('DB_PORT'))

# name of csv file
filename = "zthree.csv"
fieldname1= ["userid","email","password","role","reporting","status","company","blocked","deleted","phone","name"]

# Write the CSV File
def csvFun(rows):
    # with open (filename ,'w') as csvfile: 
    with open(filename , 'w' , encoding='UTF8', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldname1, delimiter=',',)
        # ! Header and Rows of Csv Data 
        writer.writeheader() 
        writer.writerows(rows)

def dtbse(query=None):
    dbCon = pymysql.connect(host=db_host1, user=db_user1, password=db_password1, database=db_name1, charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)

    try :
        with dbCon.cursor() as cur: 
            # cur.execute(query)
            cur.execute('select * from users')
            result =  cur.fetchall()
            dbCon.commit()
            # ! Csv Function Call
            csvFun(result)
            
    finally:

        dbCon.close()
    return {"data" : "sucees"}



@app.get("/download")
def downloadCsvFile():
    dtbse() 
    return  FileResponse(path=filename, media_type='application/octet-stream', filename="csvFile")







# ! csv to database 
def csvtodatabse(files):
    dbCon = pymysql.connect(host=db_host1, user=db_user1, password=db_password1, database=db_name1, charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)
    
    try :
        with dbCon.cursor() as cur: 
           
            for i , row in files.iterrows():
              
                userid = None if pd.isna(row.userid) ==True  else row.userid 
                password = None if pd.isna(row.password) ==True  else  row.password
                email = None if pd.isna(row.email) ==True  else row.email
                role = None if pd.isna(row.role) ==True  else row.role
                ustatus = None if pd.isna(row.status) ==True  else row.status
                company = None if pd.isna(row.company) ==True  else row.company
                reporting =None if pd.isna(row.reporting) ==True  else row.reporting
                blocked =None if pd.isna(row.blocked) ==True  else row.blocked
                deleted =None if pd.isna(row.deleted) ==True  else row.deleted 
                name=None if pd.isna(row.name) ==True  else row.name 
                phone=None if pd.isna(row.phone) ==True  else row.phone
                cur.execute('INSERT INTO users VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s ,%s)',
                            (userid, email, password, role, reporting, ustatus,company, blocked, deleted,name,phone))                 
              
                # print("Record inserted")
                dbCon.commit()
                
           
    finally:
       dbCon.close()
    return {"data" : "data set"}



@app.post("/uploadfile")
async def uploadCsv(csv_file : UploadFile =File(...)) :
  df = pd.read_csv(csv_file.file) 
  csvtodatabse(df)
 
  if df.shape[0] !=None:
    return {"status":"Sucess"}
  elif df.shape[0] ==None:
    return {"status":"Sucess"}
  else:
    return {"status":"Error"}

