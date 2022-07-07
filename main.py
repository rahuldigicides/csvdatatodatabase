import os
import json
import pymysql 
import csv
from fastapi import FastAPI,  File, UploadFile
from fastapi.responses import FileResponse
import pandas as pd


# importing the module
import timeit


app = FastAPI() 

# ! database to enter csv data 
db_name1 = 'TkPan27E9q'
db_user1= 'TkPan27E9q'
db_password1 = 'kXjf5HCrM1'
db_host1 = 'remotemysql.com'
db_port1 = str(os.getenv('DB_PORT'))




# ! database get data and convert into Csv
db_name = 'digicides'
db_user= 'kingadmin'
db_password = '12345678'
db_host = 'database-1.czaglb0mlalx.ap-south-1.rds.amazonaws.com'
db_port = str(os.getenv('DB_PORT'))


filename="zthree.csv"
fieldname1= ["id","email","password","role","reporting","status","company_id","blocked","deleted","phone","name","created_at","updated_at"]

# Write the CSV File
def csvFun(rows):
    # with open (filename ,'w') as csvfile:
    # print("=======================================", rows) 
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
            # print("=======================================", result)
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
              
                id = None if pd.isna(row.id) ==True  else row.id 
                password = None if pd.isna(row.password) ==True  else  row.password
                email = None if pd.isna(row.email) ==True  else row.email
                role = None if pd.isna(row.role) ==True  else row.role
                ustatus = None if pd.isna(row.status) ==True  else row.status
                company_id = None if pd.isna(row.company_id) ==True  else row.company_id
                reporting =None if pd.isna(row.reporting) ==True  else row.reporting
                blocked =None if pd.isna(row.blocked) ==True  else row.blocked
                deleted =None if pd.isna(row.deleted) ==True  else row.deleted 
                name=None if pd.isna(row.name) ==True  else row.name 
                phone=None if pd.isna(row.phone) ==True  else row.phone
                created_at=None if pd.isna(row.created_at) ==True  else row.created_at
                updated_at=None if pd.isna(row.updated_at) ==True  else row.updated_at

                cur.execute('INSERT INTO users VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s ,%s,%s,%s)',
                            (id, email, password, role, reporting, ustatus,company_id, blocked, deleted,name,phone,created_at,updated_at))                 
              
                print(f"Record inserted  {i} ")
                dbCon.commit()
                
           
    finally:
       dbCon.close()
    return {"data" : "data set"}

start = timeit.default_timer()

@app.post("/uploadfile")
async def uploadCsv(csv_file : UploadFile =File(...)) :
  df = pd.read_csv(csv_file.file) 
  csvtodatabse(df)
  
  print(" rows line ", df.shape[0])
  if df.shape[0] !=None:
    return {"status":"Sucess"}
  elif df.shape[0] ==None:
    return {"status":"Sucess"}
  else:
    return {"status":"Error"}

end = timeit.default_timer()

# t= timeit.timeit(lambda:uploadCsv(), number=1)
print(f"time execute for upload csv {end-start}")