import os
import json
import pymysql 
import csv
from fastapi import FastAPI,  File, UploadFile, Header, Depends
from fastapi.responses import FileResponse
import pandas as pd
import sys
from tempfile import NamedTemporaryFile



# importing the module
import timeit


async def valid_content_length(content_length: int = Header(..., lt=90_000)):
    # print(content_length)
    # print(type(content_length))
    return content_length

app = FastAPI() 

# ! database to enter csv data 
db_name1 = 'TkPan27E9q'
db_user1= 'TkPan27E9q'
db_password1 = 'HzopKYQkwc'
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


@app.post("/uploadfile", dependencies=[Depends(valid_content_length)])
async def uploadCsv(csv_file : UploadFile =File(...)) :

    df = pd.read_csv(csv_file.file) 
   
  
    csvtodatabse(df)
    
    if df.shape[0] !=None:
       return {"status":"Sucess"}
    else:
     return {"status":"Error"}





# @app.post("/uploadfile/")
# async def upload_file(file: UploadFile = File(...)):
#     contents = await file.read()

#     file_copy = NamedTemporaryFile(delete=False)
#     try:
#         file_copy.write(contents);  # copy the received file data into a new temp file. 
#         file_copy.seek(0)  # move to the beginning of the file
#         print(file_copy.read(10))
        
#         # Here, upload the file to your S3 service

#     finally:
#         file_copy.close()  # Remember to close any file instances before removing the temp file
#         os.unlink(file_copy.name)  # unlink (remove) the file
    
#     # print(contents)  # Handle file contents as desired
#     return {"filename": file.filename}
    