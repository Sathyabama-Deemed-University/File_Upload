from fastapi import FastAPI,UploadFile,File
import validifi
import duckdb
 
app=FastAPI()
db_file_name = 'logs.db'
db_query = "insert into logs values(?,?)"
@app.post('/validate_Configuration_1')
async def validate_configuration_1(file : UploadFile=File(...)):
    b_file= await file.read()
    
    validation=validifi.c1.verify(file.filename,b_file).func()
    db=duckdb.connect(db_file_name)
    cursor=db.cursor()
    if type(validation) == bytes:
        cursor.execute(db_query,(file.filename,1))
        cursor.commit()
        cursor.close()
        return {'status':1,'file':validation}
    cursor.execute(db_query,(file.filename,validation))
    db.commit()
    db.close()
    return {'status':0,'error':validation}
@app.post('/validate_Configuration_2')
async def validate_configuration_2(file : UploadFile=File(...)):
    b_file= await file.read()
    
    validation=validifi.c2.verify(file.filename,b_file).func()
    db=duckdb.connect(db_file_name')
    cursor=db.cursor()
    if type(validation) == bytes:
        cursor.execute(db_query,(file.filename,1))
        cursor.commit()
        cursor.close()
        return {'status':1,'file':validation}
    cursor.execute(db_query,(file.filename,validation))
    db.commit()
    db.close()
    return {'status':0,'error':validation}
@app.post('/get_Configuration')
# code yet to update
async def get_configuration(file : UploadFile=File(...)):
    pass
@app.get('/get_Logs')
async def get_logs():
    db = duckdb.connect(db_file_name)
    cursor = db.cursor()
    files = validifi.get_logs.get_logs().func(cursor)
    return {'file':files}

   

    
