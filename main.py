from fastapi import FastAPI,UploadFile,File
import validifi
import duckdb
 
app=FastAPI()

@app.post('/validate_Configuration_1')
async def Validate_Configuration_1(file : UploadFile=File(...)):
    b_file= await file.read()
    
    validation=validifi.c1.verify(file.filename,b_file).func()
    db=duckdb.connect('logs.db')
    cursor=db.cursor()
    if type(validation) == bytes:
        cursor.execute("insert into logs values(?,?)",(file.filename,1))
        cursor.commit()
        cursor.close()
        return {'status':1,'file':validation}
    cursor.execute("insert into logs values(?,?)",(file.filename,validation))
    db.commit()
    db.close()
    return {'status':0,'error':validation}
@app.post('/validate_Configuration_2')
async def Validate_Configuration_2(file : UploadFile=File(...)):
    b_file= await file.read()
    
    validation=validifi.c2.verify(file.filename,b_file).func()
    db=duckdb.connect('logs.db')
    cursor=db.cursor()
    if type(validation) == bytes:
        cursor.execute("insert into logs values(?,?)",(file.filename,1))
        cursor.commit()
        cursor.close()
        return {'status':1,'file':validation}
    cursor.execute("insert into logs values(?,?)",(file.filename,validation))
    db.commit()
    db.close()
    return {'status':0,'error':validation}
@app.post('/get_Configuration')
async def Get_Configuration(file : UploadFile=File(...)):
    pass
@app.post('/get_Logs')
async def Get_Logs():
    db = duckdb.connect('logs.db')
    cursor = db.cursor()
    File = validifi.get_logs.get_logs().func(cursor)
    return {'file':File}

   

    
