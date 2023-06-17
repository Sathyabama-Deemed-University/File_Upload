from fastapi import FastAPI,UploadFile,File
import validifi
import duckdb
 
app=FastAPI()

def update(file_name,validation): #UPDATES THE LOGS TO THE DATABASE
    db=duckdb.connect('logs.db')
    cursor=db.cursor()
    ret_val={'status':1,'file':validation}
    if type(validation)==bytes:
        cursor.execute("insert into logs values(?,?)",(file_name,1))
    else :
        cursor.execute("insert into logs values(?,?)",(file_name,validation))
        ret_val={'status':0,'error':validation}
    cursor.commit()
    cursor.close()
    return ret_val
 
@app.post('/validate_Configuration_1')
async def validate_configuration_1(file : UploadFile=File(...)):
    b_file= await file.read()
    validation=validifi.configuration1.verify(file.filename,b_file).func()
    return update(file.filename,validation)
 
@app.post('/validate_Configuration_2')
async def validate_configuration_2(file : UploadFile=File(...)):
    b_file= await file.read()
    
    validation=validifi.configuration2.verify(file.filename,b_file).func()
    return update(file.filename,validation)
 

@app.get('/get_Logs')
async def get_logs():
    db = duckdb.connect('logs.db')
    cursor = db.cursor()
    files = validifi.get_logs.get_logs().func(cursor)
    return {'file':files}

   

    
