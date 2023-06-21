from fastapi import FastAPI,UploadFile,File,Depends
import validifi
import database.db as db
import database.table as table
 
app=FastAPI()

table.base.metadata.create_all(bind=db.engine)
 
def update(file_name,validation,db):#UPDATES THE LOGS TO THE DATABASE
    if type(validation) == bytes:
        file_log = {
            'file':validation,
            'file_name':file_name,
            
        }
        file_log = table.valid_files(**file_log)
        db.add(file_log)
        db.commit()
        return {'status':'file uploaded successfully'}
    
    else:
        return {'error':validation}
 
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
 


   

    
