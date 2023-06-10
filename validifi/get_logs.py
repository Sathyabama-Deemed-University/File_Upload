import pandas as pd
class get_logs:
    # no variables are bound to get_logs
    def __init__(self):
        pass
       
    def convert_logs(self,data):
        data = list(zip(*[list(i) for i in data]))
        logs = pd.DataFrame({
        'Filename':data[0],
        'status':data[1]
        }).to_csv(index=False)
        
        return logs.encode()
    def func(self,cursor):
        log_data = cursor.execute('select * from logs')
        log_data = log_data.fetchall()
        files = self.convert_logs(log_data)
        return files
