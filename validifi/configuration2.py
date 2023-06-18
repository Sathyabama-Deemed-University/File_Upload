import pandas as pd
import polars as pl
import io
from . import config
from . import errors
import base64
import re

class verify:
    def __init__(self,filename, bdata,unique_columns=config.unique_columns, date_format = config.date_format, date_time_column =config.date_time_column,
                 mandatory_column_dtypes=config.mandatory_column_dtypes ,mandatory_columns=config.mandatory_columns, file_size_limit=config.file_size_max_mb,
                 column_length=config.column_lenght,xml_multivalued_columns=config.xml_muti_valued_columns,xml_tables=config.xml_tables ):
        self.bdata = bdata
        self.filename = filename
        self.file_size_limit = file_size_limit * 1024*1024
        self.column_length = column_length
        self.unique_columns = unique_columns
        self.dict_dtypes={'Int8': pl.Int8,
             'Int16': pl.Int16,
             'Int32':pl.Int32,
             'Int64': pl.Int64,
             'UInt8': pl.UInt8,
             'Utf8':pl.Utf8,
            
             'UInt16':  pl.UInt16,
             'UInt32':  pl.UInt32,
             'UInt64':  pl.UInt64,
             'Float32': pl.Float32,
             'Float64': pl.Float64,
             'Time': pl.Time,
             'Date': pl.Date,
             'Datetime': pl.Datetime}
       
        self.mandatory_column_dtypes = mandatory_column_dtypes
        self.mandatory_columns = mandatory_columns
        self.date_time_column = date_time_column
        self.error = None
        self.date_format = date_format
        self.xml_multivalued_columns = xml_multivalued_columns
        self.xml_tables = xml_tables 
        self.func_call = {'XLSX': self.xlsx_xlsm_check, 'XLSM': self.xlsx_xlsm_check, 'CSV': self.csv_check, 'XML': self.xml_check}
       
       
    def check_file_type(self):   #VALIDATING FILE  TYPE
        
        try:
            self.file_type = self.filename.split('.')[-1].upper()
            self.func_call[self.file_type]
            return self.file_type
        except Exception:
            self.error = errors.file_type_e.format(self.file_type)
            return 0
        
    def check_column_type(self,list_index=0):   #VALIDATING COLUMN-DATA-TYPE
        self.err_cols=[]
        column_dtypes=[self.dict_dtypes[i] for i in self.mandatory_column_dtypes[list_index]]
        for i,j in zip(column_dtypes,self.mandatory_columns[list_index]):
            if self.df[j].dtype != i:
                self.err_cols.append([i,j])
           
                
        if not self.err_cols:
            return 1
        for i in self.err_cols:
            try:
                self.df = self.df.with_columns(pl.col(i[-1]).cast(i[0]))
            
            except Exception:
                self.error = errors.column_type_e
                return 0
        return 1
        
    def check_mandatory_columns(self,list_index=0):    # CHECKS FOR MANDATORY COLUMN
        for i in self.mandatory_columns[list_index]:
            if i not in self.df.columns:
                self.error = errors.mandatory_col_e
                return 0
            
               
        return 1
    
    def date_formate(self,cols):        #EXTRACTS DATE FROM THE FILE
        d_f=list(zip(*[list(map(int,re.findall(r'[0-9]{1,4}',i)))for i in cols]))
        print(d_f)
        formate=[]
        sep='-'
        if '/' in cols[0]:sep='/'
        elif '.' in cols[0]:sep='.'

        for i in d_f:
            if max(i)<=12:formate.append("%m")
            elif max(i)>31:formate.append("%Y")
            elif max(i)<=31 and max(d_f[2])>12:formate.append('%d')
        if formate.count('%m')>1:formate[formate.index('%m')]='%d'
        return sep.join(formate),sep,d_f

    def find_sep(date:list):
        if '/' in date[0]:return '/'
        elif '.' in date[0]:return '.'
        return '-'

    def map_for2(self,date:list,to): #CHANGE THE DATE FORMAT ACCORDING TO CONFIGFILE
        from_,sep,da=self.date_formate(date)
        d_f=[0]*3
        sp=lambda x:sep.join(map(str,x))
        f_l,t_l=from_.split(sep),to.split(sep)
        if len(f_l)!=3 or len(t_l)!=3:
            raise ValueError(f"WARNING:PROVIDED DATE FORMATE '{to}' IS INVALID! TRY USING SEPRATOR WHICH MATCHS '{from_}'")
        for i,j in enumerate(f_l):
            d_f[t_l.index(j)]=da[i]
        date_real=list(zip(*d_f))
        return from_,list(map(sp,date_real))
    def check_date_format(self,list_index=0):     # VALIDATING DATE-FORMAT W.R.T CONFIG FILE
        if len(self.date_time_column[list_index]) == 0:
            return 1
        
        for i in self.date_time_column[list_index]:
            try:
                print(1)
                self.df = self.df.with_columns(pl.col(i).str.strptime(pl.Date, self.date_format))
                
            except Exception:
                try:   #CHANGES COLUMN-DATE-FORMAT W.R.T  CONFIG FILE
                    from_,date_values = self.map_for2(self.df[i],self.date_format)
                    self.df = self.df.with_columns(pl.col(i).str.strptime(pl.Date, from_))
                    self.temp_df = self.temp_df.with_columns(pl.Series(name=i, values=date_values))
                               
                except Exception:   # ELSE: RETURN ERROR
                    
                    self.error = errors.date_format_e.format(i,self.date_format)
                    return 0
        return 1
    
    
    def _column_length(self,listindex=0):    #VALIDATING COLUMN-LENGTH W.R.T CONFIG FILE
        if len(self.df.columns) <= self.column_length[listindex]:
            return 1
        self.error= errors.column_length_e
        return 0
    
    def unique_col(self,list_index=0):   #CHECKS FOR UNIQUE COLUMNS
        if len(self.unique_columns[list_index]) == 0:
            return 1
        for i in self.unique_columns[list_index]:
            if len(self.df[i]) == len(self.df[i].unique()):
                return 1
            else:
                self.error = errors.unique_col_e.format(i)
                return 0
    def check_conditions(self,list_index=0):
        if not self._column_length(list_index): return 0
        if not self.check_mandatory_columns(list_index): return 0
        if not self.check_date_format(list_index): return 0
        if not self.unique_col(list_index): return 0
        if not self.check_column_type(list_index): return 0
        return 1
    def csv_check(self):  #VALIDATES CSV FILE
        try:
            self.df = pl.read_csv(io.BytesIO(self.bdata))  #READS CSV FILE
            if len(self.df) != len(self.df.unique()):
                self.is_unique=False
            self.df = self.df.unique()
            self.temp_df=self.df
        except pl.exceptions.NoDataError :
                self.error = errors.empty_e
                return 0
        except Exception:   
            
            self.error = errors.corrupted_file_e
            return 0
        if self.df.shape[0] == 0:
            self.error = errors.empty_e
            return 0
       
        if self.check_conditions():                  
            self.bdata = self.temp_df.to_pandas().to_csv(index=False).encode()
            return 1
        return 0
    
    def xlsx_xlsm_check(self):   #VALIDATES EXCEL FILE
        try:
            self.df = pl.read_excel(io.BytesIO(self.bdata))   # READS EXCEL FILE
            self.df = self.df.unique()
            self.temp_df=self.df
        except pl.exceptions.NoDataError :
                self.error = errors.empty_e
                return 0
        except Exception:
           
            self.error = errors.corrupted_file_e
            return 0
        if self.df.shape[0] == 0:
            self.error = errors.empty_e
        
        if self.check_conditions(): 
            temp_pointer=io.BytesIO()
            self.bdata = self.df.to_pandas().to_excel(temp_pointer,index=False)
            temp_pointer.seek(0)
            self.bdata=base64.encodebytes(temp_pointer.read())
            return 1
        return 0
        
    def remove_multivalued_col_tags(self,bdata,col_name): #SPLITS MULTIVALUED COLUMNS INTO INDIVIDUAL COLUMNS
        
        
        string_data = bdata.replace(f'<{col_name}>'.encode(),b"")
        string_data = string_data.replace(f'</{col_name}>'.encode(),b"")
       
        return string_data
    def get_multivalued_cols(self,list_index): #EXTRACTS EACH MULTIVALUED COLUMN NAME
        self.multivalued_cols = []
        for i in self.df.columns:
            if self.df[i].unique()[0] == None and len(self.df[i].unique()) <= 1:
                self.multivalued_cols.append(i)
        self.multivalued_cols = list(set(self.multivalued_cols)-set(self.xml_multivalued_columns[list_index]))
        for i in self.multivalued_cols:
            self.bdata = self.remove_multivalued_col_tags(self.bdata,i)
        return 1
    
    def check_multivalued_cols(self,bdata,list_index=0):  # IF MULTIVALUED COLUMN -->PINGS 'remove_multivalued_col_tags'
        self.bdata=bdata
        for i in self.xml_multivalued_columns[list_index]:
            self.bdata = self.remove_multivalued_col_tags(self.bdata,i)
        return 1
    
    def extract_tables_xml(self,table_name):  #EXTRACTS AND RETURNS TABLE CONTENT
        s_inde=self.temp_bdata.index(b'<'+table_name.encode()+b'>')
        en_inde=self.temp_bdata.index(b'</'+table_name.encode()+b'>')+len(table_name)+3
        return self.temp_bdata[s_inde:en_inde]

    def xml_check_b(self,bdata,list_index=0):     #VALIDATES XML FILE
        self.check_multivalued_cols(bdata,list_index)
       
        try:
            self.df = pd.read_xml(io.BytesIO(self.bdata)) #READS XML FILE
            self.df = pl.from_pandas(self.df)
            self.df = self.df.unique()
            self.temp_df=self.df
            
        except pl.exceptions.NoDataError :
            self.error = errors.empty_e
            return 0
        except Exception:
            self.error = errors.corrupted_file_e
            return 0
        self.get_multivalued_cols(list_index)
        if self.multivalued_cols:
            for i in self.multivalued_cols:
                self.remove_multivalued_col_tags(self.bdata,i)
            self.df = pd.read_xml(io.BytesIO(self.bdata))
            self.df = pl.from_pandas(self.df).unique()
            self.temp_df=self.df
        
        if self.check_conditions(list_index):
            self.bdata = self.temp_df.to_pandas().to_xml(index=False).encode()
            return 1
        return 0
        
    def xml_check(self):     #VALIDATES XML FILE
        if self.xml_multivalued_columns and len(self.xml_tables) in [0,1]:
            self.check_multivalued_cols(self.bdata)
            return self.xml_check_b(self.bdata)
        else:
            self.temp_bdata=self.bdata
            temp_bdata=bytes()
            for index,value in enumerate(self.xml_tables):
                
                table=self.extract_tables_xml(value)
                
                if self.xml_check_b(table,index):
                    
                    self.bdata = self.bdata.replace(b"<?xml version='1.0' encoding='utf-8'?>",b"")
                    self.bdata = self.bdata.replace(b'<data>',f'<{value}>'.encode())
                    self.bdata = self.bdata.replace(b'</data>',f'</{value}>'.encode())
                    temp_bdata += self.bdata
                else:return 0
            
            self.bdata = b"<?xml version='1.0' encoding='utf-8'?>\n<root>"+temp_bdata+b"\n</root>"
            self.bdata = self.bdata.replace(b"\n<b'",b"\n")
            return 1
                
                
    def check_size(self):  #CHECKS FILE SIZE W.R.T CONFIG FILE
        if len(self.bdata) > self.file_size_limit:
             self.file_data = None
             self.error = errors.file_size_e
             return 0    
        return 1
       
       
    def func(self): # VALIDATES FILE CONFIG AND PINGS APPOPRIATE FUNCTION CALL
        if self.check_size():
            x=self.check_file_type()
            if x and self.func_call[x]():
                return self.bdata
        return self.error
