import yaml


#READING CONFIG  FILE
with open('config.yml','rb') as file:
    config=yaml.safe_load(file.read())

Config=config['config']
Database=Config['database']

unique_columns=Config['unique_column']
date_format=Config['date_format']
date_time_column=Config['date_time_column']
mandatory_columns=Config['mandatory_columns']
mandatory_column_dtypes=Config['mandatory_column_dtypes']
file_size_max_mb=Config['file_size_max_mb']
column_lenght=Config['column_lenght']
xml_muti_valued_columns=Config['xml_muti_valued_columns']
xml_tables=Config['xml_tables']
db_url=Database['url']
