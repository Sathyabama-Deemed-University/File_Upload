import yaml

with open('config.yml','rb') as file:
    config=yaml.safe_load(file.read())
Config=config['config']
Errors=Config['Error']
column_length_e=Errors['column_lenght']
mandatory_col_e=Errors['mandatory_column']
date_format_e=Errors['date_format']
file_type_e=Errors['file_type']
column_type_e=Errors['column_type']
file_size_e=Errors['file_size']
unique_col_e=Errors['unique_col']
corrupted_file_e=Errors['corrupted file']
xml_multivalued_column_e=Errors['xml multivalued column']
xml_mutiple_tables_e=Errors['xml mutiple tables']
empty_e=Errors['empty']
