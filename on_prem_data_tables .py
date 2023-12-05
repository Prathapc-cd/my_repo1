import os, json
import csv
import psycopg2


# Database connection parameters
db_params = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Pathu@12',
    'host': 'localhost',
    'port': '5432'
}

# Create a database connection
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Specify the base directory to read from
base_directory = 'C:\\Users\\PRATHAPC\\Documents\\projects'  # Update this to your desired directory path

try:
    for schema_name in os.listdir(base_directory):
        schema = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'
        cursor.execute(schema)

        table_dir = os.path.join(base_directory, schema_name)
       #print(table_dir)

        for table_name in os.listdir(table_dir):
            try:
         
                #print(table_name)  
                csv_file_path = os.path.join(table_dir, table_name)
                with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
                    csv_reader = csv.reader(csv_file)
                    headers = next(csv_reader)  # Read the first row
                   # print(headers)
                    table_name= table_name.split(".")[0]
                
                    # Create the table if it doesn't exist, with columns from the CSV header
                    table_create = f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" ({", ".join([f"{header} varchar" for header in headers])});'
                    cursor.execute(table_create)

                    # Now, insert data from CSV into the table
                    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
                        csv_reader = csv.reader(csv_file)
                        next(csv_reader)  # Skip the header row
                        for row in csv_reader:
                            # Generate placeholders for the data values
                            value_placeholders = ', '.join(['%s'] * len(row))
                            insert_query = f'INSERT INTO "{schema_name}"."{table_name}" VALUES ({value_placeholders});'
                            cursor.execute(insert_query, row)

                conn.commit()
            except Exception as e:
                print(f"Error processing table {table_name} in schema {schema_name}: {str(e)}")
                conn.rollback()  # Roll back the transaction on error

except Exception as e:
    print(f"Error processing schema: {str(e)}")

# finally:
#     cursor.close()
#     conn.close()

####### fetch table from schema 
print("processing metadata")
SchemaName= "test4onprem"

get_table_source = f"""SELECT table_name FROM information_schema.tables WHERE table_schema='{SchemaName}' """ # Need to define Schema name
connectionSource =  conn.cursor()
connectionSource.execute(get_table_source)
tables = connectionSource.fetchall()
#extra_details = {}

for table in tables:
    table_name = table[0]
    extra_detail_list = [table_name]
    column_query = f"""SELECT * FROM information_schema.columns WHERE table_name='{table_name}'"""
    connectionSource.execute(column_query)
    rows = connectionSource.fetchall()
    column_names = [desc[0] for desc in connectionSource.description]
    data = dict()
    temp = []
    for row in rows:
        #print(row)
        extra_details = dict()
        for column_name, value in zip(column_names, row):
            extra_details[column_name] = value
        temp.append(extra_details) 
    data['data'] = temp        
    extra_detail =  json.dumps(data, indent=1)
    #print(extra_detail)   
            #Get total number of rows
    connectionSource.execute(f"SELECT COUNT(*) FROM {SchemaName+'.'+table_name}")
    row_count =  connectionSource.fetchone()[0]
   # print(row_count)


    #Size of the table 
    connectionSource.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{SchemaName+'.'+table_name}'))")
    table_size = connectionSource.fetchone()[0]
    #print(table_size)

    #Number of Column of the table 
    connectionSource.execute(f"SELECT count(*) FROM information_schema.columns WHERE table_name='{SchemaName+'.'+table_name}'")
    field_count = connectionSource.fetchone()[0]
    #print(field_count)
    
    #DESTINATION TBALE QUERY
    connectionDestination =  conn.cursor()
    project_id = 1 #GETE THE PROJET ID FROM CONFIGURATION TABLE
    connectionDestination.execute(f"select count(*) from schemadest.destination_table WHERE project_id='{project_id}' AND table_name='{table_name}'")
    table_count = connectionDestination.fetchone()[0]
    #print(table_count)
    #exit()

    if table_count > 0:
        update_query = f"""
            UPDATE schemadest.destination_table set extra_info = '{extra_detail}' WHERE project_id='{project_id}' AND table_name='{table_name}'
        """
        #print(update_query)
        connectionDestination.execute(update_query)  
        conn.commit()    
    else:
        insert_query = f"""
            INSERT INTO schemadest.destination_table (project_id,  table_name, table_size, total_record ,table_column, extra_info)
            VALUES ('{project_id}', '{table_name}', '{table_size}', '{row_count}', '{field_count}','{extra_detail}')
        """
        #print(insert_query)
        connectionDestination.execute(insert_query)
        conn.commit()
    print(f"""Meta Data Of Table, {table_name}  has been Inserted / Updated""")