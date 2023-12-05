import os, datetime, time
import psycopg2, json

folder_path = "C:\\Users\\PRATHAPC\\Documents\\Sound recordings\\unstructured"


# Database connection parameters
db_params = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Pathu@12',
    'host': 'localhost',
    'port': '5432'
}

#Create a database connection
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# List all files in the folder
file_list = os.listdir(folder_path)
#print(file_list)

# Loop through the files and print their metadata
for file_name in file_list:
    # path of the files
    file_path = os.path.join(folder_path, file_name)
    if os.path.isfile(file_path):
        file_size = os.path.getsize(file_path)  # Get file size in bytes
        file_creation_time = os.path.getctime(file_path)  # Get creation time
        file_modification_time = os.path.getmtime(file_path)  # Get modification time

        # Converting the time in seconds to a timestamp
        c_ti = time.ctime(file_creation_time)
        m_ti = time.ctime(file_modification_time)
        

        file_type= file_name.split(".")[1]
        
        print(f"File type:{file_type}")
        print(f"File Path: {file_path}")
        print(f"File Name: {file_name}")
        print(f"File Size: {file_size} bytes")
        print(f"Creation Time: {c_ti}")
        print(f"Modification Time: {m_ti}")
        print("\n")
        
        metadata_list = {
                "stored_file_name" : file_name,
                "file_size" : file_size,
                "file_type" : file_type,
                "file_path" : file_path,
                "inserted_date" : c_ti    
        }
        # print(metadata_list)
        # exit()
        
        cursor.execute(f"select count(*) from meta_data where stored_file_name = '{file_name}' and file_path = '{file_path}'")
        row_count = cursor.fetchone()[0]
        #print(row_count)
        
        if row_count > 0 :
            update_query = f"""
            UPDATE meta_data set stored_file_name = '{file_name}', file_size= '{file_size}', file_path = '{file_path}' WHERE file_path='{file_path}'
        """
            cursor.execute(update_query)  
            conn.commit()  
            # print(update_query)
            # exit()
        else:
            metadata_json = json.dumps(metadata_list)
            cursor.execute(f"INSERT INTO meta_data ( stored_file_name, file_size, file_path,stored_file_information, file_type) VALUES ('{file_name}', '{file_size}', '{file_path}','{metadata_json}','{file_type}')")
            conn.commit()


            