import os, datetime, time

folder_path = "C:\\Users\\PRATHAPC\\Documents\\unstructured"

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

        print(f"File Path: {file_path}")
        print(f"File Name: {file_name}")
        print(f"File Size: {file_size} bytes")
        print(f"Creation Time: {c_ti}")
        print(f"Modification Time: {m_ti}")
        print("\n")