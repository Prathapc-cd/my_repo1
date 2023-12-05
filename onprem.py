import os
from datetime import datetime
from pathlib import Path

base_directory = 'C:\\Users\\PRATHAPC\\Documents\\unstructured'

table_dir = os.path.join(base_directory)

#for table_name in os.listdir(table_dir):

def GetFileMetaData(table_name):
        
        FileStatitics = os.stat(table_name)
        FileSize = FileStatitics.st_size
        LastModi = datetime.fromtimestamp(FileStatitics.st_mtime)
    #path = Path(FileStatitics.__file__)

        print(f'file size {FileSize} bytes')
        print(f'last modi{LastModi} ')
    #print(path)

# GetFileMetaData('C:\\Users\\PRATHAPC\\Documents\\unstructured\\first.pdf')
# GetFileMetaData('C:\\Users\\PRATHAPC\\Documents\\unstructured\\first.csv') 
# GetFileMetaData('C:\\Users\\PRATHAPC\\Documents\\unstructured\\Funfriday.pptx') 
  
GetFileMetaData('C:\\Users\\PRATHAPC\\Documents\\unstructured\\first.pdf')