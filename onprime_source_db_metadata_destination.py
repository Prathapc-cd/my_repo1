#Basically this code is to insert metadata of structured data(database) to destination table.
import psycopg2
import json

#Destination
hostname ="localhost"
database = "postgres"
user     = "postgres"
password = "Pathu@12"
port     ="5432"
options  ="-c search_path=dbo,public"

#Source
hostname_source ="localhost"
database_source = "sourcedb"
user_source     = "postgres"
password_source = "Pathu@12"
port_source     ="5432"
options_source  ="-c search_path=dbo,public"

def db_connection_destination(hostname, database,user,password, port, options):
    
    conn = psycopg2.connect(
        host     = hostname,
        port     = port, 
        database = database,
        user     = user,
        password = password,
        options  = options
    )
    return conn

def db_connection_source(hostname_source, database_source,user_source,password_source,port_source,options_source):
    conn_source = psycopg2.connect(
        host     = hostname_source,
        port     = port_source, 
        database = database_source,
        user     = user_source,
        password = password_source,
        options  = options_source
    )
    return conn_source



def fetch_configuration(connection_source):
    cursor = connection_source.cursor()
    # Fetch configuration from the Django configuration table.  Need to change this query once Destination DATABASE Created
    config_table_query = """select * from structure_source_configuration WHERE str_status='active'"""
    cursor.execute(config_table_query)
    table_desc = cursor.fetchall()
    rowcount = cursor.rowcount
    config_dir = {}
    if rowcount == 1:      
        #print(table_desc)
        config_dir = {}
        column_names = [desc[0] for desc in cursor.description]
        for row in table_desc:
            for column_name, value in zip(column_names, row):
                #print(f"{column_name}: {value}")
                config_dir[column_name] = value
        return config_dir
    else:
        return config_dir    


def retrive_table_metadata_insert_to_destinationtable(connection_destination, connection_source, configuration):
    #print(configuration)
    get_table_source = f"""SELECT table_name FROM information_schema.tables WHERE table_schema='test4onprem' """ # Need to define Schema name
    connectionSource =  connection_source.cursor()
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
        connectionSource.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count =  connectionSource.fetchone()[0]


        #Size of the table 
        connectionSource.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{table_name}'))")
        table_size = connectionSource.fetchone()[0]


        #Number of Column of the table 
        connectionSource.execute(f"SELECT count(*) FROM information_schema.columns WHERE table_name='{table_name}'")
        field_count = connectionSource.fetchone()[0]
        #print(field_count)
        #exit()

        #DESTINATION TBALE QUERY
        connectionDestination =  connection_destination.cursor()
        project_id = configuration['project_id'] #GETE THE PROJET ID FROM CONFIGURATION TABLE
        connectionDestination.execute(f"select count(*) from structure_metadata_demo WHERE project_id='{project_id}' AND tablename='{table_name}'")
        table_count = connectionDestination.fetchone()[0]
        #print(table_count)
        #exit()

        if table_count > 0:
            update_query = f"""
                UPDATE structure_metadata_demo set extra_info = '{extra_detail}' WHERE project_id='{project_id}' AND tablename='{table_name}'
            """
            #print(update_query)
            connectionDestination.execute(update_query)      
        else:
            insert_query = f"""
                INSERT INTO structure_metadata_demo (project_id,  tablename, table_size, total_record ,total_column, extra_info)
                VALUES ('{project_id}', '{table_name}', '{table_size}', '{row_count}', '{field_count}','{extra_detail}')
            """
            #print(insert_query)
            connectionDestination.execute(insert_query)
        print(f"""Meta Data Of Table, {table_name}  has been Inserted / Updated""")
    #return print("All Done")

#destination Connection
connection_destination     = db_connection_destination(hostname, database,user,password, port, options)

#SOURCE DATABASE CONNECTION 
connection_source = db_connection_source(hostname_source, database_source,user_source,password_source,port_source,options_source)

configuration     = fetch_configuration(connection_destination)



#print(bool(configuration))
if bool(configuration): 
    retrive_table_metadata_insert_to_destinationtable(connection_destination, connection_source,  configuration)
    connection_destination.commit()
    connection_destination.close()

    connection_source.commit()
    connection_source.close()
else:
    print("Please check your configuration")
