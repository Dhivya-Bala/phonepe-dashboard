import pymysql

# Function Definition
def creation_of_table(cursor, table_name, table_create_declaration):
    create_table_query = f"""create table {table_name}({table_create_declaration});"""
    print("create_table_query = ", create_table_query)
    cursor.execute(create_table_query)
    print("Table Created Successfully")
    connection.commit()

def insertion_into_table(cursor, connection, table_name, table_insert_declaration, values_to_be_inserted):
    print("Before Inserting Row Count (Default) = ", cursor.rowcount)
    insert_table_query = f"""insert into {table_name} {table_insert_declaration};"""
    print("insert_table_query = ",insert_table_query)
    #cursor.execute(insert_table_query, values_to_be_inserted) # To insert one record
    cursor.executemany(insert_table_query, values_to_be_inserted) # To insert Multiple record
    connection.commit()
    print("After Inserting Row Count = ", cursor.rowcount)
    return "Success"

def fetching_from_table(cursor, table_name, column_selector, flag, condition):
    if flag == 'all':
        fetching_from_table_query = f"""select {column_selector} from {table_name};"""
    else:
        fetching_from_table_query = f"""select {column_selector} from {table_name} where {condition};"""
    print("fetching_from_table_query = ", fetching_from_table_query)
    cursor.execute(fetching_from_table_query)
    # fetch_one_result = cursor.fetchone()
    # print("fetch_one_result = ", fetch_one_result)
    fetch_all_result = cursor.fetchall()
    print("fetch_all_result = ", fetch_all_result)
    return len(fetch_all_result)    
try:
    # Connection Parameters
    connection = pymysql.connect(
         host = 'localhost', user = 'root',
         password = '12345', database = 'upsc')
    print("connection = ", connection)
    cursor = connection.cursor()
    print("cursor = ", cursor)

    # Table Names
    upsc_candidates_table = 'upsc_candidates_selected'
    #upsc_candidates_selected_table_create_declaration = "candidate_id int, candidate_name varchar(50), candidate_application_fees int"
    #creation_of_table(cursor, upsc_candidates_table, upsc_candidates_selected_table_create_declaration) # Function Calling
     
    #Values Inserting in Table
    #values_to_be_inserted = [(1, 'X', 45000),(2, 'Y', 45000),(3, 'X', 43589)] # Multiple Records Adding
    #insert into table_name (column1, column2, column3) values (value1, value2, value3)
    #upsc_table_insert_declaration = "(candidate_id, candidate_name, candidate_application_fees) values (%s, %s, %s)"
    #insertion_into_table(cursor, connection, table_name, table_insert_declaration, values_to_be_inserted)
    #response = insertion_into_table(cursor, connection, upsc_candidates_table, upsc_table_insert_declaration, values_to_be_inserted)
    #print("Status of Insertion of the data = ", response)

    #Fetching of results
    column_selector, flag, condition = '*', '', 'candidate_id = 1'
    number_of_records = fetching_from_table(cursor, upsc_candidates_table, column_selector, flag, condition)
    print("Number of Records = ", number_of_records)

except Exception as e:
    print(str(e))
