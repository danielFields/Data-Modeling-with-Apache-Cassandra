from Query1 import *
from Query2 import *
from Query3 import *
import pandas as pd
import cassandra
from cassandra.cluster import Cluster

# Create and set Cassandra Environment
cluster = Cluster(['127.0.0.1'])

# Connect to cluster and get session to manipulate
session = cluster.connect()

#Create KEYSPACE
session.execute("CREATE KEYSPACE IF NOT EXISTS cass1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

# Set KEYSPACE to the keyspace specified above
session.set_keyspace("cass1")


#Create a list of CREATE TABLE statments from each Query to iterate over
create_tables_from_data = [create_table1, create_table2,create_table3]

#Create a list of specific print out explaining queries for each Query to iterate over
print_outs = [table1_message,table2_message, table3_message]

#Create a list of DROP TABLE statments from each Query to iterate over
drop_tables = [drop_table1, drop_table2, drop_table3]


#Create a list of functions that laods data into the tables for each query.
# Each function is the essentially the same except for which columns from the dataframe needed for each table for each query. 
populate_tables = [table1_populate,table2_populate,table3_populate] 

# Call script to process data file.
exec(open("Proc_Data.py").read())

all_events = pd.read_csv("./Proccessed Data.csv")


print("\nCreating Database")
for i in range(len(create_tables_from_data)):
    
    # Try to create table and print out message
    try:
        session.execute(create_tables_from_data[i])
        print(print_outs[i])
        
    #If create fails, retry to create table.
    except Exception as e:
        print(e)
        print("Dropping Old Table...")
        session.execute(drop_tables[i])        
        print("Rebuilding...")
        session.execute(create_tables_from_data[i])
        print(print_outs[i])
        
    print("Populating Table...")
    
    #Then populate each table using the table specific function to popoulate and the session variable.
    populate_tables[i](session, all_events)
    
    print("\n")
    

    
#Remove intermediate csv file no longer needed.
os.remove("./Proccessed Data.csv")

print("\nAnswering Project Questions")
#Answer Question 1
print("1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4")
result1 = query_table1(338,4)
for row in session.execute(result1):
    print(row)


#Answer Question 2
print("\n2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182")
result2 = query_table2(10,182)
for row in session.execute(result2):
    print(row)


#Answer Question 3
print("\n3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'")
result3 = query_table3('All Hands Against His Own')
for row in session.execute(result3):
    print(row)

print("\n")    
print("Shutting Down Cluster")
session.shutdown()
cluster.shutdown()
    
    
    
