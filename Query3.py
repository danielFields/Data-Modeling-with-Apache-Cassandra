def getProgress(current, length):
    """This function formats a progress bar string for print out during a for loop execution.
    Currently, this uses 2% increments. Adjusting the inc variable to N will change increments to 1/N."""
    
    inc = 50
    
    n_bars = int(round(current*inc/length,1))
    rem = inc - n_bars
    progress =  '[' + '#'*n_bars + rem * ' ' + ']' + ' {}% Done'.format(round(current*100/length))
    print(progress, end = '\r')
    
create_table3 = """CREATE TABLE WHERE_SONG (
song_name text,
user_name text,
PRIMARY KEY (song_name, user_name)
);"""

table3_message = "SUCCESS: Created table to retrieve every user name (first and last name) in the music app history who listened to the queried song."

drop_table3 = "DROP TABLE IF EXISTS WHERE_SONG;"

def query_table3(song):
    """
    This function returns the SQL neccessary to get all users who listened to the song name passed as an argument to this function.
    """
    return "select user_name from WHERE_SONG where song_name = '{}';".format(song)


def table3_populate(driver, data):
    """
    This iterates through specified columns of the total dataset and inserts rows into Table 1.
    """
    table_df = data[['song','userName']]
    
    insert =  "INSERT INTO CASS1.WHERE_SONG "
    insert += "(song_name ,user_name)"
    insert +=  "VALUES ( %s,  %s)" 

    n_row = 0
    print("Inserting {} Rows to Table 3.".format(table_df.shape[0]))
    
    for index,row in table_df.iterrows():
        values = tuple(row.values.tolist())

        driver.execute(insert,values)
        n_row += 1
        
        #Print out Progress
        getProgress(n_row,table_df.shape[0])
    
    print("\r")
    print(n_row, " Rows Added to Table 3: WHERE_SONG")