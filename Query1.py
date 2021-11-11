def getProgress(current, length):
    """This function formats a progress bar string for print out during a for loop execution.
    Currently, this uses 2% increments. Adjusting the inc variable to N will change increments to 1/N."""
    
    inc = 50
    
    n_bars = int(round(current*inc/length,1))
    rem = inc - n_bars
    progress =  '[' + '#'*n_bars + rem * ' ' + ']' + ' {}% Done'.format(round(current*100/length))
    print(progress, end = '\r')


create_table1 = """CREATE TABLE WHERE_SESSIONID_ITEM (
  session_id int,
  item_in_session int,
  artist_name text,
  song_name text,
  song_length float,
  PRIMARY KEY (session_id, item_in_session,artist_name,song_name)
);"""

table1_message = "SUCCESS: Created table to retrieve the artist, song title and song's length in the music app history queried by sessionId and itemInSession."

drop_table1 = "DROP TABLE IF EXISTS WHERE_SESSIONID_ITEM;"
    
def query_table1(session_id, item_in_session):
    """
    This function returns the SQL neccessary to get the artists, songs, and lengths of the songs
    with the specified session id passed as an argumemt and specified item in session passed as an argumemt.
    """
    query = """select artist_name, song_name, song_length 
            from WHERE_SESSIONID_ITEM 
            where session_id = {} and item_in_session = {};""".format(session_id,item_in_session)
    
    return query


def table1_populate(driver, data):
    """
    This iterates through specified columns of the total dataset and inserts rows into Table 1.
    """
    table_df = data[['sessionId','itemInSession','artist','song','length']]
    
    insert =  "INSERT INTO CASS1.WHERE_SESSIONID_ITEM "
    insert += "(session_id,item_in_session,artist_name,song_name,song_length) "
    insert +=  "VALUES ( %s,  %s,  %s,  %s,  %s)" 

    n_row = 0
    print("Inserting {} Rows to Table 1.".format(table_df.shape[0]))
    
    for index,row in table_df.iterrows():
        values = tuple(row.values.tolist())

        driver.execute(insert,values)
        n_row += 1
        
        #Print out Progress
        getProgress(n_row,table_df.shape[0])
    print("\r")   
    print(n_row, " Rows Added to Table 1: WHERE_SESSIONID_ITEM")

