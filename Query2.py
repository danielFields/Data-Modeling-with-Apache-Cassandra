def getProgress(current, length):
    """This function formats a progress bar string for print out during a for loop execution.
    Currently, this uses 2% increments. Adjusting the inc variable to N will change increments to 1/N."""
    
    inc = 50
    
    n_bars = int(round(current*inc/length,1))
    rem = inc - n_bars
    progress =  '[' + '#'*n_bars + rem * ' ' + ']' + ' {}% Done'.format(round(current*100/length))
    print(progress, end = '\r')
    
create_table2 = """CREATE TABLE WHERE_USERID_SESSIONID (
  user_id int,
  session_id int,
  item_in_session int,
  artist_name text,
  song_name text,
  user_name text,
  PRIMARY KEY (user_id, session_id,item_in_session, artist_name,song_name,user_name)
);"""

table2_message = "SUCCESS: Created table from data that returns name of artist, song (sorted by itemInSession) and user (first and last name) queried by userid and sessionid."

drop_table2 = "DROP TABLE IF EXISTS WHERE_USERID_SESSIONID;"

def query_table2(user_id, session_id):
    """
    This function returns the SQL neccessary to get all artists, songs, and the user name of
    the user with user id passed as an agrument and in session with id passed as an argumemt.
    """
    return "select artist_name, song_name, user_name from WHERE_USERID_SESSIONID where user_id = {} and session_id = {}".format(user_id, session_id);

def table2_populate(driver, data):
    """
    This iterates through specified columns of the total dataset and inserts rows into Table 2.
    """
    table_df = data[['userId','sessionId','itemInSession','artist','song','userName']]
    
    insert =  "INSERT INTO CASS1.WHERE_USERID_SESSIONID "
    insert += "(user_id, session_id, item_in_session, artist_name, song_name ,user_name)"
    insert +=  "VALUES ( %s,  %s,  %s,  %s,  %s,  %s)" 

    n_row = 0
    print("Inserting {} Rows to Table 2.".format(table_df.shape[0]))
    
    for index,row in table_df.iterrows():
        values = tuple(row.values.tolist())

        driver.execute(insert,values)
        n_row += 1
        
        #Print out Progress
        getProgress(n_row,table_df.shape[0])
    print("\r")
    print(n_row, " Rows Added to Table 2: WHERE_USERID_SESSIONID")

