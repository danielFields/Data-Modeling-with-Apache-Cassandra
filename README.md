# Apache Cassandra ETL Demo

The purpose of this repo is to demonstrate the functionality of an ETL process using pandas dataframes in Python and Apache Cassandra NoSQL Database to store data.
This project creates and executes an ETL pipeline for a mock musicstreaming application. This was made during an assignment from the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

The data is mock data meant to represent the user listening activity on a streaming application. The data is stored in a directory of csv's where each csv has the below fields in each.
- artist 
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

The data is organized into three tables, with each table being focusued on a specific query.
- Table 1 is designed to give the artist, song title and song's length in the music app history and is queried by sessionId and itemInSession.
    - `PRIMARY KEY (session_id, item_in_session,artist_name,song_name)`
- Table 2 is designed to give the name of artist, song (sorted by itemInSession) and user (first and last name) and is queried by userid and sessionid
    - `PRIMARY KEY (user_id, session_id,item_in_session, artist_name,song_name,user_name)`
- Table 3 is designed to give the name (first and last) of every user in the music app history and is queried by song title.
    - `PRIMARY KEY (song_name, user_name)`


In this repo there are several files. 
 *  `Proc_Data.py` reads in all csv's of event data and creates one large file that can be read into the database. 
 * `QueryN.py` _(N = 1,2, or 3)_ creates the `CREATE TABLE` statement needed to create all the fields in Table _N_. In addition it creates the CQL statements to drop Table N. It also contains two functions specific to Table _N_.
    - `tableN_populate(driver, data)` takes as the Cassandra Cluster session driver and the entire event data aggegate csv as an argument and inserts each row into Table N. 
    - `query_tableN` with the primary key being the arguemnt(s) which returns a string that has the formatted CQL query needed to search for data in each table as specified by that table's primary key. 
    
* `Run_ETL.py` calls the data proccessing script and then for each table the script will create the table and insert data into the table.



To test the functionality of this code you can ran `Run_ETL.py` then use the `query_tableN` functions to get the data desired. For demo purposes, like the course this was made as a part of, you can run the `Demo.py` script.
