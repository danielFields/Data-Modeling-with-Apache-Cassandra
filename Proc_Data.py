import pandas as pd
import os
import glob
import csv

def getProgress(current, length):
    """This function formats a progress bar string for print out during a for loop execution.
    Currently, this uses 2% increments. Adjusting the inc variable to N will change increments to 1/N."""
    
    inc = 50
    
    n_bars = int(round(current*inc/length,1))
    rem = inc - n_bars
    progress =  '[' + '#'*n_bars + rem * ' ' + ']' + ' {}% Done'.format(round(current*100/length))
    print(progress, end = '\r')
    
    
# Get your current folder and subfolder event data
files_location = './event_data/'

#Get all files and add full path to files
file_path_list = [files_location + i for i in os.listdir(files_location)] 


#Get total number of files for progress print out
n_files = len(file_path_list)

# Process Data
print("Proccessing {} Files".format(n_files))

all_events = pd.read_csv(file_path_list[0],quoting=csv.QUOTE_ALL,engine = 'python') #Seed df to append to
    
getProgress(1,n_files)

for current_event in file_path_list[1::]:
    
    #Read current csv files
    current_event_df = pd.read_csv(current_event,quoting=csv.QUOTE_ALL,engine = 'python')
    
    #Append to all events dataframe 
    all_events = all_events.append(current_event_df)
    
    #Print progress
    getProgress(file_path_list.index(current_event) + 1,n_files)
    
    
#Drop any rows with missing values 
# Missing values in the data occur when user activity is on the pages of the site where the user is not listening to music.
all_events.dropna(inplace=True) 

#Create 1 username field instead of first name and last name separately
all_events['userName'] = all_events[['firstName','lastName']].apply(lambda x: ' '.join(x),axis = 1) 

#Drop no longer needed columns
all_events.drop(['firstName','lastName'],axis = 1,inplace=True)

#Convert tye to match the type expected by the database
all_events['userId'] = all_events['userId'].astype(int)

all_events.to_csv("./event_data_new.csv",index = False)

#Remove last progress print out
print("\r")
