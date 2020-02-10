import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process song_data files and populate data into songs and artists tables. 
    Parameters:
    cur (cursor): the cursor connected to the target database where the demension and fact tables reside.
    filepth (string) : the folder path of the song_data files. All the files under the folder and subfolder will be processed.
    """
   
    df = pd.read_json(filepath, lines=True)       
    
    artist_data = df.filter(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)
    
    song_data = df.filter(["song_id", "title", "artist_id", "year", "duration"]).values.tolist()[0]
    cur.execute(song_table_insert, song_data)

def process_log_file(cur, filepath):
    """
    Process log_data files and populate data into time, users and songplays tables. 
    Parameters:
    cur (cursor): the cursor connected to the target database where the demension and fact tables reside.
    filepth (string) : the folder path of the log_data files. All the files under the folder and subfolder will be processed.
    """
    df = pd.read_json(filepath, lines=True)    
    df = df.query('page == "NextSong"')
   
    t = pd.to_datetime(df['ts'], unit='ms')     
    time_data = (df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("timestamp", "hour", "day", "week of year", "month", "year", "weekday")
    time_df = pd.DataFrame({column_labels[0]: time_data[0], column_labels[1]: time_data[1], column_labels[2]: time_data[2], column_labels[3]: time_data[3], column_labels[4]: time_data[4], column_labels[5]: time_data[5], column_labels[6]: time_data[6]})
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    user_df = df[['userId', 'firstName', 'lastName','gender','level']]    
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)   
    for index, row in df.iterrows():      
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None       
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)  
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process song_data and log_data files and populate data into time, users and songplays tables. 
    The function will detemine which type of files need to be processed according to the provided function name.
    So be clear which type of files you need to process when you call this function.
    Parameters:
    cur (cursor): the cursor connected to the target database where the demension and fact tables reside.
    conn (connection): the connection to the target database where the demension and fact tables reside
    filepth (string) : the folder path of the files that needs to be processed. All the files under the folder and subfolder will be processed.
    func (function): the function that will be used to process data. There are two functions by now, process_song_file and process_log_file
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))    
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))    
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()