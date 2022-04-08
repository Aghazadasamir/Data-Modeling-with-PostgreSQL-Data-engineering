import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """ 
    Opens song file,
    
    insert song records,
    
    insert artist records
    
    """
    
    df = pd.read_json(filepath, lines=True)

    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    opens log file,
    
    filters by NextSong action,
    
    converts timestamp column to datetime,
    
    insert time data records,
    
    """
    
  
    df = pd.read_json(filepath, lines=True)

    df = df.loc[df['page'] == 'NextSong']

    t = pd.to_datetime(df['ts'], unit='ms')
    
    time_data = [df.ts.values, t.dt.hour.values, t.dt.day.values, t.dt.weekofyear.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values]

    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df =  pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

  
    """
    Load user table,
    
    insert user records,
    
    insert songplay records,
    
    gets songid and artistid from song and artist tables
    
    """

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

  
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

       
        songplay_data = [row.ts, row.userId,
                         row.level, songid, artistid, row.sessionId,
                         row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    gets all files matching extension from directory,
    
    gets total number of files found,
    
    iterate over files and process
    
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