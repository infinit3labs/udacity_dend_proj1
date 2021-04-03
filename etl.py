import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
# To deal with error: "can't adapt type 'numpy.int64'" when trying to insert list values with numpy.int64 data type
# See this post: https://stackoverflow.com/questions/50626058/psycopg2-cant-adapt-type-numpy-int64
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # Clean any blank cells to NaN to insert into database as NULL
    df = df.replace(r'^\s*$', np.nan, regex=True)
    
    # Clean any invalid year values and convert to -1 (limit to exclude 3-digit years)
    df.loc[df.year < 1000, 'year'] = -1

    # insert song record
    song_data = df[['song_id', 'artist_id', 'title', 'year', 'duration']].loc[0].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].loc[0].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[(df.page == 'NextSong')]
    
    # Add new columns to existing dataframe with time components
    df['dt'] = pd.to_datetime(df.ts, unit='ms')
    df['hour'] = df['dt'].dt.hour
    df['day'] = df['dt'].dt.day
    df['week'] = df['dt'].dt.isocalendar().week
    df['month'] = df['dt'].dt.month
    df['year'] = df['dt'].dt.year
    df['weekday'] = df['dt'].dt.weekday

    # Create new dataframe with only required time columns
    t = df[['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']]
    
    # insert time data records
    time_data = t.values.tolist()
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        ) 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
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