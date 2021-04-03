# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time BIGINT,
    user_id INT,
    level VARCHAR(20),
    song_id VARCHAR(18),
    artist_id VARCHAR(18),
    session_id INT,
    location VARCHAR(100),
    user_agent VARCHAR(250)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users (
    user_id int PRIMARY KEY,
    first_name VARCHAR(20),
    last_name VARCHAR(20),
    gender VARCHAR(1),
    level VARCHAR(20)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs (
    song_id VARCHAR(18) PRIMARY KEY,
    artist_id VARCHAR(18),
    title VARCHAR(100),
    year INT,
    duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists (
    artist_id VARCHAR(18) PRIMARY KEY,
    name VARCHAR(150),
    location VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    start_time BIGINT primary key,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO fact_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE
SET first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    gender = EXCLUDED.gender,
    level = EXCLUDED.level
""")

song_table_insert = """
INSERT INTO dim_songs (song_id, artist_id, title, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING
"""

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE
SET name = EXCLUDED.name, 
    location = EXCLUDED.location,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT sp.song_id,
       sp.artist_id
FROM fact_songplays sp
JOIN dim_songs s ON sp.song_id = s.song_id
JOIN dim_artists a ON sp.artist_id = a.artist_id
WHERE sp.song_id = %s
  AND a.name = %s
  AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]