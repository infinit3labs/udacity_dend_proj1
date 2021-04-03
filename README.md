## Context
Sparkify is a new startup that provides a music streaming service.
To analyse the use of their service, they capture logs of the application use.
They would like to understand what songs users are listening to.

## Database Schema Design
The database uses a star schema to optimise the read-heavy analytics queries of Sparkify business users.

The single fact table captures the core business process provided by Sparkify - playing songs.

The database contains the following fact table:
### fact_songplay
    * (PK) songplay_id: auto increment
    * start_time: this is the timestamp of when the log entry was created
    * user_id
    * level: this is the type of account the user has at the time of the song play
    * song_id
    * artist_id
    * session_id
    * location: this is the user's location at the time of the log entry
    * useragent

The dimensions are defined to capture the key business entities involved with Sparkify's service: users, songs, and artists.  There is also a time dimension to analyse usage over time.

The database contains the following dimension tables:
### dim_users
    * (PK) user_id
    * first_name
    * last_name
    * gender
    * level: this is the type of account the user has at the last time the user played a song
### dim_songs
    * (PK) song_id
    * title
    * artist_id
    * year
    * duration
### dim_artists
    * (PK) artist_id
    * name
    * location: this is the location recorded for the artist as at the last time a user played a song by the artist
    * latitude
    * longitude
### dim_time
    * (PK) start_time: this is the timestamp of when the log entry was created
    * hour
    * day
    * week
    * month
    * year
    * weekday

## ETL Design
There are two source files provided to populate the database: log files of application activity stored in a JSON format saved in folders by year and month; details of each song available on the platform stored in JSON format and save in folders by the first 3 characters of the Song ID.

The ETL contains two main processes: collect and process the song data, collect and process the log data.

In processing the song data, the ETL performs the following tasks:
* converts any fields of only whitespace into a Numpy.NaN data type.  This ensures these fields will be stored in the database as a NULL value
* converts any invalid year value to -1 to easily identify invalid records as well as to ensure the field data type remains INT (a NULL value in this column would require the larger FLOAT data type)
* extracts the required fields to populate the Songs dimension table
* inserts these values into the database (**Note**: if a duplicate song_id identified when inserting, the new records is ignored; the assumption being that the song details remain static)
* extracts the required fields to populate the Artists dimension table
* inserts these values into the database (**Note**: if a duplicate artist_id is identified when inserting, the new record is used to update the existing row; the assumption being that over time the artist details may be updated)

In processing the log data, the ETL performs the following tasks:
* filters the records to only show those actions performed on the 'NextSong' page
* converts the timestamp into a datetime field and extracts values to populate the time dimension table
* inserts the time values into the Time dimension table
* extracts the required fields to populate the Users dimension table
* inserts these values into the database (**Note**: if a duplicate user_id is identified when inserting, the new record is used to update the existing row; the assumption being that over time the user details may be updated)
* queries the Songs and Artists dimension tables to obtain the artist_id and song_id values assocated with the log entry's song title, artist name, and song duration
* extracts the required fields to populate the Song Plays fact table
* inserts these values into the database (**Note**: as this is a fact table, new rows are inserted with no validation if any duplicates are identified in a key)
 
## Data Analysis Discussion

This is just sample data so it is understandable that there are some data quality issues.

I think the most important one is that there are very few records in the songs and logs source files that can be joined.

For example, there are only 4 song titles that are common across files.  And of those, 3 have a 'length' value that is greater than the song 'duration'.

When looking at the artists, there are 11 in common.

The implication of this is that the ETL process to get the song_id and artist_id for a given log entry fails in all cases.  So these columns are NULL in the songplays fact table.

Further, the user can do no analysis of song plays without this data.

There are two possible ways around this:
* obtain a better sample of songs (we only have access to 71 records)
* use the song title and artist name values from the log entries to populate the song and artist dimension tables.

The second option would ensure referential integrity across all tables.  However, the ETL query will still fail if it uses the 'length' attribute as part of the filter.  Ideally, the song and artist should be unique enough to return a single song_id and artist_id.  The second option would also require a surrogate key for the song title and artist name combination rather than rely on the artist_id or song_id attributes in the songs source file.  There would be some overhead in implementing and maintaining this I guess (for example, dim_artists would need to be loaded first then a new query against this table when populating the dim_songs table).

The first option would be the truest to the source data (assuming a full simulated set exists).

The recommended course of action for users to get some value out of the data now is to implement Option 2 above and accept that many of the existing dim_songs and dim_artists fields will be blank (because we only have 71 entries from the source file).

Some further recommendations would be to do additional cleansing of the data.  For example, removing double quotes in the user_agent column.  It might also be useful to split out the location column into separate city and state columns.