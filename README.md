Project Summary:
    This project is to create a star schema Redshift database to falicity analytics team to perforam queries on song play analysis, currently raw data resides in S3 bucket on AWS. In order to do this, I create a new IAM role to allow Redshift clusters to call AWS services on my behalf, then attach a Amazon S3 read only access to the role and create a Redshift cluster, after proper security group setting I connect to the cluster, create two staging tables first then load the raw data into two staging tables using COPY command, once two staging tables are in place, then create one fact and four dimension tables by querying the two staging tables, since fact table is relatively big so it is sorted and distributed to four nodes, while other fact tables are small so they are sorted by primary key and distributed to every node to enable faster joins.
   
   
Output files include:
    two staging tables: staging_events and staging_songs, they include all columns from the raw data in S3 bucket.
    one fact table: songplays
    four dimension tables: users, songs, artists, time
 They comprise below columns respectively:
    staging_events: artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page,
                    registration, sessionId, song, status, ts,  userAgent, userId
    staging_songs: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id,
                   title, duration, year
    songplays:     songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    users:         user_id, first_name, last_name, gender, level
    songs:         song_id, title, artist_id, year, duration
    artists:       artist_id, name, location, lattitude, longitude
    time:          start_time, hour, day, week, month, year, weekday
    

How to run the python scripts:
First run sql_queries.py, then create_tables.py, then etl.py

Python scripts:
sql_queries.py includes:
1)drop table syntax in case it already exists, 
2)create table scripts to create two staging tables, the four dimension tables and one fact table
3)copy S3 data into two staging tables
4)insert rows to the four dimension tables and one fact table
5)a query list to allow further iterate through all creation syntax

create_tables.py includes:
1)drop tables
2)create tables

etl.py is the pipeline to read and process files and insert them into designated tables.


