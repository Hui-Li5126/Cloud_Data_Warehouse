import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"


# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events (
             artist varchar(500),
             auth varchar(50),
             firstName varchar(100),
             gender varchar(10),
             itemInSession int,
             lastName varchar(200),
             length decimal,
             level varchar(10),
             location varchar(500),
             method varchar(10),
             page varchar(25),
             registration decimal,
             sessionId int,
             song varchar(300),
             status int,
             ts bigint,
             userAgent varchar(500),
             userId int)""")
 
staging_songs_table_create = ("""
create table if not exists staging_songs (
             num_songs int,
             artist_id varchar(25),
             artist_latitude decimal,
             artist_longitude decimal,
             artist_location varchar(500),
             artist_name varchar(500),
             song_id varchar(100) ,
             title varchar(500) ,
             duration decimal,
             year int)
""")

artist_table_create = ("""
create table if not exists artists (
             artist_id varchar(25) PRIMARY KEY sortkey, 
             name varchar(500) not null, 
             location varchar(500), 
             latitude decimal, 
             longitude decimal)
             diststyle all""")


song_table_create = ("""
create table if not exists songs (
             song_id varchar(100) PRIMARY KEY sortkey, 
             title varchar(500) not null, 
             artist_id varchar(25) not null, 
             year int, 
             duration decimal)
             diststyle all""")

user_table_create = ("""
create table if not exists users (
             user_id int PRIMARY KEY sortkey, 
             first_name  varchar(100) not null, 
             last_name varchar(200) not null , 
             gender varchar(10), 
             level varchar(10))
             diststyle all""")

time_table_create = ("""
create table if not exists time (
             start_time timestamp PRIMARY KEY sortkey, 
             hour int, 
             day int, 
             week int, 
             month int, 
             year int, 
             weekday int)
             diststyle all""")

songplay_table_create = ("""
create table if not exists songplays (
             songplay_id int IDENTITY(0,1) PRIMARY KEY, 
             start_time timestamp sortkey, 
             user_id int, 
             level varchar(10), 
             song_id varchar(100), 
             artist_id varchar(25), 
             session_id int, 
             location varchar(500) distkey, 
             user_agent varchar(500))""")

# STAGING TABLES
staging_events_copy = ("""copy staging_events from {} \
                          credentials 'aws_iam_role={}' \
                          region 'us-west-2' FORMAT AS JSON {} 
                          TIMEFORMAT as 'epochmillisecs'; 
                       """.format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH")))


staging_songs_copy = ("""copy staging_songs from {} \
                         credentials 'aws_iam_role={}' \
                         region 'us-west-2' format as json 'auto';
                      """.format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN")))


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent )
                            SELECT distinct TIMESTAMP 'epoch' + events.ts/1000 *INTERVAL '1 second',
                                   events.userId, events.level, song1.song_id, song2.artist_id, events.sessionId,
                                   events.location, events.userAgent
                              FROM staging_events events 
                              LEFT JOIN staging_songs song1 on events.song=song1.title 
                              LEFT JOIN staging_songs song2 on events.artist=song2.artist_name""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT distinct userId, firstName, lastName, gender, level 
                          FROM staging_events where userId is not null""")


song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                            SELECT distinct song_id, title, artist_id, year, duration 
                              FROM staging_songs""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
                            FROM staging_songs""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
               SELECT distinct  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second',
               extract(hr from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')), 
               extract(day from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')), 
               extract(w from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')), 
               extract(mon from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')), 
               extract(y from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')), 
               extract(weekday from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'))
               FROM staging_events""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
