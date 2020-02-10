import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS stgEvents"
staging_songs_table_drop = "DROP TABLE IF EXISTS stgSongs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stgEvents (
                                                                        stgEvents_id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
                                                                        artist varchar,
                                                                        auth varchar,
                                                                        firstName varchar,
                                                                        gender varchar,
                                                                        itemInSession int,
                                                                        lastName varchar,
                                                                        length float,
                                                                        level varchar,
                                                                        location varchar,
                                                                        method varchar,
                                                                        page varchar,
                                                                        registration bigint,
                                                                        sessionId int,
                                                                        song varchar,
                                                                        status int,
                                                                        ts bigint,
                                                                        userAgent varchar,
                                                                        userId int
                                                                      )""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stgSongs (
                                                                        stgSongs_id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY, 
                                                                        num_songs int,
                                                                        artist_id varchar,
                                                                        artist_latitude float,
                                                                        artist_longitude float,
                                                                        artist_location varchar,
                                                                        artist_name varchar,
                                                                        song_id varchar,
                                                                        title varchar,
                                                                        duration float,
                                                                        year int                                                                         
                                                                      )""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                                                    songplay_key bigint IDENTITY(1,1) NOT NULL PRIMARY KEY, 
                                                                    time_key bigint NOT NULL REFERENCES time(time_key) DISTKEY, 
                                                                    user_key bigint NOT NULL REFERENCES users(user_key),   
                                                                    song_key bigint REFERENCES songs(song_key), 
                                                                    artist_key bigint REFERENCES artists(artist_key), 
                                                                    level varchar, 
                                                                    session_id int, 
                                                                    location varchar, 
                                                                    user_agent varchar
                                                                  )""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                                            user_key bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
                                                            user_id int NOT NULL, 
                                                            first_name varchar, 
                                                            last_name varchar, 
                                                            gender varchar, 
                                                            level varchar
                                                          )""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                                            song_key bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
                                                            song_id varchar NOT NULL, 
                                                            title varchar, 
                                                            artist_id varchar, 
                                                            year int, 
                                                            duration float
                                                          )""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                                                artist_key bigint IDENTITY(1,1) NOT NULL PRIMARY KEY, 
                                                                artist_id varchar NOT NULL, 
                                                                name varchar, 
                                                                location varchar, 
                                                                latitude float, 
                                                                longitude float
                                                              )""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                                            time_key bigint IDENTITY(1,1) NOT NULL PRIMARY KEY DISTKEY, 
                                                            timestamp bigint NOT NULL, 
                                                            hour int NOT NULL, 
                                                            day int NOT NULL, 
                                                            week int NOT NULL, 
                                                            month int NOT NULL, 
                                                            year int NOT NULL, 
                                                            weekday int NOT NULL
                                                         )""")


# STAGING TABLES
staging_events_copy = ("""copy stgEvents (artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
                          from '{}'
                          credentials 'aws_iam_role={}'
                          json '{}';""").format(config.get('S3','LOG_DATA').strip('\''), config.get('IAM_ROLE','ARN').strip('\''), config.get('S3','LOG_JSONPATH').strip('\''))

staging_songs_copy = ("""copy stgSongs (num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)
                          from '{}'
                          credentials 'aws_iam_role={}'
                          json 'auto';""").format(config.get('S3','SONG_DATA').strip('\''), config.get('IAM_ROLE','ARN').strip('\''))

# FINAL TABLES
songplay_table_insert = ("""INSERT INTO songplays (time_key, user_key, song_key, artist_key, level, session_id, location, user_agent)
                            SELECT DISTINCT
                                t.time_key,
                                u.user_key,
                                s.song_key,
                                a.artist_key,
                                se.level,
                                se.sessionId,
                                se.location,
                                se.userAgent    
                            FROM stgevents se
                            INNER JOIN users u
                             ON se.userId = u.user_id
                            INNER JOIN time t
                             ON se.ts = t.timestamp
                            LEFT JOIN songs s
                             ON se.song = s.title
                            LEFT JOIN artists a
                             ON se.artist = a.name
                            LEFT JOIN songplays sp
                             ON CAST(FUNC_SHA1(CAST(t.time_key AS varchar) + CAST(u.user_key AS varchar) + NVL(CAST(s.song_key AS varchar),'')
                                + NVL(CAST(a.artist_key AS varchar),'') + NVL(se.level,'') + NVL(CAST(se.sessionId AS varchar),'') 
                                + NVL(se.location,'') + NVL(se.userAgent,'')) AS VARCHAR)
                                = CAST(FUNC_SHA1(CAST(sp.time_key AS varchar) + CAST(sp.user_key AS varchar) + NVL(CAST(sp.song_key AS varchar),'')
                                + NVL(CAST(sp.artist_key AS varchar),'') + NVL(sp.level,'') + NVL(CAST(sp.session_id AS varchar),'') 
                                + NVL(sp.location,'') + NVL(sp.user_agent,''))AS VARCHAR)
                            WHERE sp.songplay_key IS NULL
                            AND se.page = 'NextSong';""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT
                            se.userId,
                            MAX(se.firstName),
                            MAX(se.lastName),
                            MAX(se.gender),
                            MAX(se.level)
                        FROM stgevents se
                        LEFT JOIN users u
                         ON se.userId = u.user_id
                        WHERE u.user_id is NULL
                        AND se.userId IS NOT NULL
                        GROUP BY se.userId;""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT 
                            ss.song_id, 
                            MAX(ss.title), 
                            MAX(ss.artist_id), 
                            MAX(ss.year), 
                            MAX(ss.duration)
                        FROM stgsongs ss
                        LEFT JOIN songs s
                         ON ss.song_id = s.song_id
                        WHERE s.song_id IS NULL
                        AND ss.song_id IS NOT NULL
                        GROUP BY ss.song_id;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT 
                             ss.artist_id, 
                             MAX(ss.artist_name),  
                             MAX(ss.artist_location),  
                             MAX(ss.artist_latitude),  
                             MAX(ss.artist_longitude)
                          FROM stgsongs ss
                          LEFT JOIN artists a
                           ON ss.artist_id = a.artist_id
                          WHERE a.artist_id IS NULL
                          AND ss.artist_id IS NOT NULL
                          GROUP BY ss.artist_id;""")

time_table_insert = ("""INSERT INTO time (timestamp, hour, day, week, month, year, weekday)
                        WITH date AS (
                        SELECT DISTINCT
                            ts,
                            timestamp 'epoch' + ts/1000 * interval '1 second' AS datetime
                        FROM stgevents
                        WHERE ts IS NOT NULL
                        AND page = 'NextSong')
                        SELECT DISTINCT 
                            d.ts,
                            extract(hour from d.datetime) AS hour,
                            extract(day from d.datetime) AS day,
                            extract(week from d.datetime) AS week,
                            extract(month from d.datetime) AS month,
                            extract(year from d.datetime) AS year,
                            extract(weekday from d.datetime) AS weekday
                        FROM date d
                        LEFT JOIN time t
                         ON d.ts = t.timestamp
                        WHERE t.timestamp IS NULL;""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, artist_table_create, song_table_create, user_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, song_table_insert, user_table_insert, time_table_insert, songplay_table_insert]