# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                                                    songplay_id serial NOT NULL PRIMARY KEY , 
                                                                    start_time bigint NOT NULL REFERENCES time(timestamp), 
                                                                    user_id int NOT NULL REFERENCES users(user_id) , 
                                                                    level varchar, 
                                                                    song_id varchar REFERENCES songs(song_id), 
                                                                    artist_id varchar REFERENCES artists(artist_id), 
                                                                    session_id int, 
                                                                    location varchar, 
                                                                    user_agent varchar
                                                                    )""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                                            user_id int NOT NULL PRIMARY KEY, 
                                                            first_name varchar, 
                                                            last_name varchar, 
                                                            gender varchar, 
                                                            level varchar
                                                            )""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                                            song_id varchar NOT NULL PRIMARY KEY, 
                                                            title varchar, 
                                                            artist_id varchar REFERENCES artists(artist_id), 
                                                            year int, 
                                                            duration int
                                                            )""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                                                artist_id varchar NOT NULL PRIMARY KEY, 
                                                                name varchar, 
                                                                location varchar, 
                                                                latitude float8, 
                                                                longitude float8
                                                                )""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                                            timestamp bigint NOT NULL PRIMARY KEY, 
                                                            hour int NOT NULL, 
                                                            day int NOT NULL, 
                                                            week int NOT NULL, 
                                                            month int NOT NULL, 
                                                            year int NOT NULL, 
                                                            weekday int NOT NULL
                                                            )""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""")

user_table_insert = ("""INSERT INTO users AS u(user_id, first_name, last_name, gender, level) 
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (user_id) DO UPDATE
                        SET level = EXCLUDED.level
                        WHERE u.level = 'free'""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) 
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (song_id) DO NOTHING""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude) 
                          VALUES(%s,%s,%s,%s,%s)
                          ON CONFLICT (artist_id) DO NOTHING""")

time_table_insert = ("""INSERT INTO time(timestamp, hour, day, week, month, year, weekday) 
                        VALUES(%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (timestamp) DO NOTHING""")

# FIND SONGS

song_select = ("""SELECT 
                    s.song_id, 
                    a.artist_id 
                FROM songs s 
                INNER JOIN artists a 
                    ON s.artist_id = a.artist_id 
                WHERE 1 = 1
                AND s.title = %s 
                AND a.name = %s 
                AND s.duration = %s""")

# QUERY LISTS

create_table_queries = [time_table_create, artist_table_create, song_table_create, user_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]