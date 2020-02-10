- [Data Source](#data-source)
- [Schema](#schema)
- [Files in the Reporistory](#files-in-the-reporistory)
- [ETL Process](#etl-process)
  - [Summary](#summary)
  - [Workflow](#workflow)
- [Analytics](#analytics)

# Data Source
The raw data comes from two types of files, song_data and log_data JSON files in the data folder.

The song_data file contains artist's informatin including artist_id, artist_name, artist_location, artist_latitude, and artist_longitude as well as song's information such as song_id,title, year, and duration. The sample song_data file is as follows,
    {
        "num_songs": 1, 
        "artist_id": "AR36F9J1187FB406F1", 
        "artist_latitude": 56.27609, 
        "artist_longitude": 9.51695, 
        "artist_location": "Denmark", 
        "artist_name": "Bombay Rockers", 
        "song_id": "SOBKWDJ12A8C13B2F3", 
        "title": "Wild Rose (Back 2 Basics Mix)", 
        "duration": 230.71302, 
        "year": 2
    }

The log_data file has users' information, for example, user_id, first_name, last_name, gender, and level. Also, the log_data file contains other inforamtion like when and where the user used the service. The sample song_data file is as follows,
    {
        "artist":"Bombay Rockers",
        "auth":"Logged In",
        "firstName":"Ryan",
        "gender":"M",
        "itemInSession":0,
        "lastName":"Smith",
        "length":655.77751,
        "level":"free",
        "location":"San Jose-Sunnyvale-Santa Clara, CA",
        "method":"PUT",
        "page":"NextSong",
        "registration":1541016707796.0,
        "sessionId":583,
        "song":"Wild Rose (Back 2 Basics Mix)",
        "status":200,
        "ts":1542241826796,
        "userAgent":"\"Mozilla\/5.0 (X11; Linux x86_64) AppleWebKit\/537.36 (KHTML, like Gecko) Ubuntu Chromium\/36.0.1985.125 Chrome\/36.0.1985.125 Safari\/537.36\"",
        "userId":"26"
    }

# Schema
There are four dimension tables, songs, artists, users and time, and one fact table, songplays under the database "sparkifydb". The data models of these tables are shown as follows,

artists
|   artist_id (PK)   |       name      |   location   | latitude | longitude |
|--------------------|-----------------|--------------|----------|-----------|
| AR36F9J1187FB406F1 | Bombay Rockers  |   Denmark    | 56.27609 |  9.51695  |

songs
|    song_id (PK)    |              title             |      artist_id     | year |  duration  |
|--------------------|--------------------------------|--------------------|------|------------|
| SOBKWDJ12A8C13B2F3 |  Wild Rose (Back 2 Basics Mix) | AR36F9J1187FB406F1 |  0   |  230.71302 |

users
| user_id (PK) | first_name | last_name | gender | level |
|--------------|------------|-----------|--------|-------|
|      26      |    Ryan    |   Smith   |   M    | free  |

time
| timestamp (PK) | hour | day | week | month | year | weekday |
|----------------|------|-----|------|-------|------|---------|
| 1542241826796  |  0   | 29  |  48  |  11   | 2018 |    3    | 

songplays
| songplay_id (PK) |  start_time   | user_id | level |      song_id       |     artist_id      | session_id |              location              |    user_agent     |
|------------------|---------------|---------|-------|--------------------|--------------------|------------|------------------------------------|-------------------|
|        1         | 1542241826796 |   26    | free  | SOBKWDJ12A8C13B2F3 | AR36F9J1187FB406F1 |    583     | San Jose-Sunnyvale-Santa Clara, CA | \"Mozilla\/5.0... |


Here, the star schema is applied to these table. There are song_id, artist_id, user_id, start_time in the songplays table. Therefore we can join the dimension tables with the fact table. Besides, the fact table has user level, session_id, location, and user_agent.

# Files in the Reporistory
 1. data: the folder contains song_data and log_data JSON files;
 2. sql_queries.py: the python script that contains all the sql queries used in create_tables.py and etl.py;
 3. create_tables.py : the python script that create all the dimension and fact tables, and truncate these tables when you run it after the first use;
 4. etl.py: the python script that process the song_data and log_data files and populate data into the dimension and fact tables;
 5. etl.ipynb: the jupyter notebook that explains each step in the etl.py;
 6. test.ipynb: the jupyter notebook that can use to see the sample data in each table.

# ETL Process
## Summary
The song_data files are used to populate the songs and artists tables, while the log_data files are used to generate data for users, time, and songplays tables. The original files are denormolized. For the tables, songs, artists, and users table are normalized, whereas the time and songplays tables are denormolized. Thus, the fact table can be used to conduct analysis withouth sacrifice the performance. For example, we can do find the number of songs each user heard join only the users. 

## Workflow
 1. Open the terminal on Linux or MAC OS (open Command Prompt on Windows);
 2. Run the create_tables.py to create tables (truncate tables after the first use);
    ```sh
    $ python create_tables.py
    ```
 3. Open the test.ipynb with Jupyter to check if tables are created (Do not forget to restart this notebook to close the connection to your database before next step);
 4. Run the etl.py to process song_data and log_data files and populate data in to tables;
    ```sh
    $ python etl.py
    ``` 
 5. Open the test.ipynb with Jupyter to check if data are populated into assocaited tables (Do not forget to restart this notebook to close the connection to your database before next step).

# Analytics
With the star schema, analysis towards the user behaviors can be achieved. Some examples are listed below.
 1. Having songs and songplays joined, the most and least popular songs can be found;
 2. Having artists and songplays joined, the most and least popular artists can be found;
 3. Having users and songplays joined, we can find which gender of users love to use the music service;
 4. Having time and songplays joined, we can find when the users like using the music service;
 5. We can find users from which area love to use the music service.