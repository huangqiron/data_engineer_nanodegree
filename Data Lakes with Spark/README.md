- [Task Description](#task-description)
- [Data Source](#data-source)
- [Anlytical Table Schema](#anlytical-table-schema)
- [ETL Process](#etl-process)
  - [Summary](#summary)
  - [Files in the Reporistory](#files-in-the-reporistory)
  - [Workflow](#workflow)
- [Analytics](#analytics)
  
# Task Description
The raw data files (JSON) of a music app are stored in a AWS S3 bucket. Since analysis toward the data is required, data needs to be transformed and then stored in another AWS S3 bucket. Apache Spark is the core of the A ETL workflow. The Spark cluster is maintained by Udacity.   

# Data Source
The raw data comes from two types of files, song_data and log_data JSON files stored in AWS S3 bucket, "udacity-dend".

The song_data file contains artist's informatin including artist_id, artist_name, artist_location, artist_latitude, and artist_longitude as well as song's information such as song_id,title, year, and duration. The sample song_data file is as follows,
    {
        "num_songs": 1, 
        "artist_id": "AR36F9J1187FB406F1", 
        "artist_latitude": 56.27609, 
        "artist_longitude": 9.51695, 
        "artist_location": "Denmark", 
        "artist_name": "Bombay Rockers", 
        "song_id": "SOBKWDJ12A8C13B2F3", 
        "title": "Wild Rose", 
        "duration": 230.71302, 
        "year": 1978
    }

The log_data file has users' information, for example, user_id, first_name, last_name, gender, and level. Also, the log_data file contains other inforamtion like when and where the user used the service. The sample song_data file is as follows,
    {
        "artist":"Bombay Rockers",
        "auth":"Logged In",
        "firstName":"Ryan",
        "gender":"M",
        "itemInSession":1,
        "lastName":"Smith",
        "length":655.77751,
        "level":"free",
        "location":"San Jose-Sunnyvale-Santa Clara, CA",
        "method":"PUT",
        "page":"NextSong",
        "registration":1541016707796,
        "sessionId":583,
        "song":"Wild Rose",
        "status":200,
        "ts":1542241826796,
        "userAgent":"\"Mozilla\/5.0 (X11; Linux x86_64) AppleWebKit\/537.36 (KHTML, like Gecko) Ubuntu Chromium\/36.0.1985.125 Chrome\/36.0.1985.125 Safari\/537.36\"",
        "userId":"26"
    }

# Anlytical Table Schema
There are four dimension tables, songs, artists, users and time, and one fact table, songplays. The data models of these tables and their mapping to staging tables are shown as follows,

artists
|   artist_id (PK)   |       name      |   location   | latitude | longitude |
|--------------------|-----------------|--------------|----------|-----------|
| AR36F9J1187FB406F1 | Bombay Rockers  |   Denmark    | 56.27609 |  9.51695  |

mapping 
|  song_data file   |      artists      |
|-------------------|-------------------|
|     artist_id     |     artist_id     |
|    artist_name    |        name       |
|  artist_location  |      location     |
|  artist_latitude  |      latitude     |
|  artist_longitude |      longitude    |

songs
|    song_id (PK)    |   title   |      artist_id     | year |  duration  |
|--------------------|-----------|--------------------|------|------------|
| SOBKWDJ12A8C13B2F3 | Wild Rose | AR36F9J1187FB406F1 |  0   |  230.71302 |

mapping 
|  song_data file   |       songs       |
|-------------------|-------------------|
|     song_id       |      song_id      |
|      title        |       title       |
|     artist_id     |     artist_id     |
|       year        |       year        |
|     duration      |     duration      |

users
| user_id (PK) | first_name | last_name | gender | level |
|--------------|------------|-----------|--------|-------|
|      26      |    Ryan    |   Smith   |   M    | free  |

mapping
|   log_data file   |       users       |
|-------------------|-------------------|
|      userId       |     user_id       |
|     firstName     |    first_name     |
|     lastName      |     last_name     |
|      gender       |      gender       |
|       level       |       level       |

time
| start_time (PK) | hour | day | week | month | year | weekday |
|-----------------|------|-----|------|-------|------|---------|
|  1542241826796  |  0   | 29  |  48  |  11   | 2018 |    3    |

mapping
|   log_data file   |       time        |
|-------------------|-------------------|
|        ts         |    start_time     |
|        ts         |       hour        |
|        ts         |        day        |
|        ts         |       week        |
|        ts         |       month       |
|        ts         |       year        |
|        ts         |      weekday      |

songplays
| songplay_id (PK)  | start_time | user_id  | song_id  | artist_id  | level | session_id |              location              |    user_agent     |
|-------------------|------------|----------|----------|------------|-------|------------|------------------------------------|-------------------|
|         1         |      1     |    1     |     1    |     1      | free  |    583     | San Jose-Sunnyvale-Santa Clara, CA | \"Mozilla\/5.0... |

mapping
|   log_data file   |       songs       |     songplays     |
|-------------------|-------------------|-------------------|
|                   |     artist_id     |     artist_id     |
|                   |      song_id      |      song_id      |
|      userId       |                   |      user_id      |
|        ts         |                   |     start_time    |
|       level       |                   |       level       |
|     sessionId     |                   |     sessionId     |
|     location      |                   |     location      |
|     userAgent     |                   |     userAgent     |

The star schema is applied to these table. There are song_key, artist_key, user_key, time_key in the songplays table. Therefore we can join the dimension tables with the fact table. Besides, the fact table has user level, session_id, location, and user_agent.

# ETL Process
## Summary
The ETL process implements loading data from raw data files to anlytical tables. The raw data are retrieved from one AWS S3 bucket, transformed in the Spark, and restored to another AWS S3 bucket. After reading the file, the file's schema are inferred on the fly. During the transfoormation, tables are created and stored as the parquet files.

## Files in the Reporistory
 1. etl.py: the python script that process the song_data and log_data files and populate data into the dimension and fact tables; 
 2. test.ipynb: the jupyter notebook that can use to see the sample data in each table.

## Workflow
 1. Open the terminal on Linux or MAC OS (open Command Prompt on Windows);
 2. Run the etl.py to process song_data and log_data files and populate data in to tables;
    ```sh
    $ python etl.py
    ``` 
 3. Open the test.ipynb with Jupyter Notebook to check the sample data in each table.

# Analytics
With the star schema, analysis towards the user behaviors can be achieved. Some examples are listed below.
 1. Having songs and songplays joined, the most and least popular songs can be found;
 2. Having artists and songplays joined, the most and least popular artists can be found;
 3. Having users and songplays joined, we can find which gender of users love to use the music service;
 4. Having time and songplays joined, we can find when the users like using the music service;
 5. We can find users from which area love to use the music service.