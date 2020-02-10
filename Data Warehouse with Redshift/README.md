- [Task Description](#task-description)
- [Data Source](#data-source)
- [Staging Table Schema](#staging-table-schema)
- [Anlytical Table Schema](#anlytical-table-schema)
- [ETL Process](#etl-process)
  - [Summary](#summary)
  - [Files in the Reporistory](#files-in-the-reporistory)
  - [Workflow](#workflow)
- [Analytics](#analytics)
  
# Task Description
The raw data files (JSON) of a music app are stored in AWS S3 bucket. Since analysis toward the data is required, data needs to transformed and then stored in the AWS Redshift data warehouse. A ETL workflow includes two main steps, loading to staging tables from raw data files and loading to anlytical tables from staging tables. 

Before the ETL process is implemented, a user, a role and a AWS Redshift cluster needs to be created. The user should have the permision to create the cluster and manange other resources. The role should have the access to the S3 bucket so that JSON files can be read from there. The Redshift cluster should have the security group that allow access from outside AWS. All these works are done in the first place.      

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

# Staging Table Schema 
The staging tables works as a container to hold all the data from JSON files, therefore, there are two staing tables, stgSongs and stgEvents. 

The stgSongs table is used to store the data from song_data files. Its data model is shown below,

stgSongs
| stgSongs_id (PK) | num_songs |      artist_id     |   artist_name   | artist_location | artist_latitude | artist_longitude |       song_id      |   title   | year |  duration  |
|------------------|-----------|--------------------|-----------------|-----------------|-----------------|------------------|--------------------|-----------|------|- ----------|
|        1         |     1     | AR36F9J1187FB406F1 | Bombay Rockers  |     Denmark     |     56.27609    |     9.51695      | SOBKWDJ12A8C13B2F3 | Wild Rose | 1978 |  230.71302 | 

The mapping bewteen the song_data file and the stgSongs table is

|   song_data file  |      stgSongs     |
|-------------------|-------------------|
|     num_songs     |     num_songs     |
|     artist_id     |     artist_id     |
|    artist_name    |    artist_name    |
|  artist_location  |  artist_location  |
|  artist_latitude  |  artist_latitude  |
|  artist_longitude |  artist_longitude |
|      song_id      |      song_id      |
|       title       |       title       |
|       year        |       year        |
|     duration      |     duration      |

The stgEvents table holds the data from log_data files and its data model is as follows,

stgEvents
| stgevents_id (PK) |     artist     |   auth    | firstName | lastName | gender | itemInSession |  length   | level |              location              | method |   page   |    song   |  status  |       ts      | registration  | sessionId |     userAgent     | userId |
|-------------------|----------------|-----------|-----------|----------|--------|---------------|-----------|-------|------------------------------------|--------|----------|-----------|----------|---------------|---------------|-----------|-------------------|--------|
|         1         | Bombay Rockers | Logged In |   Ryan    |  Smith   |   M    |       1       | 655.77751 | free  | San Jose-Sunnyvale-Santa Clara, CA |  PUT   | NextSong | Wild Rose |   200    | 1542241826796 | 1541016707796 |    583    | \"Mozilla\/5.0... |   26   |

The mapping bewteen the log_data file and the stgEventss table is 

|   song_log file   |     stgEvents     |
|-------------------|-------------------|
|      artist       |      artist       |
|       auth        |       auth        |
|     firstName     |     firstName     |
|     lastName      |     lastName      |
|      gender       |      gender       |
|   itemInSession   |   itemInSession   |
|      length       |      length       |
|       level       |       level       |
|     location      |     location      |
|      method       |      method       |
|       page        |       page        |
|   itemInSession   |   itemInSession   |
|       song        |       song        |
|      status       |      status       |
|        ts         |        ts         |
|   registration    |   registration    |
|     sessionId     |     sessionId     |
|     userAgent     |     userAgent     |
|      userId       |      userId       |

# Anlytical Table Schema
There are four dimension tables, songs, artists, users and time, and one fact table, songplays. The data models of these tables and their mapping to staging tables are shown as follows,

artists
| artist_key (PK) |      artist_id     |       name      |   location   | latitude | longitude |
|-----------------|--------------------|-----------------|--------------|----------|-----------|
|        1        | AR36F9J1187FB406F1 | Bombay Rockers  |   Denmark    | 56.27609 |  9.51695  |

mapping 
|     stgSongs      |      artists      |
|-------------------|-------------------|
|     artist_id     |     artist_id     |
|    artist_name    |        name       |
|  artist_location  |      location     |
|  artist_latitude  |      latitude     |
|  artist_longitude |      longitude    |

songs
| song_key(PK) |      song_id       |   title   |      artist_id     | year |  duration  |
|--------------|--------------------|-----------|--------------------|------|------------|
|      1       | SOBKWDJ12A8C13B2F3 | Wild Rose | AR36F9J1187FB406F1 |  0   |  230.71302 |

mapping 
|      stgSongs     |       songs       |
|-------------------|-------------------|
|      song_id      |      song_id      |
|       title       |       title       |
|     artist_id     |     artist_id     |
|       year        |       year        |
|     duration      |     duration      |

users
| user_key(PK) | user_id | first_name | last_name | gender | level |
|--------------|---------|------------|-----------|--------|-------|
|      1       |   26    |    Ryan    |   Smith   |   M    | free  |

mapping
|     stgEvents     |       users       |
|-------------------|-------------------|
|      userId       |     user_id       |
|     firstName     |    first_name     |
|     lastName      |     last_name     |
|      gender       |      gender       |
|       level       |       level       |

time
| time_key(PK) |   timestamp    | hour | day | week | month | year | weekday |
|--------------|----------------|------|-----|------|-------|------|---------|
|      1       | 1542241826796  |  0   | 29  |  48  |  11   | 2018 |    3    |

mapping
|     stgEvents     |       time        |
|-------------------|-------------------|
|        ts         |     timestamp     |
|        ts         |       hour        |
|        ts         |        day        |
|        ts         |       week        |
|        ts         |       month       |
|        ts         |       year        |
|        ts         |      weekday      |

songplays
| songplay_key (PK) |  time_key | user_key | song_key | artist_key | level | session_id |              location              |    user_agent     |
|-------------------|-----------|----------|----------|------------|-------|------------|------------------------------------|-------------------|
|         1         |     1     |    1     |     1    |     1      | free  |    583     | San Jose-Sunnyvale-Santa Clara, CA | \"Mozilla\/5.0... |

mapping
|     stgEvents     |      artists      |       songs       |       users       |        time       |     songplays     |
|-------------------|-------------------|-------------------|-------------------|-------------------|-------------------|
|                   |    artist_key     |                   |                   |                   |    artist_key     |
|                   |                   |     song_key      |                   |                   |     song_key      |
|                   |                   |                   |     user_key      |                   |     user_key      |
|                   |                   |                   |                   |     time_key      |     time_key      |
|       level       |                   |                   |                   |                   |       level       |
|     sessionId     |                   |                   |                   |                   |    session_id     |
|     location      |                   |                   |                   |                   |     location      |
|     userAgent     |                   |                   |                   |                   |    user_agent     |
 
Since the data source of the data warehouse are heterogeneous, the primary keys of the data source may have different data types and duplicates. In order to solve it, I added a auto increamental surrogate key to each analytical tables. When inserting data into songplays, surrogate keys including artist_key, song_key, user_key and time_key are retrieved from corresponding dimensional tables. What's more, since time table is quite large, the time_key is set as distkey to increase the join performance.

In addition, the star schema is applied to these table. There are song_key, artist_key, user_key, time_key in the songplays table. Therefore we can join the dimension tables with the fact table. Besides, the fact table has user level, session_id, location, and user_agent.

# ETL Process
## Summary
The ETL process implements loading data from raw data files to anlytical tables. Staging tables are used to import data from files directly and populate data into analytical tables. Therefore, the ETL process havs two steps, loading to staging tables from raw data files and loading to anlytical tables from staging tables. 

## Files in the Reporistory
 1. sql_queries.py: the python script that contains all the sql queries used in create_tables.py and etl.py;
 2. create_tables.py : the python script that create all the dimension and fact tables, and truncate these tables when you run it after the first use;
 3. etl.py: the python script that process the song_data and log_data files and populate data into the dimension and fact tables; 
 4. test.ipynb: the jupyter notebook that can use to see the sample data in each table.
 5. dwh.cfg: the configuration file that hold configuration of the Redshift cluster, IAM role, and S3 buckets

## Workflow
 1. Open the terminal on Linux or MAC OS (open Command Prompt on Windows);
 2. Run the create_tables.py to create tables (truncate tables after the first use);
    ```sh
    $ python create_tables.py
    ``` 
 3. Run the etl.py to process song_data and log_data files and populate data in to tables, and only new data will be inserted;
    ```sh
    $ python etl.py
    ``` 
 4. Open the test.ipynb with Jupyter Notebook to check the number of records of each analytical tables.

# Analytics
With the star schema, analysis towards the user behaviors can be achieved. Some examples are listed below.
 1. Having songs and songplays joined, the most and least popular songs can be found;
 2. Having artists and songplays joined, the most and least popular artists can be found;
 3. Having users and songplays joined, we can find which gender of users love to use the music service;
 4. Having time and songplays joined, we can find when the users like using the music service;
 5. We can find users from which area love to use the music service.