CREATE DATABASE assign2_zeina;

set hive.exec.dynamic.partition=true;

CREATE TABLE songs(artist_id string, artist_latitude double, artist_location string, artist_longitude double, duration double, num_songs integer, song_id string, title string)
PARTITIONED BY (year integer, artist_name string) 
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

CREATE EXTERNAL TABLE songs_ext(artist_id string, artist_latitude double, artist_location string, artist_longitude double, duration double, num_songs integer, song_id string, title string)
PARTITIONED BY (artist_name string, year integer)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/songs';

!hdfs dfs -put /employee/songs.csv  /user/hive/warehouse/songs/assign2_zeina.db/songs_ext;

select * from songs_ext limit 10;

ALTER TABLE songs_ext ADD IF NOT EXISTS PARTITION (artist_name='Marc Shaiman', year=2008) 
LOCATION '/part_table';

!hdfs dfs -put /employee/songs.csv  /part_table;
Show partitions songs; 

CREATE TABLE staging_tab(artist_id string, artist_latitude double, artist_location string, artist_longitude double,  artist_name string, duration double, num_songs integer, song_id string, title string, year integer)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/employee/songs.csv' OVERWRITE INTO TABLE staging_tab;

from staging_tab
insert overwrite table songs partition (artist_name, year)
select *;

Truncate table songs;

from staging_tab
insert overwrite table songs partition (year,artist_name )
select *;

Truncate table songs;

from staging_tab
insert overwrite table songs partition (year = 1994, artist_name)
select artist_id, artist_latitude, artist_location, artist_longitude, 
duration, num_songs , song_id , title string, artist_name 
where year = 1994;
 
CREATE TABLE avro_table LIKE staging_tab
STORED AS AVRO;

CREATE TABLE parquet_table LIKE staging_tab
STORED AS PARQUET;
