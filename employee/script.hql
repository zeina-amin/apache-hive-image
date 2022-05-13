use assign1_zeina;

drop table if exists songs_ext;

create external table songs_ext(artist_id string,
 artist_latitude double, 
 artist_location string,
 artist_longitude double,
 artist_name string,
 duration double,
 num_songs integer,
 song_id string,
 title string,
 year integer)
 
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/employee/songs.csv' OVERWRITE INTO TABLE songs_ext;