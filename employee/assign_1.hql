CREATE DATABASE assign1_zeina;

DESCRIBE DATABASE EXTENDED assign1_zeina;

CREATE DATABASE assign1_loc_zeinaa location ‘/hp_db/assign1_loc_zeina’;

use assign1_zeina;

create table assign1_intern_tab_zeina(emolyee_id integer, name string, age integer, city string)
row format delimited fields terminated by ',';

DESCRIBE FORMATTED assign1_intern_tab_zeina;

LOAD DATA LOCAL INPATH '/employee/employee_details.txt' OVERWRITE INTO TABLE assign1_intern_tab_zeina;

!hdfs dfs -copyFromLocal /employee/employee_details.txt  /user/hive/warehouse/assign1_zeina.db/assign1_intern_tab_zeina;

Select * from assign1_intern_tab_zeina
Limit 10;

use assign1_loc_zeina;

create external table assign1_intern_tab_zeina(emolyee_id integer, name string, age integer, city string);

DESCRIBE FORMATTED assign1_intern_tab_zeina;

!hdfs dfs -copyFromLocal /employee/employee_details.txt /zeina01;

!hdfs dfs -ls /zeina01;

use assign1_zeina;

drop table assign1_intern_tab_zeina;

use assign1_loc_zeinaa;

drop table assign1_intern_tab_zeina;

use assign1_zeina;

create table assign1_intern_tab_zeina(emolyee_id integer, name string, age integer, city string)
row format delimited fields terminated by ',';

use assign1_loc_zeina;

create external table assign1_intern_tab_zeina(emolyee_id integer, name string, age integer, city string);

DESCRIBE FORMATTED assign1_intern_tab_zeina;

DESCRIBE FORMATTED assign1_loc_zeina.assign1_intern_tab_zeina;

use assign1_zeina;

create table staging(emolyee_id integer, name string, age integer, city string)
row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '/employee/employee_details.txt' OVERWRITE INTO TABLE staging;

insert into assign1_zeina.assign1_intern_tab_zeina (emolyee_id, name, age , city ) 
select * from staging;

insert into assign1_loc_zeina.assign1_intern_tab_zeina (emolyee_id, name, age , city ) 
select * from staging;

DESCRIBE FORMATTED assign1_intern_tab_zeina;

DESCRIBE FORMATTED assign1_loc_zeina.assign1_intern_tab_zeina;

!wc /employee/songs.csv;

create table songs(artist_id string, artist_latitude double, artist_location string, artist_longitude double, artist_name string, duration double, num_songs integer, song_id string, title string, year integer)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/employee/songs.csv' OVERWRITE INTO TABLE songs ;

Select * from songs limit 10;

Select count(artist_id) from songs;

create external table songs_ext(artist_id string, artist_latitude double, artist_location string, artist_longitude double, artist_name string, duration double, num_songs integer, song_id string, title string, year integer)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

!hdfs dfs -put /employee/songs.csv  /user/hive/warehouse/assign1_zeina.db/songs_ext;

hive -e "select * from assign1_zeina.songs_ext limit 10";

hive -f /employee/script.hql

use assign1_zeina;

alter table assign1_intern_tab_zeina rename to assign1_loc_zeina.assign1_intern_tab;

DESCRIBE FORMATTED assign1_intern_tab;
