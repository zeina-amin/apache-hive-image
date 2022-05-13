CREATE TABLE events(artist string,auth string, firstName string, gender string, itemInSession integer, lastName string,length float, level string, location string, method string,page string, registration string, sessionId integer,song string, status integer, ts string, userAgent string, userId integer)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/employee/events.csv' OVERWRITE INTO TABLE events;

Select firstName, sessionId, 
First_value(song) over(partition by sessionId), last_value(song) over(partition by sessionId rows between unbounded preceding and unbounded following) 
From events;

Select distinct song, firstName, dense_rank() over(partition by song order by userId)
from events;

Select distinct song, firstName, row_number() over(partition by song order by userId)
from events
where page = "NextSong";

select count(song) 
from events 
group by artist,location
GROUPING SETS (artist,(artist,location),());

select artist,location,count(song) 
from events 
group by artist,location
GROUPING SETS (artist,(artist,location),());

Select firstName, lastName, lag(song) over(order by userId) as previous_song, lead(song) over(order by userId) as next_song
From events;

Select userId, song, ts
From events
Order by userId, song, ts;

Select userId, song, ts
From events
cluster by userId, song, ts;
