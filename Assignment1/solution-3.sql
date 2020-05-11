CREATE TABLE query1  as
select name,count(*) as moviecount from movies,hasagenre,genres where hasagenre.movieid= movies.movieid and hasagenre.genreid=genres.genreid group by genres.genreid;

CREATE TABLE query2 as
select distinct name,avg(rating) as rating from ratings,genres,hasagenre where hasagenre.genreid=genres.genreid and ratings.movieid=hasagenre.movieid  group by genres.name ;

CREATE TABLE query3 as
(select  movies.title,count(*) as countofratings from movies,ratings where ratings.movieid=movies.movieid group by  movies.movieid having count(*) >=10 );

CREATE TABLE query4 as
select hasagenre.movieid,title from hasagenre, movies, genres where hasagenre.movieid=movies.movieid and hasagenre.genreid=genres.genreid and name = 'Comedy';

CREATE TABLE query5 as
select title,avg(rating) as average from ratings,movies where ratings.movieid= movies.movieid group by movies.movieid;

CREATE TABLE query6 as
select avg(rating) as average from ratings,genres,hasagenre where  ratings.movieid = hasagenre.movieid and genres.genreid = hasagenre.genreid and name='Comedy' ;

CREATE TABLE query7 as
select avg(R.rating) as average from ratings R where movieid in
(select ratings.movieid from ratings,genres,hasagenre where  ratings.movieid = hasagenre.movieid and genres.genreid = hasagenre.genreid and name='Comedy'
intersect
select ratings.movieid from ratings,genres,hasagenre where  ratings.movieid = hasagenre.movieid and genres.genreid = hasagenre.genreid and name='Romance');

CREATE TABLE query8 as
SELECT avg(R.rating) as average from ratings R where movieid in
(select ratings.movieid from ratings,genres,hasagenre where  ratings.movieid = hasagenre.movieid and genres.genreid = hasagenre.genreid and name='Romance'
Except
select ratings.movieid from ratings,genres,hasagenre where  ratings.movieid = hasagenre.movieid and genres.genreid = hasagenre.genreid and name='Comedy');

CREATE TABLE query9 as
select movieid,rating from ratings where userid= :v1;

create view MoviesRatedByUser as
select movieid, rating from ratings where userid= :v1;

create view MoviesNotRatedByUser as
select movieid, title from movies where movieid not in (select movieid from ratings where userid= :v1);

create view AverageOfRatedMovies as
select movieid,avg(rating) from ratings where movieid in (select movieid from MoviesRatedByUser) group by movieid order by movieid asc;

create view AverageOfNotRatedMovies as
select movieid,avg(rating) from ratings where movieid not in (select movieid from MoviesRatedByUser) group by movieid order by movieid asc;

create view similarity as
select first.movieid as seen,second.movieid as notseen,1 - (abs(first.avg -second.avg))/5 as sim from AverageOfRatedMovies first,AverageOfNotRatedMovies second;

create view recommendation as
select title from movies,
 (select similarity.notseen as id, sum(similarity.sim * rating)/sum(similarity.sim) as average from MoviesRatedByUser,similarity where similarity.seen=
 MoviesRatedByUser.movieid group by similarity.notseen)probability where movies.movieid = Probability.id and Probability.average>3.9;
