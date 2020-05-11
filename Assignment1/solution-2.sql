create table users (  userid int not null,
                      name text not null,
                      constraint pk_users primary key(userid)
                   );

create table movies(movieid integer not null,
                    title text not null,
                    constraint pk_movies primary key(movieid));

create table taginfo(tagid int not null,
                     content text not null,
                     constraint pk_info primary key (tagid));

create table genres (genreid integer not null,
                     name text not null,
                     constraint pk_genres primary key(genreid));

create table ratings(userid int not null,
                     movieid int not null,
                     rating numeric(3,2) not null,
                     constraint check_ratinginrange check (rating >= 0 AND rating <= 5),
                     timestamp  bigint not null,
                     constraint pk_rating primary key (userid,movieid),
                     constraint fk_rusers foreign key(userid) references users(userid) on delete cascade,
                     constraint fk_rmovies foreign key(movieid) references movies(movieid) on delete cascade);


create table tags(userid int not null ,
                  movieid int not null,
                  tagid int not null,
                  timestamp bigint not null,
                  constraint fk_tusers foreign key(userid) references users(userid) on delete cascade,
                  constraint fk_tmovies foreign key(movieid) references movies(movieid) on delete cascade,
                  constraint fk_tinfo foreign key(tagid) references taginfo(tagid) on delete cascade,
                  constraint pk_tags primary key(userid,movieid,tagid));



create table hasagenre(movieid int not null ,
                       genreid int not null,
                       constraint pk_hasgenremovie primary key(genreid,movieid),
                       constraint fk_hasmovies foreign key(movieid) references movies(movieid) on delete cascade,
                       constraint fk_hasgenres foreign key(genreid) references genres(genreid) on delete cascade);
