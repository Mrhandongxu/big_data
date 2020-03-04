-- drop table to

drop table if exists new_relation;

create table new_relation( movieid int, type varchar(50), startyear smallint, runtime int, avgrating numeric(3, 1), genreid int, genre varchar(50), memberid int, birthyear smallint, role int);

 -- insert data from join of tables to new_relation

explain analysis
insert into new_relation
select mv.id,
       mv.type,
       mv.startYear,
       mv.runtime,
       mv.avgRating,
       gr.id,
       gr.name,
       mb.id,
       mb.birthyear,
       amr.role
from
 (select id,
         type,
         startYear,
         runtime,
         avgRating
  from movie
  where runtime > 90) as mv
join (select actor, movie, role
  from actor_movie_role as inamr
  where not exists
  (select movie,
            actor
     from actor_movie_role
     where movie = inamr.movie
     group by movie,
              actor having count(role) > 1)) as amr on amr.movie = mv.id
join movie_genre as mg on mv.id = mg.movie
join genre as gr on gr.id = mg.genre
join
 (select id,
         birthyear
  from member) as mb on mb.id = amr.actor;

 
 -- add keys

alter table new_relation primary key (movieid,genreid,memberid);


alter table new_relation
foreign key (genreid) references movie_genre(genre);


alter table new_relation
foreign key (memberid) references member(id);


alter table new_relation
foreign key (movieid) references movie(id);