--     1
select count(*)
from movie_actor
where not exists (select * from actor_movie_role
where movie_actor.actor = actor_movie_role.actor);


--     2
select distinct mb.name from movie_actor as ma 
join (select id, name from member where deathYear is null and name like 'Phi%') as mb
on ma.actor = mb.id
where not exists(select * from movie where startYear = 2014 and ma.movie = movie.id);


--     3
select mm.name, count(mp.movie) as ct from movie_producer as mp, member as mm
where 
exists(select * from movie where startYear = 2017 and movie.id = mp.movie) 
and 
exists(select * from member where name like '%Gill%' and member.id = mp.producer
and mp.producer = mm.id)
group by mm.name
order by ct desc
limit 1;


--     4
select mm.name, count(movie) as ct from movie_producer as mp, member as mm
where exists (select * from movie where runtime > 120 and movie.id = mp.movie)
and exists (select * from member where deathYear is null and mm.id = mp.producer)
and mp.producer = mm.id
group by mm.name
order by ct desc
limit 1;

--     5
select distinct mm.name, rl.role
from actor_movie_role as amr join member as mm 
on mm.id = amr.actor
join role as rl 
on rl.id = amr.role
where (rl.role like '%Jesus%' or rl.role like '%Christ%') and mm.deathYear is null;
