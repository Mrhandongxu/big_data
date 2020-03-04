select actor,
       movie
from actor_movie_role
group by actor,
         movie having count(role) = 1;
select column_name
from information_schema.columns
where table_name = 'new_relation';


select *
from genre
where id = 8;

