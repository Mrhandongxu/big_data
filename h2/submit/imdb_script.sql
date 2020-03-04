-- may drop these table to reset
drop table if exists Movie;
drop table if exists Genre;
drop table if exists Movie_Genre;
drop table if exists Member;
drop table if exists Movie_Actor;
drop table if exists Movie_Writer;
drop table if exists Movie_Director;
drop table if exists Movie_Producer;
drop table if exists Role;
drop table if exists Actor_Movie_Role;

create table movie(
  id bigint not null, -- integer id for a movie
  type varchar, -- type of movie
  originalTitle varchar, -- original title
  startYear int, -- start year of movie in YYYY format
  endYear int, -- end year of movie in YYYY format
  runtime int, -- runtime, in minutes
  avgRating real, -- average of all individual user ratings
  numVotes int, -- num of votes recieved
  primary key (id)
);

create table genre(
  id int not null, -- integer id for a genre
  genre varchar not null, -- name of the genre
  primary key (id)
);

create table movie_genre (
  genre int not null, -- id of genre
  movie int not null, -- id of movie reference to a movie
  primary key (genre, movie)
);

create table member(
  id bigint not null, -- integer id for a member
  name varchar, -- name by which the member is most often credited
  birthYear int, -- birth year in YYYY format or null for unknown
  deathYear int, -- death year in YYYY format or null for unknown
  primary key (id)
);

create table movie_actor(
  actor bigint not null, -- integer id for an actor
  movie bigint not null, -- integer id for a movie
  primary key (actor, movie)
);

create table movie_writer(
  writer bigint not null, -- integer id for a writer
  movie bigint not null, -- integer id for a movie
  primary key (writer, movie)
);

create table movie_director(
  director bigint not null, -- integer id for a director
  movie bigint not null, -- integer id for a movie
  primary key (director, movie)
);

create table movie_producer(
  producer bigint not null, -- integer id for a producer
  movie bigint not null, -- integer id for a movie
  primary key (producer, movie)
);

create table role(
  id int not null, -- id of a character
  role varchar, -- name of the character
  primary key (id)
);

create table actor_movie_role(
  actor int not null, -- id of actor
  movie int not null, -- id of movie the actor act in
  role int not null, -- id of role the actor plays in the movie
  primary key (actor, movie, role)
);
