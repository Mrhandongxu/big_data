drop database if exists imdbdx;
drop user if exists dongxu;
create user dongxu with password '199710' createdb;
create database imdbdx owner dongxu encoding utf8;
-- relogin as user dongxu and connect to database imdbdx
-- may drop these table to reset
drop table if exists movie;
drop table if exists person;
drop table if exists act;
drop table if exists write;
drop table if exists direct;
drop table if exists produce;
drop table if exists rating;
create table person(
  pconst int not null, -- integer id for a person
  name varchar not null, -- name by which the person is most often credited
  birthYear int, -- birth year in YYYY format or null for unknown
  deathYear int, -- death year in YYYY format or null for unknown
  primary key (pconst)
);
create table movie(
  mconst bigint not null, -- integer id for a movie
  title varchar, -- original title
  releaseTime int, -- release year of movie in YYYY format
  runtimeMinutes int, -- runtime, in minutes
  genres varchar[], -- includes up to three genres associated with the title
  primary key (mconst)
);
create table act(
  mconst bigint not null, -- integer id for a movie
  pconst int not null, -- integer id for a person
  primary key (pconst, mconst)
);
create table write(
  mconst bigint not null, -- integer id for a movie
  pconst int not null, -- integer id for a person
  primary key (pconst, mconst)
);
create table direct(
  mconst bigint not null, -- integer id for a movie
  pconst int not null, -- integer id for a person
  primary key (pconst, mconst)
);
create table produce(
  mconst bigint not null, -- integer id for a movie
  pconst int not null, -- integer id for a person
  primary key (pconst, mconst)
);
create table rating(
  mconst bigint not null, -- integer id for title
  averageRating real, -- average of all individual user ratings
  numVotes bigint, -- number of received votes
  primary key (mconst)
);