create database parking_violations owner postgres encoding utf8;
-- switch to parking_violations: \c parking_violations
create table ticket(
  ticketId bigint not null primary key, -- integer id for a ticket
  issueDate date, -- date of issued ticket
  violCode int, -- number for different violation type. See it on website www1.nyc.gov
  issueAgency varchar, -- upper case of first letter of issuing agency
  violCounty varchar, -- county where violation happened
  violTime varchar, -- time of violation in hhmmP(pm)/hhmmA(am) format
  frontOrOppo varchar, -- The place where the ticket was issued (“front of” or “opposite,” etc.),
  lawSect varchar, -- section of law it violated
  subSect varchar -- sub-section of law it violated
);
create table vehicle(
  plate varchar not null primary key, -- plate of vehicle as primary key
  year int, -- produce year of vehicle
  make varchar, -- make of vehicle(BWM, Audi, etc.)
  type varchar, -- type of vehicle(sedan, van, etc.)
  color varchar, -- color of vehicle
  registState varchar, -- registration state of vehicle
  plateType varchar, -- type of plate(PAS, COM, etc.)
  expTime int, -- expiration date in YYYYMMDD format
  isRegist boolean -- 0: registered; 1: not registered; unkown: 
);
create table issuer(
  issuerId int not null primary key, -- id of an officer(issuer)
  precinct int, -- precinct the issuer administer
  squad varchar -- squad the issuer belongs to
);
create table issueBy(
  ticketId bigint not null, -- integer id for a ticket
  issuerId int not null, -- id of an officer(issuer)
  command varchar, -- command code when issuing ticket
  primary key (ticketId, issuerId),
  foreign key (ticketId) references ticket(ticketId),
  foreign key (issuerId) references issuer(issuerId)
);
create table issueTo(
  ticketId bigint not null, -- integer id for a ticket
  plate varchar not null, -- plate of vehicle as primary key
  primary key (ticketId, plate),
  foreign key (ticketId) references ticket(ticketId),
  foreign key (plate) references vehicle(plate)
);
