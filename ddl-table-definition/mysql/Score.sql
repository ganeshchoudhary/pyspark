create database creditdb;
create table creditdb.score (id int, applied_loan_id text, customer_id text, score_for_the_applied_loan text);
ALTER TABLE creditdb.score ADD PRIMARY KEY (id);

insert into creditdb.score (id, applied_loan_id, customer_id, score_for_the_applied_loan) values
(1, "13343", "1334","39"),
(2, "1332143", "133","39"),
(3, "1343", "334","3");