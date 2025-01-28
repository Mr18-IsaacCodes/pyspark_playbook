use mydb;
create table transactions (
id int primary key,
country varchar(15),
state varchar(15),
amount int,
trans_date date
);

insert into transactions values(1,'US','approved',1000,'2023-12-18');
insert into transactions values(2,'US','declined',2000,'2023-12-19');
insert into transactions values(3,'US','approved',2000,'2024-01-01');
insert into transactions values(4,'India','approved',2000,'2023-01-07');

select * from transactions;

-- Write a query to find the total number of transactions and their amount, the number of approved transactions and their amount, for a given month and country.

SELECT 
    MONTH(trans_date),
    country,
    COUNT(*) AS TOTAL_NO_TRANS,
    SUM(amount) AS total_amount,
    SUM(CASE
        WHEN state = 'approved' THEN amount
        ELSE 0
    END) AS approved_amount,
    COUNT(CASE
        WHEN state = 'approved' THEN 1
    END) AS approved_transactions
FROM
    transactions
GROUP BY MONTH(trans_date) , country