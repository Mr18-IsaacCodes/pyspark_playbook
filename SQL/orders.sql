CREATE TABLE mydb.customer_orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    order_amount INTEGER
);
insert into mydb.customer_orders values(1,100,cast('2022-01-01' as date),2000),(2,200,cast('2022-01-01' as date),2500),(3,300,cast('2022-01-01' as date),2100)
,(4,100,cast('2022-01-02' as date),2000),(5,400,cast('2022-01-02' as date),2200),(6,500,cast('2022-01-02' as date),2700)
,(7,100,cast('2022-01-03' as date),3000),(8,400,cast('2022-01-03' as date),1000),(9,600,cast('2022-01-03' as date),3000)
;
select * from mydb.customer_orders;
-- For each customer, how many times did they place their first order, and how many times did they place subsequent orders?
WITH first_visit AS (
    SELECT customer_id, MIN(order_date) AS first_order_date
    FROM mydb.customer_orders
    GROUP BY customer_id
),
order_flags AS (
    SELECT co.*, fv.first_order_date,
        CASE WHEN co.order_date = fv.first_order_date THEN 1 ELSE 0 END AS is_first_order,
        CASE WHEN co.order_date <> fv.first_order_date THEN 1 ELSE 0 END AS is_subsequent_order
    FROM mydb.customer_orders co
    JOIN first_visit fv
        ON co.customer_id = fv.customer_id
)
SELECT r.customer_id, 
       SUM(r.is_first_order) AS first_order_count, 
       SUM(r.is_subsequent_order) AS subsequent_order_count
FROM order_flags r
GROUP BY r.customer_id;
