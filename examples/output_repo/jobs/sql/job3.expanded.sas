proc sql;
  create table work.customer_rollup as
  select customer_id,
         sum(amount) as total_amount,
         max(amount) as max_amount
  from sales_local
  where region = 'East'
  group by customer_id
  order by customer_id;
quit;
