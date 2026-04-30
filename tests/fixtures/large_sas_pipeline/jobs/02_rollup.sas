proc sql;
  create table work.customer_rollup as
  select customer_id,
         sum(revenue) as total_revenue,
         max(amount) as max_amount
  from work.orders_open
  where region ne 'West'
  group by customer_id
  order by customer_id;
quit;

proc freq data=work.orders_open;
  tables customer_id*region / out=work.customer_region_freq;
run;

proc means data=work.orders_open noprint;
  class region;
  var revenue;
  output out=work.region_summary mean= sum=;
run;
