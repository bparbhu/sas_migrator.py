data work.orders_open;
  set raw.orders(keep=customer_id order_id order_date amount qty price status region where=(status = 'OPEN'));
  revenue = qty * price;
run;

proc sort data=work.orders_open out=work.orders_sorted;
  by customer_id order_date;
run;

data work.last_order;
  set work.orders_sorted;
  by customer_id order_date;
  if last.customer_id;
run;
