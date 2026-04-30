data work.order_sequence;
  set work.orders_sorted;
  by customer_id order_date;
  retain visit_count 0;
  if first.customer_id then visit_count=1;
  else visit_count+1;
  order_sequence = visit_count;
run;

proc stdize data=work.orders_open out=work.orders_imputed reponly;
  var amount revenue;
  repvalue=median;
run;
