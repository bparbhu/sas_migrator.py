data work.customer_final;
  merge work.customer_rollup(in=a) work.customers(in=b);
  by customer_id;
  if a and b;
run;
