



libname dw oracle user=my_user password=my_password schema=analytics;
%include "../../macros/common.sas";

data sales_local;
  set dw.sales(keep=customer_id order_date amount region);
  revenue = amount;
run;


proc sort data=sales_local out=sales_sorted;
  by customer_id descending order_date;
run;

proc freq data=sales_sorted;
  tables customer_id*region / out=sales_freq;
run;
