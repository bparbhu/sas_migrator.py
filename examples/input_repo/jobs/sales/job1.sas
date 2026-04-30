libname dw oracle user=my_user password=my_password schema=analytics;
%include "../../macros/common.sas";
%load_sales(input=dw.sales, output=sales_local);

proc sort data=sales_local out=sales_sorted;
  by customer_id descending order_date;
run;

proc freq data=sales_sorted;
  tables customer_id*region / out=sales_freq;
run;
