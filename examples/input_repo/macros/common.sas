%let region_filter = 'East';

%macro load_sales(input=dw.sales, output=sales_local);
%if &region_filter = 'East' %then %do;
data &output;
  set &input(keep=customer_id order_date amount region where=(region = &region_filter));
  revenue = amount;
run;
%end;
%else %do;
data &output;
  set &input(keep=customer_id order_date amount region);
  revenue = amount;
run;
%end;
%mend;
