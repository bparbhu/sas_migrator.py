data work.daily;
  set staging.orders(keep=id qty price status where=(status = 'OPEN'));
  total = qty * price;
run;

proc means data=work.daily noprint;
  class status;
  var total;
  output out=work.daily_summary mean= sum=;
run;
