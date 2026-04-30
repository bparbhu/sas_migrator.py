data work.merged;
  merge left_ds(in=ina) right_ds(in=inb);
  by id;
  if ina and inb;
run;
