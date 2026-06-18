/* ---- 1. Read the two CSVs into SAS ---- */
proc import datafile="/home/u64522162/lc_train_raw.csv" out=work.train dbms=csv replace;
   guessingrows=max;
run;
proc import datafile="/home/u64522162/lc_test_raw.csv" out=work.test dbms=csv replace;
   guessingrows=max;
run;

/* ---- 2. Define the WoE recipe once (used 7 times below) ---- */
%macro woe(var=, type=num);
  %if &type = num %then %do;                 /* continuous -> 5 equal-size groups */
     proc rank data=work.train out=work.train groups=5; var &var; ranks &var._bin; run;
     proc rank data=work.test  out=work.test  groups=5; var &var; ranks &var._bin; run;
  %end;
  %else %do;                                 /* category -> value is its own group */
     data work.train; set work.train; &var._bin = &var; run;
     data work.test;  set work.test;  &var._bin = &var; run;
  %end;

  proc sql;                                  /* count goods/bads per group, TRAIN only */
     create table work._w as
     select &var._bin as bin,
            sum(case when is_bad=0 then 1 else 0 end) as good,
            sum(case when is_bad=1 then 1 else 0 end) as bad
     from work.train group by &var._bin;
  quit;
  proc sql noprint; select sum(good), sum(bad) into :tg, :tb from work._w; quit;
  data work._w;                              /* counts -> Weight of Evidence */
     set work._w;
     woe_&var = log( ((good+0.5)/&tg) / ((bad+0.5)/&tb) );
  run;
  proc sql;                                  /* attach the WoE column to both sets */
     create table work.train as select a.*, b.woe_&var
        from work.train a left join work._w b on a.&var._bin = b.bin;
     create table work.test as select a.*, b.woe_&var
        from work.test a left join work._w b on a.&var._bin = b.bin;
  quit;
%mend;

/* ---- 3. Build WoE for the 7 model features ---- */
%woe(var=annual_inc,          type=num);
%woe(var=dti,                 type=num);
%woe(var=grade_num,           type=cat);
%woe(var=term,                type=cat);
%woe(var=home_ownership,      type=cat);
%woe(var=purpose,             type=cat);
%woe(var=verification_status, type=cat);

/* ---- 4. Fit the scorecard model on train, score the test set ---- */
proc logistic data=work.train;
   model is_bad(event='1') =
         woe_annual_inc woe_dti woe_grade_num woe_term;
   score data=work.test out=work.scored;
run;

/* ---- 5. THE HEADLINE NUMBER: test AUC ---- */
proc logistic data=work.scored descending;
   model is_bad = P_1;
   ods select Association;
run;

/* ---- 6. Scale to a score and rank-order check by grade ---- */
data work.scored; set work.scored;
   score = 487.12 - 28.85*log(P_1/(1-P_1));   /* higher score = safer */
run;
proc means data=work.scored mean maxdec=4;
   class grade; var P_1 score;
run;

/* ---- 7. KS statistic ---- */
proc npar1way edf data=work.scored;
   class is_bad; var score;
run;
