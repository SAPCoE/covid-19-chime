/***
Based on Python code from https://code-for-philly.gitbook.io/chime/ as of April 1, 2020
Python code converted to SAP HANA SQLScript
Author: Dimitrios Lyras, SAP
Contacts: Dimitrios Lyras(dimitrios.lyras@sap.com), Qi Su (qi.su@sap.com)

4/1/2020 version changes:
- significant changes in the logic
- order of input parameters changed
- new input parameters: cur_date, mitigation_date, date_first_hospitalized, infectious_days, i_day
- removed parameters: recovery_days, start_date 
- ouput schema remains the same
- input parameter default values changed: hospitalized_length_of_stay, icu_length_of_stay, ventilated_length_of_stay, current_hospitalized


**/


------------------------
-- functions and procedures shared in common by all 3 approaches
------------------------

CREATE SCHEMA COVID19;
SET SCHEMA COVID19;

DROP FUNCTION "COVID19"."CHIME_GET_BETA";
CREATE FUNCTION "COVID19"."CHIME_GET_BETA" ( 
	IN intrinsic_growth_rate DOUBLE,
    IN gamma DOUBLE,
    IN susceptible DOUBLE,
    IN relative_contact_rate DOUBLE)
	RETURNS beta DOUBLE
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN
	beta := ( (:intrinsic_growth_rate + :gamma)  / :susceptible * (1.0 - :relative_contact_rate) );
END;

DROP FUNCTION "COVID19"."CHIME_GET_GROWTH_RATE";
CREATE FUNCTION "COVID19"."CHIME_GET_GROWTH_RATE" ( IN doubling_time DOUBLE ) 
	RETURNS growth_rate DOUBLE
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN
	IF :doubling_time IS NULL or :doubling_time = 0.0 THEN
		growth_rate := 0.0;
	ELSE
		growth_rate := (POWER(2.0, (1.0 / :doubling_time)) - 1.0);
	END IF;
END;

DROP FUNCTION "COVID19"."CHIME_GEN_SIR";
CREATE FUNCTION "COVID19"."CHIME_GEN_SIR" ( 
		IN s 	 DOUBLE,
		IN i 	 DOUBLE,
		IN r 	 DOUBLE,
		IN gamma DOUBLE,
		IN i_day INTEGER,
		IN policies TABLE (
	   	    "beta" 		DOUBLE,
			"n_days" 	INTEGER
		) 
	)
	RETURNS TABLE (
   	    "ID" 					INTEGER,
		"SIR_SUSCEPTIBLE"		DOUBLE,
		"SIR_INFECTED" 			DOUBLE,
		"SIR_RECOVERED"			DOUBLE
	)
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN
	DECLARE n 		DOUBLE 	:= (:s + :i + :r);
	DECLARE d 		INTEGER := :i_day;
	DECLARE iter 	INTEGER := 0;
	
	DECLARE s_n, i_n, r_n, scale	DOUBLE :=0;
	-- The SIR Model results table
	DECLARE sir_tbl TABLE (
   	    "ID" 				INTEGER,
		"SIR_SUSCEPTIBLE"	DOUBLE,
		"SIR_INFECTED" 		DOUBLE,
		"SIR_RECOVERED"		DOUBLE
	);
	DECLARE CURSOR curs FOR
		SELECT "beta", "n_days"
		FROM :policies
	;
	
	
	/*Simulate SIR model forward in time yielding tuples.
	Parameter order has changed to allow multiple (beta, n_days)
	to reflect multiple changing social distancing policies.*/
	s := TO_DOUBLE(:s);
	i := TO_DOUBLE(:i);
	r := TO_DOUBLE(:r);
	n := (:s + :i + :r);
	d := i_day;
	
	:sir_tbl.INSERT((:d, :s, :i, :r ) );
	d := :d + 1;
	FOR c_row AS curs DO
	    FOR iter IN 1 .. (:c_row."n_days") DO			
			s_n := (-:c_row."beta" * :s * :i) + :s;
		    i_n := (:c_row."beta" * :s * :i - :gamma * :i) + :i;
		    r_n := :gamma * :i + :r;		    
		    scale := :n / (:s_n + :i_n + :r_n);
		    
		    s := :s_n;
		    i := :i_n;
		    r := :r_n;
		    
		    :sir_tbl.INSERT((:d, (:s_n * :scale), (:i_n * :scale), (:r_n * :scale) ) );
		    
		    d := :d + 1;
		END FOR;
	END FOR;
	
	RETURN SELECT * FROM :sir_tbl;
END;


DROP FUNCTION "COVID19"."CHIME_BUILD_DISPOSITIONS";
CREATE FUNCTION "COVID19"."CHIME_BUILD_DISPOSITIONS" ( 
		IN sir_tbl TABLE (
	   	    "ID" 					INTEGER,
			"SIR_SUSCEPTIBLE"		DOUBLE,
			"SIR_INFECTED" 			DOUBLE,
			"SIR_RECOVERED"			DOUBLE
		),
		IN hospitalized_rate 	DOUBLE,
		IN icu_rate			 	DOUBLE,
		IN ventilated_rate	 	DOUBLE,
		IN market_share		 	DOUBLE,
		IN cur_date		 		DATE
	) 
	RETURNS TABLE (
   	    "ID" 					INTEGER,
   	    "DATE" 					DATE,
		"DISP_HOSPITALIZED"		DOUBLE,
		"DISP_ICU" 				DOUBLE,
		"DISP_VENTILATED"		DOUBLE
	)
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN
	IF :cur_date IS NULL THEN cur_date := TO_DATE(NOW()); END IF;	
	RETURN 	SELECT 	"ID", ADD_DAYS(TO_DATE(NOW()), "ID") AS "DATE", 
					("SIR_INFECTED" + "SIR_RECOVERED") * :hospitalized_rate * :market_share AS "DISP_HOSPITALIZED",
					("SIR_INFECTED" + "SIR_RECOVERED") * :icu_rate          * :market_share AS "DISP_ICU",
					("SIR_INFECTED" + "SIR_RECOVERED") * :ventilated_rate   * :market_share AS "DISP_VENTILATED"
			FROM :sir_tbl
			;	
END;

DROP FUNCTION "COVID19"."CHIME_BUILD_CENSUS";
CREATE FUNCTION "COVID19"."CHIME_BUILD_CENSUS" ( 
		IN admissions_tbl TABLE (
	   	    "ID" 					INTEGER,
	   	    "DATE" 					DATE,
			"ADM_HOSPITALIZED"		DOUBLE,
			"ADM_ICU" 				DOUBLE,
			"ADM_VENTILATED"		DOUBLE
		),
		IN hospitalized_length_of_stay 	INTEGER,
		IN icu_length_of_stay			INTEGER,
		IN ventilated_length_of_stay	INTEGER
	) 
	RETURNS TABLE (
   	    "ID" 					INTEGER,
   	    "DATE" 					DATE,
		"CENS_HOSPITALIZED"		DOUBLE,
		"CENS_ICU" 				DOUBLE,
		"CENS_VENTILATED"		DOUBLE
	)
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN

	-- Compute Cencus
	census_tbl_tmp 	=	SELECT 	"ID", "DATE", 
								SUM("ADM_HOSPITALIZED") OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_HOSPITALIZED_SUM",
								SUM("ADM_ICU") 			OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_ICU_SUM",
								SUM("ADM_VENTILATED") 	OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_VENTILATED_SUM"
						FROM :admissions_tbl
						;
	--SELECT * FROM :census_tbl_tmp;
	
	RETURN	SELECT 	"ID", "DATE", 
					("CENS_HOSPITALIZED_SUM"- IFNULL(LAG("CENS_HOSPITALIZED_SUM", 	:hospitalized_length_of_stay) 	OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_HOSPITALIZED",
					("CENS_ICU_SUM" 		- IFNULL(LAG("CENS_ICU_SUM", 			:icu_length_of_stay) 			OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_ICU",
					("CENS_VENTILATED_SUM" 	- IFNULL(LAG("CENS_VENTILATED_SUM", 	:ventilated_length_of_stay) 	OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_VENTILATED"
			FROM :census_tbl_tmp
			;
END;

DROP FUNCTION "COVID19"."CHIME_BUILD_ADMITS";
CREATE FUNCTION "COVID19"."CHIME_BUILD_ADMITS" ( 
		IN dispositions_tbl TABLE (
	   	    "ID" 					INTEGER,
	   	    "DATE" 					DATE,
			"DISP_HOSPITALIZED"		DOUBLE,
			"DISP_ICU" 				DOUBLE,
			"DISP_VENTILATED"		DOUBLE
		)
	) 
	RETURNS TABLE (
   	    "ID" 					INTEGER,
   	    "DATE" 					DATE,
		"ADM_HOSPITALIZED"		DOUBLE,
		"ADM_ICU" 				DOUBLE,
		"ADM_VENTILATED"		DOUBLE
	)
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN
	
	-- Compute Admissions
	RETURN	SELECT 	"ID", "DATE", 
					"DISP_HOSPITALIZED" - LAG("DISP_HOSPITALIZED", 1) OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_HOSPITALIZED",
					"DISP_ICU" 		    - LAG("DISP_ICU", 1) 		  OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_ICU",
					"DISP_VENTILATED"   - LAG("DISP_VENTILATED", 1)   OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_VENTILATED"
			FROM :dispositions_tbl
			;
END;

DROP PROCEDURE "COVID19"."CHIME_RUN_PROJECTION";
CREATE PROCEDURE "COVID19"."CHIME_RUN_PROJECTION" ( 
		IN susceptible 	 DOUBLE,
		IN infected 	 DOUBLE,
		IN recovered 	 DOUBLE,
		IN gamma 		 DOUBLE,
		IN i_day 		 INTEGER,
		IN policies 	 TABLE (
	   	    "beta" 		 DOUBLE,
			"n_days" 	 INTEGER
		), 
		
		IN hospitalized_rate 	DOUBLE,
		IN icu_rate			 	DOUBLE,
		IN ventilated_rate	 	DOUBLE,
		IN market_share		 	DOUBLE,
		IN cur_date		 		DATE,
		
		IN hospitalized_length_of_stay 	INTEGER,
		IN icu_length_of_stay			INTEGER,
		IN ventilated_length_of_stay	INTEGER,
		
		OUT sir_tbl TABLE (
	   	    "ID" 					INTEGER,
			"SIR_SUSCEPTIBLE"		DOUBLE,
			"SIR_INFECTED" 			DOUBLE,
			"SIR_RECOVERED"			DOUBLE
		),
		
		OUT dispositions_tbl TABLE (
	   	    "ID" 					INTEGER,
	   	    "DATE" 					DATE,
			"DISP_HOSPITALIZED"		DOUBLE,
			"DISP_ICU" 				DOUBLE,
			"DISP_VENTILATED"		DOUBLE
		),
		OUT admissions_tbl TABLE (
	   	    "ID" 					INTEGER,
	   	    "DATE" 					DATE,
			"ADM_HOSPITALIZED"		DOUBLE,
			"ADM_ICU" 				DOUBLE,
			"ADM_VENTILATED"		DOUBLE
		),
		OUT census_tbl TABLE (
	   	    "ID" 					INTEGER,
	   	    "DATE" 					DATE,
			"CENS_HOSPITALIZED"		DOUBLE,
			"CENS_ICU" 				DOUBLE,
			"CENS_VENTILATED"		DOUBLE
		),
		OUT current_infected 		DOUBLE
	) 
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER 
	DEFAULT SCHEMA "COVID19"
	READS SQL DATA AS
BEGIN
	sir_tbl  		 = SELECT * FROM "COVID19"."CHIME_GEN_SIR"(:susceptible, :infected, :recovered, :gamma, (-1) * :i_day, :policies);
	dispositions_tbl = SELECT * FROM "COVID19"."CHIME_BUILD_DISPOSITIONS"(:sir_tbl, :hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date);
	admissions_tbl 	 = SELECT * FROM "COVID19"."CHIME_BUILD_ADMITS"(:dispositions_tbl);
	census_tbl		 = SELECT * FROM "COVID19"."CHIME_BUILD_CENSUS"(:admissions_tbl, :hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay);
	
	--SELECT "SIR_INFECTED" INTO current_infected FROM :sir_tbl WHERE "ID" = :i_day;
	SELECT "SIR_INFECTED" INTO current_infected FROM :sir_tbl WHERE "ID" = 0;
END;

DROP FUNCTION "COVID19"."CHIME_GEN_POLICY";
CREATE FUNCTION "COVID19"."CHIME_GEN_POLICY" ( 
		IN mitigation_date 	DATE,
		IN i_day			INTEGER,
		IN n_days			INTEGER,
		IN beta				DOUBLE,
		IN beta_t			DOUBLE
	) 
	RETURNS TABLE (
   	    "beta" 		DOUBLE,
		"n_days" 	INTEGER
	)
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN
	DECLARE mitigation_day 											INTEGER := 0;
	DECLARE total_days, pre_mitigation_days, post_mitigation_days	INTEGER := 0;
	
	IF :mitigation_date IS NOT NULL THEN
		mitigation_day := -DAYS_BETWEEN (TO_DATE(NOW()), :mitigation_date);
	ELSE
		mitigation_day := 0;
	END IF;
	
	total_days := :i_day + :n_days;
	
	IF :mitigation_day < (-1 * :i_day) THEN mitigation_day := (-1 * :i_day); END IF;
	pre_mitigation_days 	:= :i_day + :mitigation_day;
    post_mitigation_days 	:= :total_days - :pre_mitigation_days;
    
    RETURN 	SELECT 	:beta 					AS "beta",
    				:pre_mitigation_days 	AS "n_days"
    		FROM DUMMY
    		UNION ALL
    		SELECT 	:beta_t 			 	AS "beta",
					:post_mitigation_days 	AS "n_days"
			FROM DUMMY;
    		
END;

DROP FUNCTION "COVID19"."CHIME_GET_LOSS";
CREATE FUNCTION "COVID19"."CHIME_GET_LOSS" ( 
		IN census_tbl TABLE (
	   	    "ID" 					INTEGER,
	   	    "DATE" 					DATE,
			"CENS_HOSPITALIZED"		DOUBLE,
			"CENS_ICU" 				DOUBLE,
			"CENS_VENTILATED"		DOUBLE
		),
		IN current_hospitalized		DOUBLE,
		IN i_day					INTEGER
	)
	RETURNS loss DOUBLE
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN
	DECLARE predicted	DOUBLE := 0;	
	SELECT "CENS_HOSPITALIZED" INTO predicted
	FROM :census_tbl
	WHERE "ID" + ABS(:i_day) = :i_day
	;
	loss := POWER( (:current_hospitalized - :predicted), 2 );
	--loss := :predicted;
END;


DROP PROCEDURE "COVID19"."CHIME_GET_ARGMIN_DOUBLING_TIME";
CREATE PROCEDURE "COVID19"."CHIME_GET_ARGMIN_DOUBLING_TIME" ( 
	    IN start_num 	 			DOUBLE,
	    IN stop_num					DOUBLE,
	    IN num_steps 	 			INTEGER,
	    
		IN susceptible 	 			DOUBLE,
		IN infected 	 			DOUBLE,
		IN recovered 	 			DOUBLE,
		IN gamma 		 			DOUBLE,
		IN relative_contact_rate	DOUBLE, 
		IN mitigation_date			DATE,
		IN i_day 		 			INTEGER,
		IN n_days		 			INTEGER,
		
		IN hospitalized_rate 		DOUBLE,
		IN icu_rate			 		DOUBLE,
		IN ventilated_rate	 		DOUBLE,
		IN market_share		 		DOUBLE,
		IN cur_date		 			DATE,
		
		IN hospitalized_length_of_stay 	INTEGER,
		IN icu_length_of_stay			INTEGER,
		IN ventilated_length_of_stay	INTEGER,
		IN current_hospitalized			INTEGER,
		
		OUT min_loss_idx 				INTEGER,
		OUT dts 						DOUBLE ARRAY
	) 
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER 
	--DEFAULT SCHEMA <default_schema_name>
	READS SQL DATA AS
BEGIN
	DECLARE iter					INTEGER := 0;
	DECLARE intrinsic_growth_rate	DOUBLE  := 0;
	DECLARE beta					DOUBLE  := 0;
	DECLARE beta_t					DOUBLE  := 0;
	
	DECLARE peak_admits_day			INTEGER := 0;
	DECLARE loss					DOUBLE  := 0;
	DECLARE min_loss				DOUBLE  := 1.7976931348623157E308; -- biggest double value
	--DECLARE min_loss_idx			INTEGER := 0;
	DECLARE current_infected		DOUBLE  := 0;
	DECLARE dts_value				DOUBLE  := :start_num;
	--DECLARE dts 					DOUBLE ARRAY;
	
	-- Make an initial coarse estimate
	FOR iter IN 1 .. (:num_steps) DO	
		dts[:iter] := :dts_value;		
		intrinsic_growth_rate := "COVID19"."CHIME_GET_GROWTH_RATE"(:dts_value);
		beta 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, 0.0);
    	beta_t 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, :relative_contact_rate);
   		
   		policies = SELECT * FROM "COVID19"."CHIME_GEN_POLICY" ( :mitigation_date, :i_day, :n_days, :beta, :beta_t); 
	    CALL "COVID19"."CHIME_RUN_PROJECTION"(
	    	:susceptible, :infected, :recovered, :gamma, :i_day, :policies,
	    	:hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date,
	    	:hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay,
	    	
	    	sir_tbl, dispositions_tbl, admissions_tbl, census_tbl, :current_infected
	    );
	    
	    SELECT (ROW_NUMBER() OVER())-1 INTO peak_admits_day
	    FROM :admissions_tbl AS a
	    LEFT JOIN (
	    	SELECT MAX("ADM_HOSPITALIZED") AS "MAX" 
	    	FROM :admissions_tbl
	    ) AS b ON a."ADM_HOSPITALIZED" = b."MAX"
	    ORDER BY "ADM_HOSPITALIZED" DESC
	    LIMIT 1
	    ;
	    
	    IF :peak_admits_day >= 0 THEN
	    	loss := "COVID19"."CHIME_GET_LOSS"(:census_tbl, :current_hospitalized, :i_day);    	
	    	IF :loss < :min_loss THEN
	    		min_loss := :loss;
	    		min_loss_idx := :iter-1;
	    	END IF;
	    END IF;
	    
	    IF :num_steps <> 1 THEN
	    	dts_value := :dts_value + ((:stop_num - :start_num)/(:num_steps-1));
	    ELSE 
	    	dts_value := :dts_value + ((:stop_num - :start_num)/(:num_steps));
	    END IF;
	END FOR;
END;



------------------------
-- Approach 1. SAP HANA SQLScript store procedure script with hard coded variable values
-- output: in addition to the SELECT statements outputing data, this script also drops then creates and populates two tables: "COVID19"."CHIME_RESULTS" and "COVID19"."CHIME_RESULTS_NON_ROUNDED"
------------------------

SET SCHEMA COVID19;

DO BEGIN
	-- Parameters Setup
	DECLARE population 				DOUBLE  := 3600000;
	DECLARE hospitalized_rate		DOUBLE  := 2.5/100;  --2.5%
	DECLARE icu_rate				DOUBLE  := 0.75/100; --0.75%
	DECLARE ventilated_rate			DOUBLE  := 0.5/100;  --0.5%
	
	DECLARE hospitalized_length_of_stay	INTEGER := 9;--7;
	DECLARE icu_length_of_stay			INTEGER := 10;--9;
	DECLARE ventilated_length_of_stay	INTEGER := 11;--10;
	
	DECLARE current_hospitalized	INTEGER := 69;
	DECLARE relative_contact_rate	DOUBLE  := 30/100; --#30% social distancing

	DECLARE cur_date				DATE	:= TO_DATE(NOW()); -- Formerly used as start_date
  DECLARE mitigation_date			DATE 	:= NULL;
  DECLARE date_first_hospitalized DATE    := NULL; --'2020-03-18';--NULL; --'2020-03-25'; --NULL
    
	DECLARE doubling_time 			DOUBLE  := 4; --4; --NULL; --4
	DECLARE infectious_days			DOUBLE  := 14; --Formerly used as recovery_days and was set by default to 14 based on CDC is recommending 14 days of self-quarantine - see https://code-for-philly.gitbook.io/chime/what-is-chime/sir-modeling
	DECLARE market_share			DOUBLE  := 15/100; --15%
	
	DECLARE	n_days					INTEGER := 100; -- days to run forward simulation
	
	DECLARE i_day					INTEGER := 0;
	
	-- Assisting Variables for CHIME Model Computations
	DECLARE infected				DOUBLE  := 0;
	DECLARE susceptible				DOUBLE  := 0;
	DECLARE recovered				DOUBLE  := 0;
	DECLARE intrinsic_growth_rate	DOUBLE  := 0;
	DECLARE gamma					DOUBLE  := 0;
	DECLARE beta					DOUBLE  := 0;
	DECLARE beta_t					DOUBLE  := 0;
	
	DECLARE current_infected		DOUBLE  := 0;
	DECLARE iter					INTEGER := 0;
	DECLARE min_loss_idx			INTEGER := 0;
	DECLARE dts 					DOUBLE ARRAY;
	DECLARE r_t						DOUBLE := 0.0;
	DECLARE r_naught				DOUBLE := 0.0;
	DECLARE doubling_time_t 		DOUBLE  := NULL;
	DECLARE daily_growth_rate, daily_growth_rate_t DOUBLE := 0.0;
	DECLARE days_bug_fix			INTEGER := 3;
	
	DECLARE temp_policy				TABLE (
   	    "beta" 		DOUBLE,
		"n_days" 	INTEGER
	);
	
	-- Basic SIR Model Computations
	recovered		:= 0;
	infected 		:= (1.0 / :market_share / :hospitalized_rate);
	susceptible 	:= :population - :infected;
	gamma 			:= 1.0/ :infectious_days;
	
	IF :date_first_hospitalized IS NOT NULL THEN doubling_time := NULL; END IF;
	
	-- Bug Fix: In the app for some reason ithe current day is alwyas assumed to be 3 days before
	cur_date := ADD_DAYS(:cur_date, (-1 *:days_bug_fix));
	
	IF :date_first_hospitalized IS NULL AND (:doubling_time IS NOT NULL AND :doubling_time <> 0.0) THEN
		days_bug_fix := 0; -- For some reason in the app 3 more days are subtracted when this case is selected
		
		-- Back-projecting to when the first hospitalized case would have been admitted
		intrinsic_growth_rate := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
		beta 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate,  :gamma, :susceptible, 0.0);
    	beta_t 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, :relative_contact_rate);
   		
   		i_day   := 0; --seed to the full length
		
	    :temp_policy.INSERT( (:beta, :n_days ) );
	    
	    CALL "COVID19"."CHIME_RUN_PROJECTION"(
	    	:susceptible, :infected, :recovered, :gamma, :i_day, :temp_policy,
	    	:hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date,
	    	:hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay,
	    	
	    	sir_tbl, dispositions_tbl, admissions_tbl, census_tbl, :current_infected
	    );
	    
		
		-- The code equivalent for computing the get_argmin_ds part
		SELECT TO_INTEGER("ID") INTO i_day 
		FROM (
			SELECT a."ID", POWER( (a."CENS_HOSPITALIZED" - :current_hospitalized), 2) AS "POW"
			FROM :census_tbl AS a
			INNER JOIN (
				SELECT a."ID" AS "ID"
				FROM :census_tbl AS a
				INNER JOIN (
					SELECT MAX("CENS_HOSPITALIZED") AS "MAX" FROM :census_tbl
				)AS b ON a."CENS_HOSPITALIZED" = b."MAX"
				ORDER BY "ID" ASC
			) AS b ON a."ID" < b."ID"
		)
		WHERE "POW" IS NOT NULL
		ORDER BY "POW" ASC
		LIMIT 1
		;
		
	    policies = SELECT * FROM "COVID19"."CHIME_GEN_POLICY" ( :mitigation_date, :i_day, :n_days, :beta, :beta_t); 
	    CALL "COVID19"."CHIME_RUN_PROJECTION"(
	    	:susceptible, :infected, :recovered, :gamma, :i_day, :policies,
	    	:hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date,
	    	:hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay,
	    	
	    	sir_tbl, dispositions_tbl, admissions_tbl, census_tbl, :current_infected
	    );
	    date_first_hospitalized := ADD_DAYS(:cur_date, (-1*:i_day));
	ELSEIF :date_first_hospitalized IS NOT NULL AND :doubling_time IS NULL  THEN 
		-- Fitting spread parameter to observed hospital census (dates of 1 patient and today)
		i_day := DAYS_BETWEEN(:date_first_hospitalized, :cur_date);
		
		-- Make an initial coarse estimate
		CALL "COVID19"."CHIME_GET_ARGMIN_DOUBLING_TIME" (1, 15, 15, :susceptible, :infected, :recovered, :gamma, :relative_contact_rate, :mitigation_date, :i_day, :n_days, :hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date, :hospitalized_length_of_stay, :icu_length_of_stay,  :ventilated_length_of_stay, :current_hospitalized, :min_loss_idx, :dts);
	
		-- Refine the coarse estimate
		FOR iter IN 1 .. (4) DO	
			CALL "COVID19"."CHIME_GET_ARGMIN_DOUBLING_TIME" (:dts[:min_loss_idx-1 +1], :dts[:min_loss_idx +1], 15, :susceptible, :infected, :recovered, :gamma, :relative_contact_rate, :mitigation_date, :i_day, :n_days, :hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date, :hospitalized_length_of_stay, :icu_length_of_stay,  :ventilated_length_of_stay, :current_hospitalized, :min_loss_idx, :dts);
			--SELECT :min_loss_idx AS "min_loss_idx" FROM DUMMY;
			--SELECT :dts[:min_loss_idx+1] AS "doubling_time in for", :min_loss_idx AS "min_loss_idx" FROM DUMMY;
			--res_check = UNNEST(:dts);
			--SELECT * FROM :res_check;
		END FOR;
		
		doubling_time := :dts[:min_loss_idx +1];
		--SELECT :dts[:min_loss_idx +1];AS "doubling_time", :min_loss_idx AS "min_loss_idx" FROM DUMMY;
		
		intrinsic_growth_rate := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
		beta 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, 0.0);
    	beta_t 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, :relative_contact_rate);
   		
   		policies = SELECT * FROM "COVID19"."CHIME_GEN_POLICY" ( :mitigation_date, :i_day, :n_days, :beta, :beta_t); 
	    CALL "COVID19"."CHIME_RUN_PROJECTION"(
	    	:susceptible, :infected, :recovered, :gamma, :i_day, :policies,
	    	:hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date,
	    	:hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay,
	    	
	    	sir_tbl, dispositions_tbl, admissions_tbl, census_tbl, :current_infected
	    );
	END IF;

	intrinsic_growth_rate := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
	beta 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, 0.0);
    beta_t 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, :relative_contact_rate);
	
	-- r_t is r_0 after distancing
	r_t 	 := :beta_t / :gamma * :susceptible;
	r_naught := :beta   / :gamma * :susceptible;
	
	doubling_time_t := LOG( (:beta_t * :susceptible - :gamma + 1), 2);
	
	daily_growth_rate   := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
	daily_growth_rate_t := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time_t);
		
	SELECT "SIR_SUSCEPTIBLE" INTO susceptible	FROM :sir_tbl WHERE "ID" = 0;
	SELECT "SIR_INFECTED" 	 INTO infected		FROM :sir_tbl WHERE "ID" = 0;
	SELECT "SIR_RECOVERED" 	 INTO recovered		FROM :sir_tbl WHERE "ID" = 0;
	
	chime_result = 	SELECT 	a."ID", --b."DATE", -- there is some inconsistency in the results to the ID and the cur_date
							ADD_DAYS(TO_DATE(:cur_date), a."ID") AS "DATE", 
							a."SIR_SUSCEPTIBLE", a."SIR_INFECTED", a."SIR_RECOVERED", 
							FLOOR(b."DISP_HOSPITALIZED") AS "DISP_HOSPITALIZED", FLOOR(b."DISP_ICU") AS "DISP_ICU", FLOOR(b."DISP_VENTILATED") AS "DISP_VENTILATED",
							FLOOR(c."ADM_HOSPITALIZED")  AS "ADM_HOSPITALIZED",  FLOOR(c."ADM_ICU")  AS "ADM_ICU",  FLOOR(c."ADM_VENTILATED")  AS "ADM_VENTILATED",
							FLOOR(d."CENS_HOSPITALIZED") AS "CENS_HOSPITALIZED", FLOOR(d."CENS_ICU") AS "CENS_ICU", FLOOR(d."CENS_VENTILATED") AS "CENS_VENTILATED"
					FROM :sir_tbl AS a
					LEFT JOIN :dispositions_tbl AS b ON a."ID" = b."ID"
					LEFT JOIN :admissions_tbl 	AS c ON a."ID" = c."ID"
					LEFT JOIN :census_tbl 		AS d ON a."ID" = d."ID"
	;
	SELECT * FROM :chime_result;
	if exists ( select * from tables where schema_name='COVID19' and table_name='CHIME_RESULTS' ) then
		DROP TABLE "COVID19"."CHIME_RESULTS";
	end if;	

	CREATE COLUMN TABLE "COVID19"."CHIME_RESULTS" AS (SELECT * FROM :chime_result);
	
	chime_result_non_rounded = 	SELECT 	a."ID", --b."DATE", -- there is some inconsistency in the results to the ID and the cur_date
										ADD_DAYS(TO_DATE(:cur_date), a."ID") AS "DATE", 
										a."SIR_SUSCEPTIBLE", a."SIR_INFECTED", a."SIR_RECOVERED", 
										"DISP_HOSPITALIZED", "DISP_ICU", "DISP_VENTILATED",
										"ADM_HOSPITALIZED",  "ADM_ICU",  "ADM_VENTILATED",
										"CENS_HOSPITALIZED", "CENS_ICU", "CENS_VENTILATED"
								FROM :sir_tbl AS a
								LEFT JOIN :dispositions_tbl AS b ON a."ID" = b."ID"
								LEFT JOIN :admissions_tbl 	AS c ON a."ID" = c."ID"
								LEFT JOIN :census_tbl 		AS d ON a."ID" = d."ID"
				;
	SELECT * FROM :chime_result_non_rounded;
	if exists ( select * from tables where schema_name='COVID19' and table_name='CHIME_RESULTS_NON_ROUNDED' ) then
		DROP TABLE "COVID19"."CHIME_RESULTS_NON_ROUNDED";
	end if;	
	CREATE COLUMN TABLE "COVID19"."CHIME_RESULTS_NON_ROUNDED" AS (SELECT * FROM :chime_result_non_rounded);
	
	additional_stats=
			SELECT 	ROUND(:susceptible, 6) AS "susceptible",
					ROUND(:infected,6) AS "infected",
					ROUND(:recovered,6) AS "recovered",
					ROUND(:intrinsic_growth_rate,6) AS "intrinsic_growth_rate",
					ROUND(:gamma,6) AS "gamma",
					ROUND(:beta,6) AS "beta",
					ROUND(:beta_t,6) AS "beta_t",
					ROUND(:r_t,6) AS "r_t",
					ROUND(:r_naught,6) AS "r_naught",
					ROUND(:doubling_time,6) AS "doubling_time",
					ROUND(:doubling_time_t,6) AS "doubling_time_t",
					ROUND(:daily_growth_rate,6) AS "daily_growth_rate",
					ROUND(:daily_growth_rate_t,6) AS "daily_growth_rate_t"
			FROM DUMMY; 
	SELECT * FROM :additional_stats;
	
	--SELECT * FROM :sir_tbl;
	--SELECT * FROM :dispositions_tbl;
	--SELECT * FROM :admissions_tbl;
	--SELECT * FROM :census_tbl;
	--SELECT :current_infected AS "current_infected" 
	--FROM DUMMY;
END;





------------------------
-- Approach 2. SAP HANA user defined table function with CHIME parameters as scalar function input parameters
--
-- If you try to execute the entire script for approach 2 in one shot and encounter error message:
--    Could not execute 'CREATE FUNCTION COVID19.CHIME_TABLE_UDF_SCALAR_INPUT ( population DOUBLE, hospitalized_rate DOUBLE, ...'
--    SAP DBTech JDBC: [257]: sql syntax error: incorrect syntax near "SELECT": line 198 col 1 (at pos 9111)
-- The solution is to execute the CREATE FUNCTION statement first, then execute the SELECT * FROM COVID19.CHIME_TABLE_UDF_SCALAR_INPUT( ...) statement separately
------------------------

SET SCHEMA COVID19;

DROP FUNCTION COVID19.CHIME_TABLE_UDF_SCALAR_INPUT;
CREATE FUNCTION COVID19.CHIME_TABLE_UDF_SCALAR_INPUT (
  population DOUBLE,
  hospitalized_rate DOUBLE,
  icu_rate DOUBLE,
  ventilated_rate DOUBLE,
  hospitalized_length_of_stay INTEGER,
  icu_length_of_stay INTEGER,
  ventilated_length_of_stay INTEGER,
  current_hospitalized INTEGER,
  relative_contact_rate DOUBLE,  
  cur_date DATE,
  mitigation_date DATE,
  date_first_hospitalized DATE,  
  doubling_time DOUBLE,
  infectious_days			DOUBLE,
  market_share DOUBLE,  
  n_days INTEGER,
  i_day INTEGER  
) RETURNS TABLE (
  "ID" 				INTEGER,
	"DATE"   			DATE,
	"SIR_SUSCEPTIBLE"	DOUBLE,
	"SIR_INFECTED" 		DOUBLE,
	"SIR_RECOVERED"		DOUBLE,
	"DISP_HOSPITALIZED" DOUBLE, 
	"DISP_ICU" DOUBLE, 
	"DISP_VENTILATED" DOUBLE,
	"ADM_HOSPITALIZED" DOUBLE,
	"ADM_ICU" DOUBLE,
	"ADM_VENTILATED" DOUBLE,
	"CENS_HOSPITALIZED" DOUBLE,
	"CENS_ICU" DOUBLE,
	"CENS_VENTILATED" DOUBLE			
) AS
BEGIN		
	-- Assisting Variables for CHIME Model Computations
	DECLARE infected				DOUBLE  := 0;
	DECLARE susceptible				DOUBLE  := 0;
	DECLARE recovered				DOUBLE  := 0;
	DECLARE intrinsic_growth_rate	DOUBLE  := 0;
	DECLARE gamma					DOUBLE  := 0;
	DECLARE beta					DOUBLE  := 0;
	DECLARE beta_t					DOUBLE  := 0;
	
	DECLARE current_infected		DOUBLE  := 0;
	DECLARE iter					INTEGER := 0;
	DECLARE min_loss_idx			INTEGER := 0;
	DECLARE dts 					DOUBLE ARRAY;
	DECLARE r_t						DOUBLE := 0.0;
	DECLARE r_naught				DOUBLE := 0.0;
	DECLARE doubling_time_t 		DOUBLE  := NULL;
	DECLARE daily_growth_rate, daily_growth_rate_t DOUBLE := 0.0;
	DECLARE days_bug_fix			INTEGER := 3;
	
	DECLARE temp_policy				TABLE (
   	    "beta" 		DOUBLE,
		"n_days" 	INTEGER
	);
	
	-- Basic SIR Model Computations
	recovered		:= 0;
	infected 		:= (1.0 / :market_share / :hospitalized_rate);
	susceptible 	:= :population - :infected;
	gamma 			:= 1.0/ :infectious_days;
	
	IF :date_first_hospitalized IS NOT NULL THEN doubling_time := NULL; END IF;
	
	-- Bug Fix: In the app for some reason ithe current day is alwyas assumed to be 3 days before
	cur_date := ADD_DAYS(:cur_date, (-1 *:days_bug_fix));
	
	IF :date_first_hospitalized IS NULL AND (:doubling_time IS NOT NULL AND :doubling_time <> 0.0) THEN
		days_bug_fix := 0; -- For some reason in the app 3 more days are subtracted when this case is selected
		
		-- Back-projecting to when the first hospitalized case would have been admitted
		intrinsic_growth_rate := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
		beta 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate,  :gamma, :susceptible, 0.0);
    	beta_t 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, :relative_contact_rate);
   		
   		i_day   := 0; --seed to the full length
		
	    :temp_policy.INSERT( (:beta, :n_days ) );
	    
	    CALL "COVID19"."CHIME_RUN_PROJECTION"(
	    	:susceptible, :infected, :recovered, :gamma, :i_day, :temp_policy,
	    	:hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date,
	    	:hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay,
	    	
	    	sir_tbl, dispositions_tbl, admissions_tbl, census_tbl, :current_infected
	    );
	    
		
		-- The code equivalent for computing the get_argmin_ds part
		SELECT TO_INTEGER("ID") INTO i_day 
		FROM (
			SELECT a."ID", POWER( (a."CENS_HOSPITALIZED" - :current_hospitalized), 2) AS "POW"
			FROM :census_tbl AS a
			INNER JOIN (
				SELECT a."ID" AS "ID"
				FROM :census_tbl AS a
				INNER JOIN (
					SELECT MAX("CENS_HOSPITALIZED") AS "MAX" FROM :census_tbl
				)AS b ON a."CENS_HOSPITALIZED" = b."MAX"
				ORDER BY "ID" ASC
			) AS b ON a."ID" < b."ID"
		)
		WHERE "POW" IS NOT NULL
		ORDER BY "POW" ASC
		LIMIT 1
		;
		
	    policies = SELECT * FROM "COVID19"."CHIME_GEN_POLICY" ( :mitigation_date, :i_day, :n_days, :beta, :beta_t); 
	    CALL "COVID19"."CHIME_RUN_PROJECTION"(
	    	:susceptible, :infected, :recovered, :gamma, :i_day, :policies,
	    	:hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date,
	    	:hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay,
	    	
	    	sir_tbl, dispositions_tbl, admissions_tbl, census_tbl, :current_infected
	    );
	    date_first_hospitalized := ADD_DAYS(:cur_date, (-1*:i_day));
	ELSEIF :date_first_hospitalized IS NOT NULL AND :doubling_time IS NULL  THEN 
		-- Fitting spread parameter to observed hospital census (dates of 1 patient and today)
		i_day := DAYS_BETWEEN(:date_first_hospitalized, :cur_date);
		
		-- Make an initial coarse estimate
		CALL "COVID19"."CHIME_GET_ARGMIN_DOUBLING_TIME" (1, 15, 15, :susceptible, :infected, :recovered, :gamma, :relative_contact_rate, :mitigation_date, :i_day, :n_days, :hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date, :hospitalized_length_of_stay, :icu_length_of_stay,  :ventilated_length_of_stay, :current_hospitalized, :min_loss_idx, :dts);
	
		-- Refine the coarse estimate
		FOR iter IN 1 .. (4) DO	
			CALL "COVID19"."CHIME_GET_ARGMIN_DOUBLING_TIME" (:dts[:min_loss_idx-1 +1], :dts[:min_loss_idx +1], 15, :susceptible, :infected, :recovered, :gamma, :relative_contact_rate, :mitigation_date, :i_day, :n_days, :hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date, :hospitalized_length_of_stay, :icu_length_of_stay,  :ventilated_length_of_stay, :current_hospitalized, :min_loss_idx, :dts);
			--SELECT :min_loss_idx AS "min_loss_idx" FROM DUMMY;
			--SELECT :dts[:min_loss_idx+1] AS "doubling_time in for", :min_loss_idx AS "min_loss_idx" FROM DUMMY;
			--res_check = UNNEST(:dts);
			--SELECT * FROM :res_check;
		END FOR;
		
		doubling_time := :dts[:min_loss_idx +1];
		--SELECT :dts[:min_loss_idx +1];AS "doubling_time", :min_loss_idx AS "min_loss_idx" FROM DUMMY;
		
		intrinsic_growth_rate := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
		beta 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, 0.0);
    	beta_t 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, :relative_contact_rate);
   		
   		policies = SELECT * FROM "COVID19"."CHIME_GEN_POLICY" ( :mitigation_date, :i_day, :n_days, :beta, :beta_t); 
	    CALL "COVID19"."CHIME_RUN_PROJECTION"(
	    	:susceptible, :infected, :recovered, :gamma, :i_day, :policies,
	    	:hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date,
	    	:hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay,
	    	
	    	sir_tbl, dispositions_tbl, admissions_tbl, census_tbl, :current_infected
	    );
	END IF;

	intrinsic_growth_rate := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
	beta 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, 0.0);
    beta_t 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, :relative_contact_rate);
	
	-- r_t is r_0 after distancing
	r_t 	 := :beta_t / :gamma * :susceptible;
	r_naught := :beta   / :gamma * :susceptible;
	
	doubling_time_t := LOG( (:beta_t * :susceptible - :gamma + 1), 2);
	
	daily_growth_rate   := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
	daily_growth_rate_t := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time_t);
		
	SELECT "SIR_SUSCEPTIBLE" INTO susceptible	FROM :sir_tbl WHERE "ID" = 0;
	SELECT "SIR_INFECTED" 	 INTO infected		FROM :sir_tbl WHERE "ID" = 0;
	SELECT "SIR_RECOVERED" 	 INTO recovered		FROM :sir_tbl WHERE "ID" = 0;
		
	RETURN SELECT 	a."ID", --b."DATE", -- there is some inconsistency in the results to the ID and the cur_date
							ADD_DAYS(TO_DATE(:cur_date), a."ID") AS "DATE", 
							a."SIR_SUSCEPTIBLE", a."SIR_INFECTED", a."SIR_RECOVERED", 
							b."DISP_HOSPITALIZED", b."DISP_ICU", b."DISP_VENTILATED",
							c."ADM_HOSPITALIZED",  c."ADM_ICU",  c."ADM_VENTILATED",
							d."CENS_HOSPITALIZED", d."CENS_ICU", d."CENS_VENTILATED"
					FROM :sir_tbl AS a
					LEFT JOIN :dispositions_tbl AS b ON a."ID" = b."ID"
					LEFT JOIN :admissions_tbl 	AS c ON a."ID" = c."ID"
					LEFT JOIN :census_tbl 		AS d ON a."ID" = d."ID"
	;
	
	/* -- alternatively, return rounded results for disp/adm/cens hospitalized/icu/ventilated
	RETURN 	SELECT 	a."ID", --b."DATE", -- there is some inconsistency in the results to the ID and the cur_date
							ADD_DAYS(TO_DATE(:cur_date), a."ID") AS "DATE", 
							a."SIR_SUSCEPTIBLE", a."SIR_INFECTED", a."SIR_RECOVERED", 
							FLOOR(b."DISP_HOSPITALIZED") AS "DISP_HOSPITALIZED", FLOOR(b."DISP_ICU") AS "DISP_ICU", FLOOR(b."DISP_VENTILATED") AS "DISP_VENTILATED",
							FLOOR(c."ADM_HOSPITALIZED")  AS "ADM_HOSPITALIZED",  FLOOR(c."ADM_ICU")  AS "ADM_ICU",  FLOOR(c."ADM_VENTILATED")  AS "ADM_VENTILATED",
							FLOOR(d."CENS_HOSPITALIZED") AS "CENS_HOSPITALIZED", FLOOR(d."CENS_ICU") AS "CENS_ICU", FLOOR(d."CENS_VENTILATED") AS "CENS_VENTILATED"
					FROM :sir_tbl AS a
					LEFT JOIN :dispositions_tbl AS b ON a."ID" = b."ID"
					LEFT JOIN :admissions_tbl 	AS c ON a."ID" = c."ID"
					LEFT JOIN :census_tbl 		AS d ON a."ID" = d."ID"
	;
	*/			
		
END;

SELECT * FROM COVID19.CHIME_TABLE_UDF_SCALAR_INPUT( 
	3600000,  -- population
	2.5/100, --hospitalized_rate  2.5%
	0.75/100, --icu_rate 0.75%
	0.5/100, --ventilated_rate 0.5%
	9, --7; --hospitalized_length_of_stay
	10, --9; --icu_length_of_stay
	11, --10; --ventilated_length_of_stay
	69, --14, --current_hospitalized
	30/100, --relative_contact_rate   30% social distancing
	TO_DATE(NOW()),   -- cur_date     Formerly used as start_date
	NULL,   --mitigation_date
	NULL,   --date_first_hospitalized	
	4,  --doubling_time
	14, --infectious_days --Formerly used as recovery_days and was set by default to 14 based on CDC is recommending 14 days of self-quarantine - see https://code-for-philly.gitbook.io/chime/what-is-chime/sir-modeling
	15/100, --market_share 15%
	60,  -- n_days   days to run forward simulation
	0    -- i_day		
);



------------------------
-- Approach 3. SAP HANA user defined table function with CHIME parameters read in from a table
--
-- If you try to execute the entire script for approach 2 in one shot and encounter error message:
--    Could not execute 'CREATE FUNCTION COVID19.CHIME_TABLE_UDF_TABLE_INPUT RETURNS TABLE ( "ID" INTEGER, "DATE" DATE, ...'
--    SAP DBTech JDBC: [257]: sql syntax error: incorrect syntax near "TRUNCATE": line 209 col 1 (at pos 10556)
-- The solution is to execute the CREATE FUNCTION statement first, then separately execute as one block the three subsequent statements for truncate/insert/select
------------------------

SET SCHEMA COVID19;

-- table holding the input parameters to the CHIME calculation. 
-- this can also be a view, e.g. the parameter current_hospitalized may be a calculated field from other hospital data sources.
DROP TABLE COVID19.CHIME_INPUT_PARAMETERS;
CREATE COLUMN TABLE COVID19.CHIME_INPUT_PARAMETERS(
  population DOUBLE,
  hospitalized_rate DOUBLE,
  icu_rate DOUBLE,
  ventilated_rate DOUBLE,
  hospitalized_length_of_stay INTEGER,
  icu_length_of_stay INTEGER,
  ventilated_length_of_stay INTEGER,
  current_hospitalized INTEGER,
  relative_contact_rate DOUBLE,  
  cur_date DATE,
  mitigation_date DATE,
  date_first_hospitalized DATE,  
  doubling_time DOUBLE,
  infectious_days			DOUBLE,
  market_share DOUBLE,  
  n_days INTEGER,
  i_day INTEGER  
);


DROP FUNCTION COVID19.CHIME_TABLE_UDF_TABLE_INPUT;
CREATE FUNCTION COVID19.CHIME_TABLE_UDF_TABLE_INPUT RETURNS TABLE (
  "ID" 				INTEGER,
	"DATE"   			DATE,
	"SIR_SUSCEPTIBLE"	DOUBLE,
	"SIR_INFECTED" 		DOUBLE,
	"SIR_RECOVERED"		DOUBLE,
	"DISP_HOSPITALIZED" DOUBLE, 
	"DISP_ICU" DOUBLE, 
	"DISP_VENTILATED" DOUBLE,
	"ADM_HOSPITALIZED" DOUBLE,
	"ADM_ICU" DOUBLE,
	"ADM_VENTILATED" DOUBLE,
	"CENS_HOSPITALIZED" DOUBLE,
	"CENS_ICU" DOUBLE,
	"CENS_VENTILATED" DOUBLE			
) AS
BEGIN
	-- Parameters Setup
	DECLARE population 				DOUBLE  := 3600000;
	DECLARE hospitalized_rate		DOUBLE  := 2.5/100;  --2.5%
	DECLARE icu_rate				DOUBLE  := 0.75/100; --0.75%
	DECLARE ventilated_rate			DOUBLE  := 0.5/100;  --0.5%
	
	DECLARE hospitalized_length_of_stay	INTEGER := 9;--7;
	DECLARE icu_length_of_stay			INTEGER := 10;--9;
	DECLARE ventilated_length_of_stay	INTEGER := 11;--10;
	
	DECLARE current_hospitalized	INTEGER := 69;
	DECLARE relative_contact_rate	DOUBLE  := 30/100; --#30% social distancing

	DECLARE cur_date				DATE	:= TO_DATE(NOW()); -- Formerly used as start_date
  DECLARE mitigation_date			DATE 	:= NULL;
  DECLARE date_first_hospitalized DATE    := NULL; --'2020-03-18';--NULL; --'2020-03-25'; --NULL
    
	DECLARE doubling_time 			DOUBLE  := 4; --4; --NULL; --4
	DECLARE infectious_days			DOUBLE  := 14; --Formerly used as recovery_days and was set by default to 14 based on CDC is recommending 14 days of self-quarantine - see https://code-for-philly.gitbook.io/chime/what-is-chime/sir-modeling
	DECLARE market_share			DOUBLE  := 15/100; --15%
	
	DECLARE	n_days					INTEGER := 100; -- days to run forward simulation
	
	DECLARE i_day					INTEGER := 0;
	
	-- Assisting Variables for CHIME Model Computations
	DECLARE infected				DOUBLE  := 0;
	DECLARE susceptible				DOUBLE  := 0;
	DECLARE recovered				DOUBLE  := 0;
	DECLARE intrinsic_growth_rate	DOUBLE  := 0;
	DECLARE gamma					DOUBLE  := 0;
	DECLARE beta					DOUBLE  := 0;
	DECLARE beta_t					DOUBLE  := 0;
	
	DECLARE current_infected		DOUBLE  := 0;
	DECLARE iter					INTEGER := 0;
	DECLARE min_loss_idx			INTEGER := 0;
	DECLARE dts 					DOUBLE ARRAY;
	DECLARE r_t						DOUBLE := 0.0;
	DECLARE r_naught				DOUBLE := 0.0;
	DECLARE doubling_time_t 		DOUBLE  := NULL;
	DECLARE daily_growth_rate, daily_growth_rate_t DOUBLE := 0.0;
	DECLARE days_bug_fix			INTEGER := 3;
	DECLARE temp_policy				TABLE (
   	    "beta" 		DOUBLE,
		"n_days" 	INTEGER
	);
  
	-- read input parameter values from table COVID19.CHIME_TABLE_UDF_TABLE_INPUT
	select population, hospitalized_rate, icu_rate, ventilated_rate, hospitalized_length_of_stay, icu_length_of_stay, ventilated_length_of_stay,	current_hospitalized,relative_contact_rate,cur_date, mitigation_date, date_first_hospitalized, 	doubling_time, infectious_days,market_share,  n_days, i_day  
	   into population, hospitalized_rate, icu_rate, ventilated_rate, hospitalized_length_of_stay, icu_length_of_stay, ventilated_length_of_stay,	current_hospitalized,relative_contact_rate,cur_date, mitigation_date, date_first_hospitalized, 	doubling_time, infectious_days,market_share,  n_days, i_day  
	from COVID19.CHIME_INPUT_PARAMETERS;

	-- Basic SIR Model Computations
	recovered		:= 0;
	infected 		:= (1.0 / :market_share / :hospitalized_rate);
	susceptible 	:= :population - :infected;
	gamma 			:= 1.0/ :infectious_days;
	
	IF :date_first_hospitalized IS NOT NULL THEN doubling_time := NULL; END IF;
	
	-- Bug Fix: In the app for some reason ithe current day is alwyas assumed to be 3 days before
	cur_date := ADD_DAYS(:cur_date, (-1 *:days_bug_fix));
	
	IF :date_first_hospitalized IS NULL AND (:doubling_time IS NOT NULL AND :doubling_time <> 0.0) THEN
		days_bug_fix := 0; -- For some reason in the app 3 more days are subtracted when this case is selected
		
		-- Back-projecting to when the first hospitalized case would have been admitted
		intrinsic_growth_rate := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
		beta 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate,  :gamma, :susceptible, 0.0);
    	beta_t 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, :relative_contact_rate);
   		
   		i_day   := 0; --seed to the full length
		
	    :temp_policy.INSERT( (:beta, :n_days ) );
	    
	    CALL "COVID19"."CHIME_RUN_PROJECTION"(
	    	:susceptible, :infected, :recovered, :gamma, :i_day, :temp_policy,
	    	:hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date,
	    	:hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay,
	    	
	    	sir_tbl, dispositions_tbl, admissions_tbl, census_tbl, :current_infected
	    );
	    
		
		-- The code equivalent for computing the get_argmin_ds part
		SELECT TO_INTEGER("ID") INTO i_day 
		FROM (
			SELECT a."ID", POWER( (a."CENS_HOSPITALIZED" - :current_hospitalized), 2) AS "POW"
			FROM :census_tbl AS a
			INNER JOIN (
				SELECT a."ID" AS "ID"
				FROM :census_tbl AS a
				INNER JOIN (
					SELECT MAX("CENS_HOSPITALIZED") AS "MAX" FROM :census_tbl
				)AS b ON a."CENS_HOSPITALIZED" = b."MAX"
				ORDER BY "ID" ASC
			) AS b ON a."ID" < b."ID"
		)
		WHERE "POW" IS NOT NULL
		ORDER BY "POW" ASC
		LIMIT 1
		;
		
	    policies = SELECT * FROM "COVID19"."CHIME_GEN_POLICY" ( :mitigation_date, :i_day, :n_days, :beta, :beta_t); 
	    CALL "COVID19"."CHIME_RUN_PROJECTION"(
	    	:susceptible, :infected, :recovered, :gamma, :i_day, :policies,
	    	:hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date,
	    	:hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay,
	    	
	    	sir_tbl, dispositions_tbl, admissions_tbl, census_tbl, :current_infected
	    );
	    date_first_hospitalized := ADD_DAYS(:cur_date, (-1*:i_day));
	ELSEIF :date_first_hospitalized IS NOT NULL AND :doubling_time IS NULL  THEN 
		-- Fitting spread parameter to observed hospital census (dates of 1 patient and today)
		i_day := DAYS_BETWEEN(:date_first_hospitalized, :cur_date);
		
		-- Make an initial coarse estimate
		CALL "COVID19"."CHIME_GET_ARGMIN_DOUBLING_TIME" (1, 15, 15, :susceptible, :infected, :recovered, :gamma, :relative_contact_rate, :mitigation_date, :i_day, :n_days, :hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date, :hospitalized_length_of_stay, :icu_length_of_stay,  :ventilated_length_of_stay, :current_hospitalized, :min_loss_idx, :dts);
	
		-- Refine the coarse estimate
		FOR iter IN 1 .. (4) DO	
			CALL "COVID19"."CHIME_GET_ARGMIN_DOUBLING_TIME" (:dts[:min_loss_idx-1 +1], :dts[:min_loss_idx +1], 15, :susceptible, :infected, :recovered, :gamma, :relative_contact_rate, :mitigation_date, :i_day, :n_days, :hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date, :hospitalized_length_of_stay, :icu_length_of_stay,  :ventilated_length_of_stay, :current_hospitalized, :min_loss_idx, :dts);
			--SELECT :min_loss_idx AS "min_loss_idx" FROM DUMMY;
			--SELECT :dts[:min_loss_idx+1] AS "doubling_time in for", :min_loss_idx AS "min_loss_idx" FROM DUMMY;
			--res_check = UNNEST(:dts);
			--SELECT * FROM :res_check;
		END FOR;
		
		doubling_time := :dts[:min_loss_idx +1];
		--SELECT :dts[:min_loss_idx +1];AS "doubling_time", :min_loss_idx AS "min_loss_idx" FROM DUMMY;
		
		intrinsic_growth_rate := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
		beta 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, 0.0);
    	beta_t 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, :relative_contact_rate);
   		
   		policies = SELECT * FROM "COVID19"."CHIME_GEN_POLICY" ( :mitigation_date, :i_day, :n_days, :beta, :beta_t); 
	    CALL "COVID19"."CHIME_RUN_PROJECTION"(
	    	:susceptible, :infected, :recovered, :gamma, :i_day, :policies,
	    	:hospitalized_rate, :icu_rate, :ventilated_rate, :market_share, :cur_date,
	    	:hospitalized_length_of_stay, :icu_length_of_stay, :ventilated_length_of_stay,
	    	
	    	sir_tbl, dispositions_tbl, admissions_tbl, census_tbl, :current_infected
	    );
	END IF;

	intrinsic_growth_rate := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
	beta 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, 0.0);
    beta_t 	:= "COVID19"."CHIME_GET_BETA"(:intrinsic_growth_rate, :gamma, :susceptible, :relative_contact_rate);
	
	-- r_t is r_0 after distancing
	r_t 	 := :beta_t / :gamma * :susceptible;
	r_naught := :beta   / :gamma * :susceptible;
	
	doubling_time_t := LOG( (:beta_t * :susceptible - :gamma + 1), 2);
	
	daily_growth_rate   := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time);
	daily_growth_rate_t := "COVID19"."CHIME_GET_GROWTH_RATE"(:doubling_time_t);
		
	SELECT "SIR_SUSCEPTIBLE" INTO susceptible	FROM :sir_tbl WHERE "ID" = 0;
	SELECT "SIR_INFECTED" 	 INTO infected		FROM :sir_tbl WHERE "ID" = 0;
	SELECT "SIR_RECOVERED" 	 INTO recovered		FROM :sir_tbl WHERE "ID" = 0;
		
	RETURN SELECT 	a."ID", --b."DATE", -- there is some inconsistency in the results to the ID and the cur_date
							ADD_DAYS(TO_DATE(:cur_date), a."ID") AS "DATE", 
							a."SIR_SUSCEPTIBLE", a."SIR_INFECTED", a."SIR_RECOVERED", 
							b."DISP_HOSPITALIZED", b."DISP_ICU", b."DISP_VENTILATED",
							c."ADM_HOSPITALIZED",  c."ADM_ICU",  c."ADM_VENTILATED",
							d."CENS_HOSPITALIZED", d."CENS_ICU", d."CENS_VENTILATED"
					FROM :sir_tbl AS a
					LEFT JOIN :dispositions_tbl AS b ON a."ID" = b."ID"
					LEFT JOIN :admissions_tbl 	AS c ON a."ID" = c."ID"
					LEFT JOIN :census_tbl 		AS d ON a."ID" = d."ID"
	;
	
	/* -- alternatively, return rounded results for disp/adm/cens hospitalized/icu/ventilated
	RETURN 	SELECT 	a."ID", --b."DATE", -- there is some inconsistency in the results to the ID and the cur_date
							ADD_DAYS(TO_DATE(:cur_date), a."ID") AS "DATE", 
							a."SIR_SUSCEPTIBLE", a."SIR_INFECTED", a."SIR_RECOVERED", 
							FLOOR(b."DISP_HOSPITALIZED") AS "DISP_HOSPITALIZED", FLOOR(b."DISP_ICU") AS "DISP_ICU", FLOOR(b."DISP_VENTILATED") AS "DISP_VENTILATED",
							FLOOR(c."ADM_HOSPITALIZED")  AS "ADM_HOSPITALIZED",  FLOOR(c."ADM_ICU")  AS "ADM_ICU",  FLOOR(c."ADM_VENTILATED")  AS "ADM_VENTILATED",
							FLOOR(d."CENS_HOSPITALIZED") AS "CENS_HOSPITALIZED", FLOOR(d."CENS_ICU") AS "CENS_ICU", FLOOR(d."CENS_VENTILATED") AS "CENS_VENTILATED"
					FROM :sir_tbl AS a
					LEFT JOIN :dispositions_tbl AS b ON a."ID" = b."ID"
					LEFT JOIN :admissions_tbl 	AS c ON a."ID" = c."ID"
					LEFT JOIN :census_tbl 		AS d ON a."ID" = d."ID"
	;
	*/		
END;


TRUNCATE TABLE COVID19.CHIME_INPUT_PARAMETERS;
INSERT INTO COVID19.CHIME_INPUT_PARAMETERS VALUES ( 
3600000,  -- population
	2.5/100, --hospitalized_rate  2.5%
	0.75/100, --icu_rate 0.75%
	0.5/100, --ventilated_rate 0.5%
	9, --7; --hospitalized_length_of_stay
	10, --9; --icu_length_of_stay
	11, --10; --ventilated_length_of_stay
	69, --14, --current_hospitalized
	30/100, --relative_contact_rate   30% social distancing
	TO_DATE(NOW()),   -- cur_date     Formerly used as start_date
	NULL,   --mitigation_date
	NULL,   --date_first_hospitalized	
	4,  --doubling_time
	14, --infectious_days --Formerly used as recovery_days and was set by default to 14 based on CDC is recommending 14 days of self-quarantine - see https://code-for-philly.gitbook.io/chime/what-is-chime/sir-modeling
	15/100, --market_share 15%
	60,  -- n_days   days to run forward simulation
	0    -- i_day		
);
SELECT * FROM COVID19.CHIME_TABLE_UDF_TABLE_INPUT();


