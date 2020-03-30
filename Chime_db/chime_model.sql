/***
Based on Python code from https://code-for-philly.gitbook.io/chime/ as of March 28, 2020
Python code converted to SAP HANA SQLScript
Author: Dimitrios Lyras, SAP

Contacts: Dimitrios Lyras(dimitrios.lyras@sap.com), Qi Su (qi.su@sap.com)
**/

------------------------
-- Approach 1. SAP HANA SQLScript store procedure script with hard coded variable values
------------------------


DO BEGIN
	-- Parameters Setup
	DECLARE population 				DOUBLE  := 4119404.9966666666 +3733.333333334;
	DECLARE doubling_time 			DOUBLE  := 4;
	DECLARE recovery_days			DOUBLE  := 14;
	DECLARE	n_days					INTEGER := 60; -- days to run forward simulation
	DECLARE start_date				DATE	:= TO_DATE(NOW());
	
	DECLARE current_hospitalized	INTEGER := 14;
	DECLARE market_share			DOUBLE  := 15/100; --15%
	DECLARE relative_contact_rate	DOUBLE  := 30/100; --#30% social distancing

	DECLARE hospitalized_rate		DOUBLE  := 2.5/100;  --2.5%
	DECLARE icu_rate				DOUBLE  := 0.75/100; --0.75%
	DECLARE ventilated_rate			DOUBLE  := 0.5/100;  --0.5%

	DECLARE hospitalized_length_of_stay	INTEGER := 7;
	DECLARE icu_length_of_stay			INTEGER := 9;
	DECLARE ventilated_length_of_stay	INTEGER := 10;

	-- Assisting Variables for SIR Model Computations
	DECLARE infected				DOUBLE  := 0;
	DECLARE susceptible				DOUBLE  := 0;
	DECLARE recovered				DOUBLE  := 0;
	DECLARE intrinsic_growth_rate	DOUBLE  := 0;
	DECLARE gamma					DOUBLE  := 0;
	DECLARE beta					DOUBLE  := 0;
	
	-- The SIR Model results table
	DECLARE sir_tbl TABLE (
   	    "ID" 				INTEGER,
		"DATE"   			DATE,
		"SIR_SUSCEPTIBLE"	DOUBLE,
		"SIR_INFECTED" 		DOUBLE,
		"SIR_RECOVERED"		DOUBLE
	)
	;
	

	-- Basic SIR Model Computations
	infected 		:= ( :current_hospitalized / :market_share / :hospitalized_rate);
	susceptible 	:= :population - :infected;
	IF :doubling_time > 0 THEN
		intrinsic_growth_rate := POWER(2.0, (1.0 / :doubling_time)) - 1.0;
	ELSE
		intrinsic_growth_rate := 0.0;
	END IF;
	gamma := 1.0 / :recovery_days;
	beta := ((:intrinsic_growth_rate + :gamma)/ :susceptible * (1.0 - :relative_contact_rate));

	-- Run forward Simulations using SIR model
	BEGIN
		DECLARE s 		DOUBLE := ROUND(:susceptible, 0);
		DECLARE i 		DOUBLE := :infected;
		DECLARE r 		DOUBLE := :recovered;
		DECLARE n 		DOUBLE := (:s + :i + :r);
		DECLARE iter 	INTEGER := 0;
		DECLARE s_n, i_n, r_n, scale	DOUBLE :=0;
		
		:sir_tbl.INSERT((0, :start_date, :s, :i, :r ) );
		FOR iter IN 1 .. (:n_days) DO
			
			s_n := (-:beta * :s * :i) + :s;
		    i_n := (:beta * :s * :i - :gamma * :i) + :i;
		    r_n := :gamma * :i + :r;
		   
		    IF :s_n < 0.0 THEN s_n := 0.0; END IF; 
		    IF :i_n < 0.0 THEN i_n := 0.0; END IF;  
		    IF :r_n < 0.0 THEN r_n := 0.0; END IF;  
		    
		    scale := :n / (:s_n + :i_n + :r_n);
		    
		    s := :s_n;
		    i := :i_n;
		    r := :r_n;
		    
		    :sir_tbl.INSERT((:iter, ADD_DAYS(:start_date, :iter), (:s_n * :scale), (:i_n * :scale), (:r_n * :scale) ) );
		END FOR;
		--SELECT * FROM :sir_tbl;
	END;
	
	-- Compute Dispositions
	dispositions_tbl = 	SELECT 	"ID", "DATE", 
								("SIR_INFECTED" + "SIR_RECOVERED") * :hospitalized_rate * :market_share AS "DISP_HOSPITALIZED",
								("SIR_INFECTED" + "SIR_RECOVERED") * :icu_rate          * :market_share AS "DISP_ICU",
								("SIR_INFECTED" + "SIR_RECOVERED") * :ventilated_rate   * :market_share AS "DISP_VENTILATED"
						FROM :sir_tbl
						;
	--SELECT * FROM :dispositions_tbl;
	
	-- Compute Admissions
	admissions_tbl = 	SELECT 	"ID", "DATE", 
								"DISP_HOSPITALIZED" - LAG("DISP_HOSPITALIZED", 1) OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_HOSPITALIZED",
								"DISP_ICU" 		    - LAG("DISP_ICU", 1) 		  OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_ICU",
								"DISP_VENTILATED"   - LAG("DISP_VENTILATED", 1)   OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_VENTILATED"
						FROM :dispositions_tbl
						;
	--SELECT * FROM :admissions_tbl;
	
	-- Compute Cencus
	census_tbl_tmp 	=	SELECT 	"ID", "DATE", 
								SUM("ADM_HOSPITALIZED") OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_HOSPITALIZED_SUM",
								SUM("ADM_ICU") 			OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_ICU_SUM",
								SUM("ADM_VENTILATED") 	OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_VENTILATED_SUM"
						FROM :admissions_tbl
						;
	--SELECT * FROM :census_tbl_tmp;
	
	census_tbl 	= 		SELECT 	"ID", "DATE", 
								CEIL("CENS_HOSPITALIZED_SUM"- IFNULL(LAG("CENS_HOSPITALIZED_SUM", 	:hospitalized_length_of_stay) 	OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_HOSPITALIZED",
								CEIL("CENS_ICU_SUM" 		- IFNULL(LAG("CENS_ICU_SUM", 			:icu_length_of_stay) 			OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_ICU",
								CEIL("CENS_VENTILATED_SUM" 	- IFNULL(LAG("CENS_VENTILATED_SUM", 	:ventilated_length_of_stay) 	OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_VENTILATED"
						FROM :census_tbl_tmp
						;
	--SELECT * FROM :census_tbl;	
	
	SELECT 	a.*, 
			b."DISP_HOSPITALIZED", b."DISP_ICU", b."DISP_VENTILATED",
			c."ADM_HOSPITALIZED",  c."ADM_ICU",  c."ADM_VENTILATED",
			d."CENS_HOSPITALIZED", d."CENS_ICU", d."CENS_VENTILATED"
	FROM :sir_tbl AS a
	LEFT JOIN :dispositions_tbl AS b ON a."ID" = b."ID"
	LEFT JOIN :admissions_tbl 	AS c ON a."ID" = c."ID"
	LEFT JOIN :census_tbl 		AS d ON a."ID" = d."ID"
	;
END;



------------------------
-- Approach 2. SAP HANA user defined table function with CHIME parameters as scalar function input parameters
------------------------

CREATE SCHEMA COVID19;
SET SCHEMA COVID19;

DROP FUNCTION COVID19.CHIME_TABLE_UDF_SCALAR_INPUT;
CREATE FUNCTION COVID19.CHIME_TABLE_UDF_SCALAR_INPUT (
  population DOUBLE,
  doubling_time DOUBLE,
  recovery_days DOUBLE,
  n_days INTEGER,
  start_date DATE,
  current_hospitalized INTEGER,
  market_share DOUBLE,
  relative_contact_rate DOUBLE,
  hospitalized_rate DOUBLE,
  icu_rate DOUBLE,
  ventilated_rate DOUBLE,
  hospitalized_length_of_stay INTEGER,
  icu_length_of_stay INTEGER,
  ventilated_length_of_stay INTEGER  
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
	
	-- Assisting Variables for SIR Model Computations
	DECLARE infected				DOUBLE  := 0;
	DECLARE susceptible				DOUBLE  := 0;
	DECLARE recovered				DOUBLE  := 0;
	DECLARE intrinsic_growth_rate	DOUBLE  := 0;
	DECLARE gamma					DOUBLE  := 0;
	DECLARE beta					DOUBLE  := 0;
	
	-- The SIR Model results table
	DECLARE sir_tbl TABLE (
   	    "ID" 				INTEGER,
		"DATE"   			DATE,
		"SIR_SUSCEPTIBLE"	DOUBLE,
		"SIR_INFECTED" 		DOUBLE,
		"SIR_RECOVERED"		DOUBLE
	)
	;
	

	-- Basic SIR Model Computations
	infected 		:= ( :current_hospitalized / :market_share / :hospitalized_rate);
	susceptible 	:= :population - :infected;
	IF :doubling_time > 0 THEN
		intrinsic_growth_rate := POWER(2.0, (1.0 / :doubling_time)) - 1.0;
	ELSE
		intrinsic_growth_rate := 0.0;
	END IF;
	gamma := 1.0 / :recovery_days;
	beta := ((:intrinsic_growth_rate + :gamma)/ :susceptible * (1.0 - :relative_contact_rate));

	-- Run forward Simulations using SIR model
	BEGIN
		DECLARE s 		DOUBLE := ROUND(:susceptible, 0);
		DECLARE i 		DOUBLE := :infected;
		DECLARE r 		DOUBLE := :recovered;
		DECLARE n 		DOUBLE := (:s + :i + :r);
		DECLARE iter 	INTEGER := 0;
		DECLARE s_n, i_n, r_n, scale	DOUBLE :=0;
		
		:sir_tbl.INSERT((0, :start_date, :s, :i, :r ) );
		FOR iter IN 1 .. (:n_days) DO
			
			s_n := (-:beta * :s * :i) + :s;
		    i_n := (:beta * :s * :i - :gamma * :i) + :i;
		    r_n := :gamma * :i + :r;
		   
		    IF :s_n < 0.0 THEN s_n := 0.0; END IF; 
		    IF :i_n < 0.0 THEN i_n := 0.0; END IF;  
		    IF :r_n < 0.0 THEN r_n := 0.0; END IF;  
		    
		    scale := :n / (:s_n + :i_n + :r_n);
		    
		    s := :s_n;
		    i := :i_n;
		    r := :r_n;
		    
		    :sir_tbl.INSERT((:iter, ADD_DAYS(:start_date, :iter), (:s_n * :scale), (:i_n * :scale), (:r_n * :scale) ) );
		END FOR;
		--SELECT * FROM :sir_tbl;
	END;
	
	-- Compute Dispositions
	dispositions_tbl = 	SELECT 	"ID", "DATE", 
								("SIR_INFECTED" + "SIR_RECOVERED") * :hospitalized_rate * :market_share AS "DISP_HOSPITALIZED",
								("SIR_INFECTED" + "SIR_RECOVERED") * :icu_rate          * :market_share AS "DISP_ICU",
								("SIR_INFECTED" + "SIR_RECOVERED") * :ventilated_rate   * :market_share AS "DISP_VENTILATED"
						FROM :sir_tbl
						;
	--SELECT * FROM :dispositions_tbl;
	
	-- Compute Admissions
	admissions_tbl = 	SELECT 	"ID", "DATE", 
								"DISP_HOSPITALIZED" - LAG("DISP_HOSPITALIZED", 1) OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_HOSPITALIZED",
								"DISP_ICU" 		    - LAG("DISP_ICU", 1) 		  OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_ICU",
								"DISP_VENTILATED"   - LAG("DISP_VENTILATED", 1)   OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_VENTILATED"
						FROM :dispositions_tbl
						;
	--SELECT * FROM :admissions_tbl;
	
	-- Compute Cencus
	census_tbl_tmp 	=	SELECT 	"ID", "DATE", 
								SUM("ADM_HOSPITALIZED") OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_HOSPITALIZED_SUM",
								SUM("ADM_ICU") 			OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_ICU_SUM",
								SUM("ADM_VENTILATED") 	OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_VENTILATED_SUM"
						FROM :admissions_tbl
						;
	--SELECT * FROM :census_tbl_tmp;
	
	census_tbl 	= 		SELECT 	"ID", "DATE", 
								CEIL("CENS_HOSPITALIZED_SUM"- IFNULL(LAG("CENS_HOSPITALIZED_SUM", 	:hospitalized_length_of_stay) 	OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_HOSPITALIZED",
								CEIL("CENS_ICU_SUM" 		- IFNULL(LAG("CENS_ICU_SUM", 			:icu_length_of_stay) 			OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_ICU",
								CEIL("CENS_VENTILATED_SUM" 	- IFNULL(LAG("CENS_VENTILATED_SUM", 	:ventilated_length_of_stay) 	OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_VENTILATED"
						FROM :census_tbl_tmp
						;
	--SELECT * FROM :census_tbl;	
	
	RETURN SELECT 	a.*, 
			b."DISP_HOSPITALIZED", b."DISP_ICU", b."DISP_VENTILATED",
			c."ADM_HOSPITALIZED",  c."ADM_ICU",  c."ADM_VENTILATED",
			d."CENS_HOSPITALIZED", d."CENS_ICU", d."CENS_VENTILATED"
	FROM :sir_tbl AS a
	LEFT JOIN :dispositions_tbl AS b ON a."ID" = b."ID"
	LEFT JOIN :admissions_tbl 	AS c ON a."ID" = c."ID"
	LEFT JOIN :census_tbl 		AS d ON a."ID" = d."ID"
	;
END;

SELECT * FROM COVID19.CHIME_TABLE_UDF_SCALAR_INPUT( 
	4119404.9966666666 +3733.333333334,  -- population
	4,  --doubling_time
	14,  --recovery_days
	60,  -- days to run forward simulation
	TO_DATE(NOW()), --start_date
	14, --current_hospitalized
	15/100, --market_share 15%
	30/100, --relative_contact_rate   30% social distancing
	2.5/100, --hospitalized_rate  2.5%
	0.75/100, --icu_rate 0.75%
	0.5/100, --ventilated_rate 0.5%
	7, --hospitalized_length_of_stay
	9, --icu_length_of_stay
	10 --ventilated_length_of_stay
);



------------------------
-- Approach 3. SAP HANA user defined table function with CHIME parameters read in from a table
------------------------

CREATE SCHEMA COVID19;
SET SCHEMA COVID19;

-- table holding the input parameters to the CHIME calculation. 
-- this can also be a view, e.g. the parameter current_hospitalized may be a calculated field from other hospital data sources.
DROP TABLE COVID19.CHIME_INPUT_PARAMETERS;
CREATE COLUMN TABLE COVID19.CHIME_INPUT_PARAMETERS(
  population DOUBLE,
  doubling_time DOUBLE,
  recovery_days DOUBLE,
  n_days INTEGER,
  start_date DATE,
  current_hospitalized INTEGER,
  market_share DOUBLE,
  relative_contact_rate DOUBLE,
  hospitalized_rate DOUBLE,
  icu_rate DOUBLE,
  ventilated_rate DOUBLE,
  hospitalized_length_of_stay INTEGER,
  icu_length_of_stay INTEGER,
  ventilated_length_of_stay INTEGER  
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
	DECLARE population 				DOUBLE  ;
	DECLARE doubling_time 			DOUBLE  ;
	DECLARE recovery_days			DOUBLE  ;
	DECLARE	n_days					INTEGER ; -- days to run forward simulation
	DECLARE start_date				DATE	;
	
	DECLARE current_hospitalized	INTEGER ;
	DECLARE market_share			DOUBLE  ; --15%
	DECLARE relative_contact_rate	DOUBLE  ; --#30% social distancing

	DECLARE hospitalized_rate		DOUBLE  ;  --2.5%
	DECLARE icu_rate				DOUBLE  ; --0.75%
	DECLARE ventilated_rate			DOUBLE  ;  --0.5%

	DECLARE hospitalized_length_of_stay	INTEGER ;
	DECLARE icu_length_of_stay			INTEGER ;
	DECLARE ventilated_length_of_stay	INTEGER ;
	
	-- Assisting Variables for SIR Model Computations
	DECLARE infected				DOUBLE  := 0;
	DECLARE susceptible				DOUBLE  := 0;
	DECLARE recovered				DOUBLE  := 0;
	DECLARE intrinsic_growth_rate	DOUBLE  := 0;
	DECLARE gamma					DOUBLE  := 0;
	DECLARE beta					DOUBLE  := 0;
	
	-- The SIR Model results table
	DECLARE sir_tbl TABLE (
   	    "ID" 				INTEGER,
		"DATE"   			DATE,
		"SIR_SUSCEPTIBLE"	DOUBLE,
		"SIR_INFECTED" 		DOUBLE,
		"SIR_RECOVERED"		DOUBLE
	)
	;
	
	-- read input parameter values from table COVID19.CHIME_TABLE_UDF_TABLE_INPUT
	select population, doubling_time, recovery_days, n_days, start_date, current_hospitalized, market_share, relative_contact_rate, hospitalized_rate, icu_rate, ventilated_rate, hospitalized_length_of_stay, icu_length_of_stay, ventilated_length_of_stay
	   into population, doubling_time, recovery_days, n_days, start_date, current_hospitalized, market_share, relative_contact_rate, hospitalized_rate, icu_rate, ventilated_rate, hospitalized_length_of_stay, icu_length_of_stay, ventilated_length_of_stay 
	from COVID19.CHIME_INPUT_PARAMETERS;

	-- Basic SIR Model Computations
	infected 		:= ( :current_hospitalized / :market_share / :hospitalized_rate);
	susceptible 	:= :population - :infected;
	IF :doubling_time > 0 THEN
		intrinsic_growth_rate := POWER(2.0, (1.0 / :doubling_time)) - 1.0;
	ELSE
		intrinsic_growth_rate := 0.0;
	END IF;
	gamma := 1.0 / :recovery_days;
	beta := ((:intrinsic_growth_rate + :gamma)/ :susceptible * (1.0 - :relative_contact_rate));

	-- Run forward Simulations using SIR model
	BEGIN
		DECLARE s 		DOUBLE := ROUND(:susceptible, 0);
		DECLARE i 		DOUBLE := :infected;
		DECLARE r 		DOUBLE := :recovered;
		DECLARE n 		DOUBLE := (:s + :i + :r);
		DECLARE iter 	INTEGER := 0;
		DECLARE s_n, i_n, r_n, scale	DOUBLE :=0;
		
		:sir_tbl.INSERT((0, :start_date, :s, :i, :r ) );
		FOR iter IN 1 .. (:n_days) DO
			
			s_n := (-:beta * :s * :i) + :s;
		    i_n := (:beta * :s * :i - :gamma * :i) + :i;
		    r_n := :gamma * :i + :r;
		   
		    IF :s_n < 0.0 THEN s_n := 0.0; END IF; 
		    IF :i_n < 0.0 THEN i_n := 0.0; END IF;  
		    IF :r_n < 0.0 THEN r_n := 0.0; END IF;  
		    
		    scale := :n / (:s_n + :i_n + :r_n);
		    
		    s := :s_n;
		    i := :i_n;
		    r := :r_n;
		    
		    :sir_tbl.INSERT((:iter, ADD_DAYS(:start_date, :iter), (:s_n * :scale), (:i_n * :scale), (:r_n * :scale) ) );
		END FOR;
		--SELECT * FROM :sir_tbl;
	END;
	
	-- Compute Dispositions
	dispositions_tbl = 	SELECT 	"ID", "DATE", 
								("SIR_INFECTED" + "SIR_RECOVERED") * :hospitalized_rate * :market_share AS "DISP_HOSPITALIZED",
								("SIR_INFECTED" + "SIR_RECOVERED") * :icu_rate          * :market_share AS "DISP_ICU",
								("SIR_INFECTED" + "SIR_RECOVERED") * :ventilated_rate   * :market_share AS "DISP_VENTILATED"
						FROM :sir_tbl
						;
	--SELECT * FROM :dispositions_tbl;
	
	-- Compute Admissions
	admissions_tbl = 	SELECT 	"ID", "DATE", 
								"DISP_HOSPITALIZED" - LAG("DISP_HOSPITALIZED", 1) OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_HOSPITALIZED",
								"DISP_ICU" 		    - LAG("DISP_ICU", 1) 		  OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_ICU",
								"DISP_VENTILATED"   - LAG("DISP_VENTILATED", 1)   OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "ADM_VENTILATED"
						FROM :dispositions_tbl
						;
	--SELECT * FROM :admissions_tbl;
	
	-- Compute Cencus
	census_tbl_tmp 	=	SELECT 	"ID", "DATE", 
								SUM("ADM_HOSPITALIZED") OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_HOSPITALIZED_SUM",
								SUM("ADM_ICU") 			OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_ICU_SUM",
								SUM("ADM_VENTILATED") 	OVER (ORDER BY "ID" ASC, "DATE" ASC) AS "CENS_VENTILATED_SUM"
						FROM :admissions_tbl
						;
	--SELECT * FROM :census_tbl_tmp;
	
	census_tbl 	= 		SELECT 	"ID", "DATE", 
								CEIL("CENS_HOSPITALIZED_SUM"- IFNULL(LAG("CENS_HOSPITALIZED_SUM", 	:hospitalized_length_of_stay) 	OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_HOSPITALIZED",
								CEIL("CENS_ICU_SUM" 		- IFNULL(LAG("CENS_ICU_SUM", 			:icu_length_of_stay) 			OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_ICU",
								CEIL("CENS_VENTILATED_SUM" 	- IFNULL(LAG("CENS_VENTILATED_SUM", 	:ventilated_length_of_stay) 	OVER (ORDER BY "ID" ASC, "DATE" ASC), 0)) AS "CENS_VENTILATED"
						FROM :census_tbl_tmp
						;
	--SELECT * FROM :census_tbl;	
	
	RETURN SELECT 	a.*, 
			b."DISP_HOSPITALIZED", b."DISP_ICU", b."DISP_VENTILATED",
			c."ADM_HOSPITALIZED",  c."ADM_ICU",  c."ADM_VENTILATED",
			d."CENS_HOSPITALIZED", d."CENS_ICU", d."CENS_VENTILATED"
	FROM :sir_tbl AS a
	LEFT JOIN :dispositions_tbl AS b ON a."ID" = b."ID"
	LEFT JOIN :admissions_tbl 	AS c ON a."ID" = c."ID"
	LEFT JOIN :census_tbl 		AS d ON a."ID" = d."ID"
	;
END;


TRUNCATE TABLE COVID19.CHIME_INPUT_PARAMETERS;
INSERT INTO COVID19.CHIME_INPUT_PARAMETERS VALUES ( 
4119404.9966666666 +3733.333333334,  -- population
	4,  --doubling_time
	14,  --recovery_days
	60,  -- days to run forward simulation
	TO_DATE(NOW()), --start_date
	14, --current_hospitalized
	15/100, --market_share 15%
	30/100, --relative_contact_rate   30% social distancing
	2.5/100, --hospitalized_rate  2.5%
	0.75/100, --icu_rate 0.75%
	0.5/100, --ventilated_rate 0.5%
	7, --hospitalized_length_of_stay
	9, --icu_length_of_stay
	10 --ventilated_length_of_stay
);
SELECT * FROM COVID19.CHIME_TABLE_UDF_TABLE_INPUT();

-- next change the population value from 4.1M down to 1M
TRUNCATE TABLE COVID19.CHIME_INPUT_PARAMETERS;
INSERT INTO COVID19.CHIME_INPUT_PARAMETERS VALUES ( 
1000000,  -- population
	4,  --doubling_time
	14,  --recovery_days
	60,  -- days to run forward simulation
	TO_DATE(NOW()), --start_date
	14, --current_hospitalized
	15/100, --market_share 15%
	30/100, --relative_contact_rate   30% social distancing
	2.5/100, --hospitalized_rate  2.5%
	0.75/100, --icu_rate 0.75%
	0.5/100, --ventilated_rate 0.5%
	7, --hospitalized_length_of_stay
	9, --icu_length_of_stay
	10 --ventilated_length_of_stay
);
SELECT * FROM COVID19.CHIME_TABLE_UDF_TABLE_INPUT();

-- next change the doubling time to assume the virus is much more infectious (doubling days of 2.2 instead of 4)
TRUNCATE TABLE COVID19.CHIME_INPUT_PARAMETERS;
INSERT INTO COVID19.CHIME_INPUT_PARAMETERS VALUES ( 
1000000,  -- population
	2.2,  --doubling_time
	14,  --recovery_days
	60,  -- days to run forward simulation
	TO_DATE(NOW()), --start_date
	14, --current_hospitalized
	15/100, --market_share 15%
	30/100, --relative_contact_rate   30% social distancing
	2.5/100, --hospitalized_rate  2.5%
	0.75/100, --icu_rate 0.75%
	0.5/100, --ventilated_rate 0.5%
	7, --hospitalized_length_of_stay
	9, --icu_length_of_stay
	10 --ventilated_length_of_stay
);
SELECT * FROM COVID19.CHIME_TABLE_UDF_TABLE_INPUT();