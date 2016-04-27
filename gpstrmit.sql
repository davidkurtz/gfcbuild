set pause off
column num_rows heading 'Total|Segments'
column min_Emplid heading 'First|EMPLID'
column max_Emplid heading 'Last|EMPLID'
column ADJ new_value ADJ heading 'Adjustment|Employees'
column EMP new_value EMP heading 'Total|Employees'
column RPE new_value RPE heading 'Rows per|Employee'

spool gpstrmit
select	cal_run_id
,	min(emplid) min_emplid
,	max(emplid) max_emplid
,	count(distinct emplid) EMP
,	count(*) num_rows
,	count(*)/count(distinct emplid) RPE
,	0 ADJ
FROM	ps_gp_pye_seg_stat
WHERE	cal_run_id = (
		SELECT 	MAX(cal_run_id) max_cal_run_id
		FROM 	ps_gp_pye_seg_stat	
--		WHERE	cal_run_id LIKE 'AN2008%'
		)
group by cal_run_id
;

rollback;
delete FROM  ps_gp_strm;

INSERT INTO ps_gp_strm
(strm_num, emplid_FROM , emplid_to)
SELECT 	partition_number
, 	MIN(part_value) part_start
, 	MAX(part_value) part_end
FROM  	( --calculate partition for each emplid
	SELECT 	part_value
 	,	CEIL(&num_partitions*	
			LEAST(	1,
				SUM(proportion) OVER (ORDER BY part_value range unbounded preceding)
   		    )
                ) partition_number
 	FROM  	( --calculate proportion of month
  		SELECT 	part_value
  		, 	ratio_to_report(elements) OVER () proportion
  		FROM  	( -- sum elements by partion value
			SELECT 	part_value
   			, 	SUM(elements) elements
   			FROM  	( --filter and generate partition key
	    			SELECT 	s.emplid part_value
    				, 	COUNT(*) elements 
    				FROM  	ps_gp_pye_seg_stat s
--				WHERE	cal_run_id = (
--					SELECT 	MAX(cal_run_id) max_cal_run_id
--					FROM 	ps_gp_pye_seg_stat
--					WHERE 	cal_run_id LIKE 'AN2008%'
--					and     NOT CAL_RUN_ID LIKE 'AN2008/__OFF'
--                                      )
				GROUP BY s.emplid
   				UNION ALL
 				SELECT  LTRIM(TO_CHAR(TO_NUMBER(EMPLID_LAST_EMPL)+rownum)), &&RPE
				FROM 	ps_installation_hr /*was ps_installation in HR9.0*/
				CONNECT BY LEVEL <= &&ADJ
    				)
   			GROUP BY part_value
			)
  		)
	)
GROUP BY partition_number
ORDER BY partition_number
/


UPDATE	ps_gp_strm
SET 	emplid_from = '0'
WHERE	strm_num = (
	SELECT 	MIN(strm_num)
	FROM	ps_gp_strm)
;

UPDATE	ps_gp_strm a
SET 	emplid_to = (
		SELECT	SUBSTR(emplid_from,1,LENGTH(emplid_from)-1)
		||	CHR(ASCII(SUBSTR(emplid_from,LENGTH(emplid_from),1))-1)
		||	'Z'
		FROM	ps_gp_strm b
		WHERE	b.strm_num = a.strm_num + 1)
WHERE	strm_num < (
	SELECT 	MAX(strm_num)
	FROM	ps_gp_strm)
;

UPDATE	ps_gp_strm
SET 	emplid_to = 'ZZZZZZZZZZZ'
WHERE	strm_num = (
	SELECT 	MAX(strm_num)
	FROM	ps_gp_strm)
;


SELECT * from ps_gp_strm
;

spool off



