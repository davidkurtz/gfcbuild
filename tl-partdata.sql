xREM tl-partdata.sql
REM  3.6.2009 added SCH_DEFN_SHFT and TL_DSTOFFSET to list of partitioned TL tables
REM 29.7.2010 added WMS_TL_EXCEPNS and WMS_SUP_EXCP and hash sub-partitioned
REM  4.5.2011 Remove partitioning from index PSETL_RPTD_TIME
REM 28.7.2011 SCH_ASSIGN added to list of partitioned tables
REM  6.7.2012 comression attributes set for TL and SCH range partitions that are 6 months only
REM 14.7.2012 partitioning of TL_CALENDAR now driven directly from table but limited to start with 2008
REM 19.7.2012 comment history added
--------------------------------------------------------------------------------------------------------------
--insert data to describe temporary tables
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--insert data to generate the function based indexed
--------------------------------------------------------------------------------------------------------------
--INSERT INTO gfc_ps_idxddlparm (RECNAME,INDEXID,PARMNAME,PARMVALUE)

INSERT INTO gfc_ps_indexdefn (recname, indexid, subrecname, subindexid, platform_ora, custkeyorder, uniqueflag)
VALUES ('TL_RPTD_TIME','Z','TL_RPTD_TIME','Z', 1, 0, 0);

--(recname,indexid,keyposn,fieldname)
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_RPTD_TIME','Z',1,'EMPLID',1);
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_RPTD_TIME','Z',2,'EMPL_RCD',1);
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_RPTD_TIME','Z',3,'TO_DATE(TO_CHAR(DUR,''YYYY-MM-DD-'')||TO_CHAR(PUNCH_TIME,''HH24.MI.SS''),''YYYY-MM-DD-HH24.MI.SS'')',1);
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_RPTD_TIME','Z',4,'PUNCH_TYPE',1);
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_RPTD_TIME','Z',5,'REPORTED_STATUS',1);


--------------------------------------------------------------------------------------------------------------
--insert data to specify the tables to be partitioned
--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_tables
WHERE part_id IN('TL','SCH','TLTZ')
/

INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'SCH'
,	'DUR', 'R'
FROM    psrecdefn r
WHERE   r.recname IN('SCH_MNG_SCH_TBL'
                    ,'SCH_ADHOC_DTL'
                    )
/

INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'SCH'
,	'EFFDT', 'R'
FROM    psrecdefn r
WHERE   r.recname IN('SCH_DEFN_DTL'
                    ,'SCH_DEFN_SHFT' --added 3.6.2009 - for WMS_SBMT_SH-WS
                    ,'SCH_DEFN_TBL'
		    ,'SCH_ASSIGN' --added 28.7.2011 - for archiving
                    ,'SCH_DEFN_ROTATN')
/

INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'TL'
,	'DUR', 'R'
FROM    psrecdefn r
WHERE   r.recname IN('TL_FRCS_PYBL_TM' --added 3.6.2009 - for WMS_SBMT_SH-BE
                    ,'TL_RPTD_TIME'
                    ,'TL_EXCEPTION'    --added 29.7.2010 - for WMS_SUP_EXCP
                    )
/

UPDATE 	gfc_part_tables x
SET	x.tab_storage    = 'PCTUSED 90 PCTFREE 10'
,	subpart_type = 'H'
,	subpart_column = 'EMPLID'
,	hash_partitions = 16
WHERE	x.part_id = 'TL'
AND   x.recname IN('TL_RPTD_TIME'
                    ,'TL_EXCEPTION' --added 29.7.2010 - for WMS_SUP_EXCP
                    ,'WMS_TL_EXCEPNS' --added 29.7.2010 - for WMS_SUP_EXCP
                    )
/

UPDATE 	gfc_part_tables x
SET	x.tab_tablespace = (SELECT y.ddlspacename FROM psrectblspc y WHERE y.recname = x.recname) /*default PSFT tablespace*/
,	x.idx_tablespace = 'PSINDEX' /*default PSFT tablespace*/
,	x.tab_storage    = 'PCTUSED 90 PCTFREE 1'
,	x.idx_storage    = 'PCTFREE 0'
WHERE	x.part_id IN('TL','SCH')
/

UPDATE 	gfc_part_tables x
SET	x.tab_storage    = 'INITRANS 4 '||x.tab_storage
,	x.idx_storage    = 'INITRANS 4 '||x.idx_storage
WHERE	x.recname IN('SCH_ADHOC_DTL')
/

INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'TLTZ'
,	'TIMEZONE', 'L'
FROM    psrecdefn r
WHERE   r.recname IN('TL_DSTOFFSET') --added 3.6.2009
/


--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_tables
WHERE part_id IN('N')
/
INSERT INTO gfc_part_tables
(recname, part_id, part_type, part_column, idx_storage) 
VALUES
('TL_TR_STATUS','N','N','EMPLID','COMPRESS 1')
/

--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------

INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_RPTD_TIME','0','TL','N',' ')
/
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_RPTD_TIME','A','TL','N',' ')
/
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_RPTD_TIME','C','TL','N',' ')
/
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_RPTD_TIME','D','TL','N',' ')
/
--2009.11.26 this index is going back to local partition - its not ideal, but there are some hints required first
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, hash_partitions, idx_storage)
--VALUES ('TL_RPTD_TIME','E','TL','H','EMPLID', 16, 'COMPRESS 1') --2010.05.20 --WMS_ST_LOAD Step02e hinted to force this
VALUES ('TL_RPTD_TIME','E','TL','N',' ', NULL, 'COMPRESS 1') --2011.05.04 --remove hash subpartitioning
/
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_RPTD_TIME','F','TL','N',' ')
/

--2011.08.29 - global non-partitioned index, though considering other options.
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('SCH_DEFN_DTL','_','SCH','N',' ','PCTFREE 1 COMPRESS 4')
/

--2011.09.16 - global non-partitioned unique index
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('SCH_ASSIGN','_','SCH','N',' ','PCTFREE 1 COMPRESS 2')
/

INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_EXCEPTION','C','TL','R','ACTION_DTTM')
/

--2011.10.9 - globally partition unique index on TL_TR_STATUS
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, hash_partitions, idx_storage)
VALUES ('TL_TR_STATUS','_','N','H','EMPLID', 16, 'COMPRESS 1')
/

--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategry
--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_ranges
WHERE part_id IN('TL','SCH')
/

INSERT INTO gfc_part_ranges 
(        part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,    tab_storage,   arch_flag)
SELECT y.part_id, y.part_no, y.part_name, y.part_value, t.tablespace_name, i.tablespace_name, y.tab_storage, y.arch_flag
FROM	(
	SELECT	'SCH' part_id
	, 	TO_NUMBER(TO_CHAR(mydate,'iyIW')) part_no
	,	TO_CHAR(mydate,'iyIW') part_name
	,	'TO_DATE('''||TO_CHAR(MAX(mydate)+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
	,	'TL'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'TAB' tab_tablespace
	,	'TL'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'IDX' idx_tablespace
	,	CASE WHEN MAX(mydate)<TRUNC(ADD_MONTHS(SYSDATE,-6),'IW') THEN 'PCTFREE 0 PCTUSED 99 COMPRESS' ELSE 'PCTFREE 10' END tab_storage --compress after 6 months --added 6.7.2012
	,	CASE WHEN MAX(mydate)<TRUNC(SYSDATE-(14*28),'IW') THEN 'A' ELSE 'N' END arch_flag --archiving
	FROM	(
		SELECT	a.from_dt+b.n mydate
		from	(
			select TO_DATE('20081107','YYYYMMDD') from_dt
			from dual
			) a
		,	(
			select rownum n
			from dual
			connect by level <= (TO_DATE('20130501','yyyymmdd')-TO_DATE('20081107','YYYYMMDD'))
			) b
		) x
	WHERE mydate >= TO_DATE('20081107','yyyymmdd')
	GROUP BY TO_CHAR(mydate,'iyIW')
	HAVING MIN(mydate) < TO_DATE('20130501','yyyymmdd')
	) y
	left outer join dba_tablespaces t on t.tablespace_name = y.tab_tablespace
	left outer join dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3
/
INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value)
VALUES
('SCH', 9999, 9999, 'MAXVALUE')
/



INSERT INTO gfc_part_ranges 
(        part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,    tab_storage,   arch_flag)
SELECT y.part_id, y.part_no, y.part_name, y.part_value, t.tablespace_name, i.tablespace_name, y.tab_storage, y.arch_flag
FROM	(
	SELECT	'TL' part_id
	, 	TO_NUMBER(TO_CHAR(mydate,'iyIW')) part_no
	,	TO_CHAR(mydate,'iyIW') part_name
	,	'TO_DATE('''||TO_CHAR(MAX(mydate)+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
	,	'TL'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'TAB' tab_tablespace
	,	'TL'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'IDX' idx_tablespace
	,	CASE WHEN MAX(mydate)<TRUNC(ADD_MONTHS(SYSDATE,-6),'IW') THEN 'PCTFREE 0 PCTUSED 99 COMPRESS' ELSE 'PCTFREE 10' END tab_storage --compress after 6 months --added 6.7.2012
	,	'N' arch_flag --archiving
	FROM	(
		SELECT	a.from_dt+b.n mydate
		from	(
			select TO_DATE('20081107','YYYYMMDD') from_dt
			from dual
			) a
		,	(
			select rownum n
			from dual
			connect by level <= (TO_DATE('20130501','yyyymmdd')-TO_DATE('20081107','YYYYMMDD'))
			) b
		) x
	WHERE mydate >= TO_DATE('20081107','yyyymmdd')
	GROUP BY TO_CHAR(mydate,'iyIW')
	HAVING MIN(mydate) < TO_DATE('20130501','yyyymmdd')
	) y
	left outer join dba_tablespaces t on t.tablespace_name = y.tab_tablespace
	left outer join dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3
/
INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value)
VALUES
('TL', 9999, 9999, 'MAXVALUE')
/

--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_ranges
WHERE part_id IN('TLX')
/
INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 1  , 'CENTR' ,'''CENTRz'',MAXVALUE');
INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 2  , 'DISTR' ,'''DISTRz'',MAXVALUE');
INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 3  , 'MANUF' ,'''MANUFz'',MAXVALUE');
INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 4  , 'SHARE' ,'''SHAREz'',MAXVALUE');
INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 5.2, 'STORE2','''STOREz'',''3''');
INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 999, 'STORE3','MAXVALUE,MAXVALUE');


-----------------------------------------------------------------------------------------------------------
--insert data to list partitions
-----------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_lists
WHERE part_id = 'TL'
/

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
SELECT	'TL'
,	rownum
, 	LTRIM(TO_CHAR(rownum,'0'))
, 	''''||LTRIM(TO_CHAR(rownum,'0'))||''''
FROM dual			
CONNECT BY level <= 7
ORDER BY 1,2,3
/

UPDATE 	gfc_part_lists
SET 	tab_tablespace = 'TL'||part_name||'TAB'
,   	idx_tablespace = 'TL'||part_name||'IDX'
--,	tab_storage = '/*TAB STORAGE*/'
--,	idx_storage = '/*IDX STORAGE*/'
WHERE 	part_id = 'TL'
AND 1=2
/

-----------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_lists
WHERE part_id = 'TLTZ'
/
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
SELECT	'TLTZ'
,	rownum
, 	timezone
, 	''''||timezone||''''
FROM    pstimezone
WHERE 	timezone IN('GMT','WEST')
ORDER BY 1,2,3
/
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
VALUES ('TLTZ',9999,'Z_OTHERS','DEFAULT')
;



----------------------------------------------------------------------------------------------------------- 
--mapping between ranges and lists 
----------------------------------------------------------------------------------------------------------- 
DELETE FROM gfc_part_range_lists 
WHERE part_id = 'TL' 
/ 

INSERT INTO gfc_part_range_Lists 
(part_id, range_name, list_name) 
SELECT r.part_id, r.part_name, l.part_name 
FROM   gfc_part_ranges r 
,      gfc_part_lists l 
WHERE  l.part_id = r.part_id 
AND    l.part_id = 'TL'
/ 

-----------------------------------------------------------------------------------------------------------
--delete range/list combinations that are not needed
-----------------------------------------------------------------------------------------------------------



-----------------------------------------------------------------------------------------------------------
--Archiving Metadata
-----------------------------------------------------------------------------------------------------------
UPDATE 	gfc_part_tables
SET     arch_flag = 'A'
,	arch_schema = 'PSARCH'
WHERE 	part_id IN('SCH')
AND 1=2
/

--schedule tables will be purged not archived.
UPDATE 	gfc_part_tables
SET     arch_flag = 'D'
,	arch_schema = 'SYSADM'
WHERE 	part_id IN('SCH')
/
UPDATE 	gfc_part_ranges
SET     arch_flag = 'D'
WHERE 	part_id IN('SCH')
and	arch_flag = 'A'
/
UPDATE 	gfc_part_tables
SET 	noarch_condition = 'end_effdt >= TRUNC(SYSDATE-(14*28),''IW'') AND effdt < TRUNC(SYSDATE-(14*28),''IW'')'
WHERE 	recname IN('SCH_DEFN_TBL','SCH_ASSIGN')
/
UPDATE 	gfc_part_tables
SET 	noarch_condition = ''
WHERE 	recname IN('SCH_ADHOC_DTL','SCH_MNG_SCH_TBL')
/
UPDATE 	gfc_part_tables
SET 		noarch_condition = 'EXISTS(SELECT 1 FROM sysadm.ps_sch_defn_tbl y WHERE x.setid = y.setid AND x.sch_adhoc_ind = y.sch_adhoc_ind '
				||'AND x.schedule_id = y.schedule_id AND x.effdt = y.effdt '
				||'AND y.end_effdt >= TRUNC(SYSDATE-(14*28),''IW'') '
				||'AND y.effdt < TRUNC(SYSDATE-(14*28),''IW'')) ' --added 7.10.11 to eliminate partitions
WHERE 	recname IN('SCH_DEFN_SHFT' ,'SCH_DEFN_ROTATN', 'SCH_DEFN_DTL')
/

--see also statement that populates gfc_part_ranges

-----------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_tables
WHERE part_id IN('TLCAL')
/

INSERT INTO gfc_part_tables
(recname, part_id, part_type, part_column, subpart_type, subpart_column, tab_tablespace, idx_tablespace) 
SELECT  r.recname, 'TLCAL'
,	'R', 'START_DT'
,	'L', 'PERIOD_ID'
,	'TLAPP','PSINDEX'
FROM    psrecdefn r
WHERE   r.recname IN('TL_CALENDAR')
/


DELETE FROM gfc_part_lists
WHERE part_id = 'TLCAL'
/
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value, idx_storage)
SELECT 'TLCAL', rownum, part_name, list_value, '**PCTFREE**'
FROM (
SELECT	DISTINCT 
 	TRANSLATE(period_id,'-','_') part_name
, 	''''||period_id||'''' list_value
FROM    ps_tl_calendar
ORDER BY 1
)
/
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
VALUES ('TLCAL',9999,'Z_OTHERS','DEFAULT')
;

DELETE FROM gfc_part_ranges 
WHERE part_id = 'TLCAL'
;


--14.7.2012 commented out
--INSERT INTO gfc_part_ranges 
--(part_id, part_no, part_name, part_value)
--SELECT	y.part_id, y.part_no, y.part_name, y.part_value
--FROM	(
--	SELECT	'TLCAL' part_id
--	, 	TO_NUMBER(TO_CHAR(mydate,'yyyy')) part_no
--	,	TO_CHAR(mydate,'yyyy') part_name
--	,	'TO_DATE('''||TO_CHAR(mydate+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
--	FROM	(
--		SELECT	ADD_MONTHS(TO_DATE('20080101','YYYYMMDD'),12*rownum)-1 mydate
--		FROM	dual			
--		CONNECT BY level <= 20
--		) 
--	WHERE mydate BETWEEN TO_DATE('20070209','yyyymmdd') AND TO_DATE('20201231','yyyymmdd')
--	) y
--/


--14.7.2012 partitioning now driven directly from ps_tl_calendar but starting with 2008
INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value)
SELECT	y.part_id, y.part_no, y.part_name, y.part_value
FROM	(
	SELECT	'TLCAL' part_id
	, 	TO_NUMBER(TO_CHAR(mydate,'yyyy')) part_no
	,	TO_CHAR(mydate,'yyyy') part_name
	,	'TO_DATE('''||TO_CHAR(mydate+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
	FROM	(
		SELECT 	DISTINCT ADD_MONTHS(TRUNC(START_DT,'YYYY'),12)-1 mydate
		FROM		ps_tl_calendar
		WHERE		start_dt >= TO_DATE('20080101','yyyymmdd')
		) 
	) y
/


DELETE FROM gfc_part_range_lists 
WHERE part_id = 'TLCAL' 
/ 

INSERT INTO gfc_part_range_Lists 
(part_id, range_name, list_name) 
SELECT r.part_id, r.part_name, l.part_name 
FROM   gfc_part_ranges r 
,      gfc_part_lists l 
WHERE  l.part_id = r.part_id 
AND    l.part_id = 'TLCAL'
/ 

