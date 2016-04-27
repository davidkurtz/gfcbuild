REM aud-partdata.sql
REM 28.8.2009 partitioned AUDIT_TL_PAY_TM, AUDIT_PSOPRDEFN
REM  1.6.2012 added various audit tables to partition strategy
REM 13.6.2012 removed default audit definition
REM 19.7.2012 comment history added
--------------------------------------------------------------------------------------------------------------
--insert data to specify the tables to be partitioned
--------------------------------------------------------------------------------------------------------------
--Tablespaces?
-- 6 months	AUDMP6M
-- 1/2 yr   AUDM1Y2Y, AUDW1Y2Y, 
-- 3 year   AUDMA3Y
--------------------------------------------------------------------------------------------------------------
--AUD	      monthly partitioning, no archive/purge policy --not used any more.
--AUDW6M    weekly partition, archive at 6 months, no purge policy defined --also not used any more
--AUDMP6M   monthly partition, don't archive purge after 6 months
--AUDM3M1Y  monthly partition, archive after 3 months, purge after 1 year
--AUDM1Y2Y  monthly partition, archive after 1 year, purge after a 2nd year
--AUDW1Y2Y  weekly partition, archive after 1 year, purge after a 2nd year
--AUDMA3Y   monthly partition, archive after 3 years, never purge
--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_tables
WHERE part_id LIKE 'AUD%'
/

-- KH 13.06.2012 - There are no tables that now have default partitioning so the following section has been commented out.
--default archive PARTITIONING
--INSERT INTO gfc_part_tables
--(recname, part_id, part_column, part_type, arch_flag) 
--SELECT  r.recname, 'AUD'
--,	'AUDIT_STAMP', 'R', 'N'
--FROM    psrecdefn r
--WHERE   r.recname IN('AUDIT_TL_PAY_TM' --partitioned 28.8.2009, because volume increasing
--                    )
--/

--monthly partitions archive after 3 months, purge after 1 year
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag) 
SELECT  r.recname, 'AUDM3M1Y'
,	'AUDIT_STAMP', 'R', 'A'
FROM    psrecdefn r
WHERE   r.recname IN('AUDIT_AUTHITEM'  --Added by KH 01.06.2012
                    ,'AUDIT_CLASSDEFN' --Added by KH 01.06.2012
                    ,'AUDIT_PSOPRDEFN' --partitioned 28.8.2009, because volume increasing 
                    ,'AUDIT_ROLECLASS' 
                    ,'AUDIT_ROLEDEFN'  
                    ,'AUDIT_ROLEUSER'  
                    )
/

--weekly partitions just archive after 6 months
-- KH 01.06.2012 This has now been commented out, as we have had confirmation that the AUDIT_SCH_TBL table
-- can be purged after 6 months, so is being added to the AUDMP6M category
--INSERT INTO gfc_part_tables
--(recname, part_id, part_column, part_type, arch_flag) 
--SELECT  r.recname, 'AUDW6M'
--,	'AUDIT_STAMP', 'R', 'A'
--FROM    psrecdefn r
--WHERE   r.recname IN('AUDIT_SCH_TBL' --archive at 6 months, but need to determine purge
--                    )
--/

--monthly partitions just purge after 6 months
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag) 
SELECT  r.recname, 'AUDMP6M'
,	'AUDIT_STAMP', 'R', 'D'
FROM    psrecdefn r
WHERE   r.recname IN('AUDIT_COMPENSTN'
--                    ,'AUDIT_EE_STKHLD' --small
--                    ,'AUDIT_GPABSCMTS' --small
--                    ,'AUDIT_JPMJITEMS' --small
--                    ,'AUDIT_JPMPROFIL' --small
                    ,'AUDIT_NAMES'
                    ,'AUDIT_PERORGASG'   --Added by KH 01.06.2012
--                    ,'AUDIT_PERORGINS' --small
--                    ,'AUDIT_PERSDTAEF' --small
                    ,'AUDIT_PERSON'
                    ,'AUDIT_SCH_TBL'     --Added by KH 01.06.2012
--                    ,'AUDIT_WMS_PSDIR' --small
                    ,'AUDIT_WMSBSSCHD'
--                    ,'AUDIT_WMSECLWIT' --small
                    )
/

--monthly partitions archive after 1 year, purge after 2
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag) 
SELECT  r.recname, 'AUDM1Y2Y'
,	'AUDIT_STAMP', 'R', 'A'
FROM    psrecdefn r
WHERE   r.recname IN(
--                     'AUDIT_ABS_HIST' --small
--                    ,'AUDIT_ABSHISTDT' --small
--                    ,'AUDIT_BENPRGPTC' --small (small comment addded by KH 01.06.2012)
--                    ,'AUDIT_CAR_PLAN' --small
                     'AUDIT_GPABSEVNT' --originally partitioned 21.8.2009, for benefit of WMS_APPAY_01
                    ,'AUDIT_GPABSEVTJ' --originally partitioned 28.8.2009, because volume increasing
                    ,'AUDIT_GPACMUSER'
--                    ,'AUDIT_GPEECODTL' --small
--                    ,'AUDIT_GPEECOHDR' --small
--                    ,'AUDIT_GPEELOAND' --small
--                    ,'AUDIT_GPEELOANH' --small
                    ,'AUDIT_GPEENI'
--                    ,'AUDIT_GPEESS' --small
--                    ,'AUDIT_GPEESSHOL' --small
--                    ,'AUDIT_GPEESTLOA' --small
                    ,'AUDIT_GPEETAX'
--                    ,'AUDIT_GPGLDSDTL' --small
--                    ,'AUDIT_GPGLDSPIN' --small
--                    ,'AUDIT_GPNETDIST' --small
                    ,'AUDIT_GPNTDSTDL'
                    ,'AUDIT_GPNTDSTDT'  -- added by KH 01.06.2012
                    ,'AUDIT_GPPIMNLDT'
                    ,'AUDIT_GPPYEOVRD'
                    ,'AUDIT_GPPYESOVR'
                    ,'AUDIT_GPPYOVSOV'
--                    ,'AUDIT_GPRCPPYDT' --small
                    ,'AUDIT_GPRSLTACM'
--                    ,'AUDIT_WMSGIBESI' --small
--                    ,'AUDIT_WMSGIBETX' --small
                    ,'PSAUDIT')
/

--weekly partitions archive after 1 year, purge after 2
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag) 
SELECT  r.recname, 'AUDW1Y2Y'
,	'AUDIT_STAMP', 'R', 'A'
FROM    psrecdefn r
WHERE   r.recname IN('AUDIT_TLRPTTIME' --previously weekly partitioned
                    ,'AUDIT_TL_PAY_TM' -- added by KH 13.06.2012 - previously weekly partitioned
                    )
/

--monthly partitions in monthly tablespaces archive after 3 years and retain
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag) 
SELECT  r.recname, 'AUDMA3Y'
,	'AUDIT_STAMP', 'R', 'A'
FROM    psrecdefn r
WHERE   r.recname IN('AUDIT_JOB'
--                  ,'AUDIT_PERS_NID' --small
                    )
/

-----------------------------------------------------------------------------------------------------------
--apply rebuild filter in line with purge policy to table rebuild definition so that if we every rebuild the
--table we do not rebuild data that we would purge anyway
-----------------------------------------------------------------------------------------------------------
UPDATE gfc_part_tables
SET    tab_tablespace = 'AUD6MTAB'
,	 idx_tablespace = 'AUD6MIDX' 
,	 criteria = 'WHERE audit_stamp >= TRUNC(ADD_MONTHS(SYSDATE,-6),''MM'')'
WHERE  part_id IN('AUDMP6M')
/
UPDATE gfc_part_tables
SET    tab_tablespace = 'AUD1YTAB'
,	 idx_tablespace = 'AUD1YIDX' 
,	 criteria = 'WHERE audit_stamp >= TRUNC(ADD_MONTHS(SYSDATE,-12),''MM'')'
WHERE  part_id IN('AUDM3M1Y')
/
UPDATE gfc_part_tables
SET    tab_tablespace = 'AUD2YTAB'
,	 idx_tablespace = 'AUD2YIDX' 
,	 criteria = 'WHERE audit_stamp >= TRUNC(ADD_MONTHS(SYSDATE,-24),''MM'')'
WHERE  part_id IN('AUDM1Y2Y','AUDW1Y2Y')
/
UPDATE gfc_part_tables
SET    tab_tablespace = 'AUD_TAB'
,	 idx_tablespace = 'AUD_IDX' 
,	 criteria = ''
WHERE  part_id IN('AUDMA3Y')
/

-----------------------------------------------------------------------------------------------------------
--set archive schema and storage options for archive tables
--note we are not setting default tablespaces
-----------------------------------------------------------------------------------------------------------
UPDATE gfc_part_tables x
SET	 x.arch_schema    = CASE x.arch_flag WHEN 'A' THEN 'PSARCH' 
                                           WHEN 'D' THEN 'PSARCH' 
                                           ELSE '' END
--,      x.tab_tablespace = (
-- 		SELECT y.ddlspacename 
-- 		FROM   psrectblspc y 
-- 		WHERE  y.recname = x.recname
--		AND    y.dbtype = (
--			SELECT MAX (y1.dbtype)
--			FROM   psrectblspc y1
--			WHERE  y1.recname = x.recname
--			AND    y1.dbtype IN(' ','2'))) /*default PSFT tablespace*/
--,	 x.idx_tablespace = 'PSINDEX' /*default PSFT tablespace*/
,	 x.tab_storage    = 'PCTUSED 99 PCTFREE 0'
,	 x.idx_storage    = 'PCTFREE 0 COMPRESS 1'
WHERE	 x.part_id LIKE 'AUD%'
/

--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategry
--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_ranges
WHERE part_id LIKE 'AUD%'
/

--INSERT INTO gfc_part_ranges 
--(         part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,	  tab_storage,	arch_flag)
--SELECT    y.part_id, y.part_no, y.part_name, y.part_value, t.tablespace_name, i.tablespace_name,  y.tab_storage,	y.arch_flag
--FROM	(
--	SELECT	'AUD' part_id
--	, 	TO_NUMBER(TO_CHAR(mydate,'YYMM')) part_no
--	,	TO_CHAR(mydate,'YYMM') part_name
--	,	'TO_DATE('''||TO_CHAR(MAX(mydate)+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
--	,	'AUD'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'TAB' tab_tablespace
--	,	'AUD'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'IDX' idx_tablespace
--	,	CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
--	,	'N' arch_flag --no archiving or purge
--	FROM	(
--		SELECT	a.from_dt+b.n mydate
--		FROM	(
--			SELECT TO_DATE('20081011','YYYYMMDD') from_dt
--			FROM dual 
--			) a
--		,	(
--			SELECT rownum n
--			FROM dual
--			CONNECT BY level <= (TO_DATE('20130501','YYYYMMDD')-TO_DATE('20081001','YYYYMMDD'))
--			) b
--		) x
--	WHERE mydate >= TO_DATE('20081107','YYYYMMDD')
--	GROUP BY TO_CHAR(mydate,'YYMM')
--	HAVING MIN(mydate) < TO_DATE('20130501','YYYYMMDD') 
--	) y
--	LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
--	LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
--ORDER BY 1,2,3
--/

--just archive after 6 months and never purge
--INSERT INTO gfc_part_ranges 
--(         part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,	  tab_storage,	arch_flag)
--SELECT    y.part_id, y.part_no, y.part_name, y.part_value, t.tablespace_name, i.tablespace_name,  y.tab_storage,	y.arch_flag
--FROM	(
--	SELECT 	'AUDW6M' part_id
--	, 	TO_NUMBER(TO_CHAR(mydate,'iyIW')) part_no
--	,	TO_CHAR(mydate,'iyIW') part_name
--	,	'TO_DATE('''||TO_CHAR(MAX(mydate)+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
--	,	CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'AUD6MTAB'
--	      ELSE 'AUD'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'TAB' 
--		END tab_tablespace
--	,	CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'AUD6MIDX'
-- 		     ELSE 'AUD'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'IDX' 
--		END idx_tablespace
--	,	CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
--	,	CASE WHEN MAX(mydate)+1<ADD_MONTHS(SYSDATE,-6) THEN 'A' ELSE 'N' END arch_flag --archiving
--	FROM	(
--		SELECT	a.from_dt+b.n mydate
--		FROM	(
--			SELECT TO_DATE('20081011','YYYYMMDD') from_dt
--			FROM dual 
--			) a
--		,	(
--			SELECT rownum n
--			FROM dual
--			CONNECT BY level <= (TO_DATE('20130501','YYYYMMDD')-TO_DATE('20081001','YYYYMMDD'))
--			) b
--		) x
--	WHERE mydate >= TO_DATE('20081107','YYYYMMDD')
--	GROUP BY TO_CHAR(mydate,'iyIW')
--	HAVING MIN(mydate) < TO_DATE('20130501','YYYYMMDD') 
--	) y
--	LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
--	LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
--ORDER BY 1,2,3
--/

--just purge after 6 months
INSERT INTO gfc_part_ranges 
(         part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,	 tab_storage,   arch_flag)
SELECT    y.part_id, y.part_no, y.part_name, y.part_value, t.tablespace_name, i.tablespace_name, y.tab_storage, y.arch_flag
FROM	(
	SELECT	'AUDMP6M' part_id
	, 	TO_NUMBER(TO_CHAR(mydate,'YYMM')) part_no
	,	TO_CHAR(mydate,'YYMM') part_name
	,	'TO_DATE('''||TO_CHAR(MAX(mydate)+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
	,	'AUD6MTAB' tab_tablespace
	,	'AUD6MIDX' idx_tablespace
	,	CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
	,	CASE WHEN MAX(mydate)+1<ADD_MONTHS(SYSDATE,-6) THEN 'D' ELSE 'N' END arch_flag --purge
	FROM	(
		SELECT	a.from_dt+b.n mydate
		FROM	(
			SELECT TO_DATE('20081011','YYYYMMDD') from_dt
			FROM dual 
			) a
		,	(
			SELECT rownum n
			FROM dual
			CONNECT BY level <= (TO_DATE('20130501','YYYYMMDD')-TO_DATE('20081001','YYYYMMDD'))
			) b
		) x
	WHERE mydate >= TO_DATE('20081107','YYYYMMDD')
	GROUP BY TO_CHAR(mydate,'YYMM')
	HAVING MIN(mydate) < TO_DATE('20130501','YYYYMMDD') 
	) y
	LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
	LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3
/

--monthly partitions - archive after 3 months, purge after 1 year
INSERT INTO gfc_part_ranges 
(         part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,	  tab_storage,	arch_flag)
SELECT    y.part_id, y.part_no, y.part_name, y.part_value, t.tablespace_name, i.tablespace_name,  y.tab_storage,	y.arch_flag
FROM	(
	SELECT	'AUDM3M1Y' part_id
	, 	TO_NUMBER(TO_CHAR(mydate,'YYMM')) part_no
	,	TO_CHAR(mydate,'YYMM') part_name
	,	'TO_DATE('''||TO_CHAR(MAX(mydate)+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
	,	'AUD1YTAB' tab_tablespace
	,	'AUD1YIDX' idx_tablespace
	,	CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
	,	CASE WHEN MAX(mydate)+1<ADD_MONTHS(SYSDATE,-12) THEN 'D' --note sequence of condititons
                     WHEN MAX(mydate)+1<ADD_MONTHS(SYSDATE,-3) THEN 'A' 
                     ELSE 'N' END arch_flag --purge
	FROM	(
		SELECT	a.from_dt+b.n mydate
		FROM	(
			SELECT TO_DATE('20081011','YYYYMMDD') from_dt
			FROM dual 
			) a
		,	(
			SELECT rownum n
			FROM dual
			CONNECT BY level <= (TO_DATE('20130501','YYYYMMDD')-TO_DATE('20081001','YYYYMMDD'))
			) b
		) x
	WHERE mydate >= TO_DATE('20081107','YYYYMMDD')
	GROUP BY TO_CHAR(mydate,'YYMM')
	HAVING MIN(mydate) < TO_DATE('20130501','YYYYMMDD') 
	) y
	LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
	LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3
/

--monthly partitions - archive after 1 year, purge after 2 year
INSERT INTO gfc_part_ranges 
(         part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,	  tab_storage,	arch_flag)
SELECT    y.part_id, y.part_no, y.part_name, y.part_value, t.tablespace_name, i.tablespace_name,  y.tab_storage,	y.arch_flag
FROM	(
	SELECT	'AUDM1Y2Y' part_id
	, 	TO_NUMBER(TO_CHAR(mydate,'YYMM')) part_no
	,	TO_CHAR(mydate,'YYMM') part_name
	,	'TO_DATE('''||TO_CHAR(MAX(mydate)+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
	,	'AUD2YTAB' tab_tablespace
	,	'AUD2YIDX' idx_tablespace
	,	CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
	,	CASE WHEN MAX(mydate)+1<ADD_MONTHS(SYSDATE,-24) THEN 'D' --note sequence of condititons
                     WHEN MAX(mydate)+1<ADD_MONTHS(SYSDATE,-12) THEN 'A' 
                     ELSE 'N' END arch_flag --purge
	FROM	(
		SELECT	a.from_dt+b.n mydate
		FROM	(
			SELECT TO_DATE('20081011','YYYYMMDD') from_dt
			FROM dual 
			) a
		,	(
			SELECT rownum n
			FROM dual
			CONNECT BY level <= (TO_DATE('20130501','YYYYMMDD')-TO_DATE('20081001','YYYYMMDD'))
			) b
		) x
	WHERE mydate >= TO_DATE('20081107','YYYYMMDD')
	GROUP BY TO_CHAR(mydate,'YYMM')
	HAVING MIN(mydate) < TO_DATE('20130501','YYYYMMDD') 
	) y
	LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
	LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3
/

--weekly partitions - archive after 1 year, purge after 2 year
INSERT INTO gfc_part_ranges 
(         part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,	  tab_storage,	arch_flag)
SELECT    y.part_id, y.part_no, y.part_name, y.part_value, t.tablespace_name, i.tablespace_name,  y.tab_storage,	y.arch_flag
FROM	(
	SELECT	'AUDW1Y2Y' part_id
	, 	TO_NUMBER(TO_CHAR(mydate,'iyIW')) part_no
	,	TO_CHAR(mydate,'iyIW') part_name
	,	'TO_DATE('''||TO_CHAR(MAX(mydate)+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
	,	'AUD2YTAB' tab_tablespace
	,	'AUD2YIDX' idx_tablespace
	,	CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
	,	CASE WHEN MAX(mydate)+1<ADD_MONTHS(SYSDATE,-24) THEN 'D' --note sequence of condititons
                     WHEN MAX(mydate)+1<ADD_MONTHS(SYSDATE,-12) THEN 'A' 
                     ELSE 'N' END arch_flag --purge
	FROM	(
		SELECT	a.from_dt+b.n mydate
		FROM	(
			SELECT TO_DATE('20081011','YYYYMMDD') from_dt
			FROM dual 
			) a
		,	(
			SELECT rownum n
			FROM dual
			CONNECT BY level <= (TO_DATE('20130501','YYYYMMDD')-TO_DATE('20081001','YYYYMMDD'))
			) b
		) x
	WHERE mydate >= TO_DATE('20081107','YYYYMMDD')
	GROUP BY TO_CHAR(mydate,'iyIW')
	HAVING MIN(mydate) < TO_DATE('20130501','YYYYMMDD') 
	) y
	LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
	LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3
/

--monthly partitions, archive after 3 years, and retain
INSERT INTO gfc_part_ranges 
(         part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,	  tab_storage,	arch_flag)
SELECT    y.part_id, y.part_no, y.part_name, y.part_value, t.tablespace_name, i.tablespace_name,  y.tab_storage,	y.arch_flag
FROM	(
	SELECT	'AUDMA3Y' part_id
	, 	TO_NUMBER(TO_CHAR(mydate,'YYMM')) part_no
	,	TO_CHAR(mydate,'YYMM') part_name
	,	'TO_DATE('''||TO_CHAR(MAX(mydate)+1,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
	,	'AUD_TAB' tab_tablespace
	,	'AUD_IDX' idx_tablespace
	,	CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
	,	CASE WHEN MAX(mydate)+1<ADD_MONTHS(SYSDATE,-36) THEN 'A' 
                 ELSE 'N' END arch_flag --purge
	FROM	(
		SELECT	a.from_dt+b.n mydate
		FROM	(
			SELECT TO_DATE('20081011','YYYYMMDD') from_dt
			FROM dual 
			) a
		,	(
			SELECT rownum n
			FROM dual
			CONNECT BY level <= (TO_DATE('20130501','YYYYMMDD')-TO_DATE('20081001','YYYYMMDD'))
			) b
		) x
	WHERE mydate >= TO_DATE('20081107','YYYYMMDD')
	GROUP BY TO_CHAR(mydate,'YYMM')
	HAVING MIN(mydate) < TO_DATE('20130501','YYYYMMDD') 
	) y
	LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
	LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3
/

-----------------------------------------------------------------------------------------------------------
--archive/purge partitions should be compressed if built
-----------------------------------------------------------------------------------------------------------
UPDATE gfc_part_ranges
SET    tab_storage = 'COMPRESS'
WHERE  part_id LIKE 'AUD%'
AND	 arch_flag IN('A','D')
AND    tab_storage IS NULL
/
UPDATE gfc_part_ranges
SET    tab_storage = tab_storage||' COMPRESS'
WHERE  part_id LIKE 'AUD%'
AND	 arch_flag IN('A','D')
AND    NOT UPPER(tab_storage) LIKE '%COMPRESS%'
AND    tab_storage IS NOT NULL
/
-----------------------------------------------------------------------------------------------------------
--End Archiving Metadata
-----------------------------------------------------------------------------------------------------------

column Mb format 999,990.0
ttitle 'Small Audit Tables (Stats)'
SELECT p.part_id, t.table_name, t.num_rows, t.partitioned
FROM	gfc_part_tables p
,	psrecdefn r
,	user_tables t
WHERE	p.part_id like 'AUD%'
AND	r.recname = p.recname
AND	t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
--and 	t.num_rows < 100000
ORDER BY num_rows
/
break on owner skip 1
ttitle 'Audit Size by Partitioning Strategy (Stats)'
SELECT t.owner, p.part_id, SUM(t.blocks)*8/1024 Mb
FROM	sysadm.gfc_part_tables p
,	sysadm.psrecdefn r
,	dba_tables t
WHERE	r.recname = p.recname
AND	t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
AND	p.part_id like 'AUD%'
GROUP BY t.owner, p.part_id
ORDER BY owner, part_id
/


break on owner skip 1
ttitle 'Audit Size by Partitioning Strategy (Segments)'
SELECT p.part_id, SUM(t.blocks)*8/1024 Mb
FROM	sysadm.gfc_part_tables p
,	sysadm.psrecdefn r
,	dba_segments t
WHERE	r.recname = p.recname
AND	t.segment_name LIKE DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
AND	p.part_id like 'AUD%'
GROUP BY p.part_id
ORDER BY part_id
/
break on part_id
SELECT p.part_id, t.segment_type, SUM(t.blocks)*8/1024 Mb
FROM	sysadm.gfc_part_tables p
,	sysadm.psrecdefn r
,	dba_segments t
WHERE	r.recname = p.recname
AND	t.segment_name LIKE DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
AND	p.part_id like 'AUD%'
GROUP BY p.part_id, t.segment_type
ORDER BY substr(part_id,-2), part_id
/
break on owner skip 1
SELECT t.owner, p.part_id, SUM(t.blocks)*8/1024 Mb
FROM	sysadm.gfc_part_tables p
,	sysadm.psrecdefn r
,	dba_segments t
WHERE	r.recname = p.recname
AND	t.segment_name LIKE DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
AND	p.part_id like 'AUD%'
GROUP BY t.owner, p.part_id
ORDER BY owner, part_id
/
ttitle off

DELETE FROM gfc_part_tables p
WHERE	p.part_id like 'AUD%'
AND EXISTS (
	SELECT 'x'
	FROM 	psrecdefn r
	,	user_tables t
	WHERE	r.recname = p.recname
	AND	t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
	AND	t.num_rows < 100000)
AND 1=2
/


