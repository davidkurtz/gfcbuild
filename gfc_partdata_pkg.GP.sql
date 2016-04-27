--------------------------------------------------------------------------------------
--
-- script        : gfc_partdata_pkg.sql
--
-- created by    : DMK
-- creation date : 25.07.2012
--
-- description   : Reload partitioning/archiving metadata
--                 package PL/SQL procedure (gfc_partdata) supercedes simple *-partdata.sql Scripts
--
-- dependencies:  package procedure psftapi
--
-- development & maintenance history
--
--------------------------------------------------------------------------------------
-- HISTORY BLOCK MOVED TO PACKAGE HEADER SO IT CAN BE SEEN IN PACAKGE SOURCE
--------------------------------------------------------------------------------------
set echo on
spool gfc_partdata_pkg
--------------------------------------------------------------------------------------------------------------
--gp_partdata procedure uses this view exists to determine which records (that correspond to tables) to process.  
--For country specific GP records (matched by the beginning of the record name) It returns the GP installation 
--flag for the country
----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gfc_installed_gp AS
SELECT         recname,
        rectype, 
        CASE SUBSTR(r.recname,1,5) 
             WHEN 'GPAU_' THEN i.INSTALLED_GP_AUS
             WHEN 'GPAR_' THEN i.INSTALLED_GP_ARG
             WHEN 'GPBR_' THEN i.INSTALLED_GP_BRA
             WHEN 'GPCA_' THEN i.INSTALLED_GP_CAN
             WHEN 'GPCH_' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHA' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHS' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHT' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHU' THEN i.INSTALLED_GP_CHE
             WHEN 'GPDE_' THEN i.INSTALLED_GP_DEU
             WHEN 'GPES_' THEN i.INSTALLED_GP_ESP
             WHEN 'GPFR_' THEN i.INSTALLED_GP_FRA
             WHEN 'GPGB_' THEN i.INSTALLED_GP_UK 
             WHEN 'GPIE_' THEN i.INSTALLED_GP_IRL
             WHEN 'GPIT_' THEN i.INSTALLED_GP_ITA
             WHEN 'GPJP_' THEN i.INSTALLED_GP_JPN
             WHEN 'GPIN_' THEN i.INSTALLED_GP_IND
             WHEN 'GPHK_' THEN i.INSTALLED_GP_HKG
             WHEN 'GPMX_' THEN i.INSTALLED_GP_MEX
             WHEN 'GPMY_' THEN i.INSTALLED_GP_MYS
             WHEN 'GPNL_' THEN i.INSTALLED_GP_NLD
             WHEN 'GPNZ_' THEN i.INSTALLED_GP_NZL
             WHEN 'GPSG_' THEN i.INSTALLED_GP_SGP
             WHEN 'GPTW_' THEN i.INSTALLED_GP_TWN
             WHEN 'GPUS_' THEN i.INSTALLED_GP_USA
             ELSE 'Y'
        END AS installed_gp
FROM    psrecdefn r
,       ps_installation i
WHERE   r.rectype IN(0,7) --only SQL tables can be partitioned or rebuilt as GTTs
/

set timi on serveroutput on echo on termout off
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--header of package that populates GFC metadata tables
--------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE gfc_partdata AS
--------------------------------------------------------------------------------------
-- date           author            version   reference           description
--------------------------------------------------------------------------------------
-- 23.08.2012     DMK               1.01      CR1152              Create new script to create package PL/SQL procedure (gfc_partdata) 
-- 28.08.2012     DMK               1.02      CR1152              Changed msg function to call psftapi.message_log
-- 31.08.2012     DMK               1.03      CR1152              to allow for tables that are purged but not archived;
-- 05.09.2012     DMK               1.04      CR1152              remove metadata to filter by PIN_NUM - all pins will now be archived
-- 20.09.2012     DMK               1.05      CR1152              remove archive objects no longer defined because of previous change
-- 17.10.2012     Kay Humphries     1.06      CR1152              Correct the sql that inserts the range partitioning metadata for AUDW1Y2Y
--                                                                so that partitions that are closed as of the start of the week in which the code
--                                                                is being run are compressed (rather than closed as of the start of the month in 
--                                                                which the code is being run are compressed)
-- 31.10.2012     DMK               1.07      CR1152              Additional criteria on GP tables for archiving
-- 10.01.2013     DMK               1.08      CR1152              List subpartitions on GP result tables will now only contain UL pay periods
--                                                                Move maximum build dates to include Tax year 2013-14
-- 15.01.2013     DMK               1.09      CR1152              Added arch_flag to enable historical purge of GPARCH tables
-- 06.03.2013     DMK               1.10      BAU                 Update year end variables to build SCH partitions to 1st Jan 2015
--                                                                TL and GP remain unchanged to end of taxyear 2013-14
-- 20.03.2013     DMK               1.11      CR1152              TL and SCH purge changes
-- 09.04.2013     DMK               1.12      CR1152              Changes to partitioning of indexes on PS_TL_PAYABLE_TIME
-- 19.04.2013     DMK               1.13      CR1152              Index PSCTL_PAYABLE_TIME made global non-partitioned
-- 01.05.2013     DMK               1.14      CR1152              added archive schema to TL records
-- 03.06.2013     DMK               1.15      CR1152              explicitly specify compression and PCTFREE on reporting table indexes
-- 13.06.2013     DMK               1.16      CR1152              added index specfic compression parameters for GPARCH
-- 20.06.2013     DMK               1.17      CR1152              storage options on GP reporting tables and indexes
-- 23.07.2013     DMK               1.18      CR1152              added index compression metadata for wms_gbedi_r_hst, wms_gpgblnwahst 
-- 14.08.2013     DMK               1.19      RTI                 new partitioned and global temp tables for RTI
-- 20.08.2013     DMK               1.20      CR1152              new index G on TL_PAYABLE_TIME for GPPSERVC_I_TRGWRK
--------------------------------------------------------------------------------------
--procedure to populate audit data
--------------------------------------------------------------------------------------------------------------
PROCEDURE partdata;        --head procedure that calls others
PROCEDURE aud_partdata;    --audit metadata
PROCEDURE gp_partdata;     --GP metadata
PROCEDURE gparch_partdata; --GP archive metadata
PROCEDURE gppin_partdata;  --GP reporting tables metadata
PROCEDURE sch_partdata;    --Scheduler meta data
PROCEDURE tl_partdata;     --TL meta data
--------------------------------------------------------------------------------------------------------------
END gfc_partdata;
/

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--body of package that populates GFC metdata tables
--------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY gfc_partdata AS
--------------------------------------------------------------------------------------------------------------
--Constants used in the meta data that may need to be changed over time as partitions are added/archived/purged
--Currently, before the end of each UK tax year, the partitions for the next year are built.  This requires that
--------------------------------------------------------------------------------------------------------------
--This is the Latest UK Tax Year for which partitions have been built. 2013=Tax Year 2013/14
k_gp_last_year CONSTANT INTEGER := 2013;

--This is the partition high value for the maximum partition in each partitioned audit, schedule and TL table 
--respectively.  Tables are effective parititoned up to the day prior to this date
k_audit_max_part_value CONSTANT DATE := TO_DATE('20140501','YYYYMMDD'); 
k_sch_max_part_value   CONSTANT DATE := TO_DATE('20150101','YYYYMMDD'); 
k_tl_max_part_value    CONSTANT DATE := TO_DATE('20140501','YYYYMMDD'); 
--------------------------------------------------------------------------------------------------------------
--Other constants that probably do not need to be changed
--------------------------------------------------------------------------------------------------------------
--This is the earliest date for audit, schedule and TL data, ie when the system first went live.  Therefore, 
--partitions go back to this date
k_min_audit_date CONSTANT DATE := TO_DATE('20081107','YYYYMMDD'); 
k_min_tl_date    CONSTANT DATE := TO_DATE('20081107','YYYYMMDD'); 
k_min_sch_date   CONSTANT DATE := TO_DATE('20081107','YYYYMMDD'); 

--First UK Tax Year of for GP
k_gp_first_year   CONSTANT INTEGER := 2008;
--------------------------------------------------------------------------------------------------------------
--Other constants that should not be changed
--------------------------------------------------------------------------------------------------------------
k_datetime_format CONSTANT VARCHAR2(25) := 'hh24:mi:ss dd.mm.yyyy'; --date format picture for message stamps
k_module          CONSTANT VARCHAR2(48) := $$PLSQL_UNIT; --name of package for instrumentation

--------------------------------------------------------------------------------------------------------------
--prints message with leading timestamp
--28.08.2012 DMK changed to call psftapi.message_log
--------------------------------------------------------------------------------------------------------------
PROCEDURE msg(p_msg VARCHAR2) IS
BEGIN
  psftapi.message_log(p_message=>p_msg,p_verbose=>TRUE);
  --dbms_output.put_line(TO_CHAR(SYSDATE,k_datetime_format)||':'||p_msg);
END msg;

--------------------------------------------------------------------------------------------------------------
--execute SQL
--------------------------------------------------------------------------------------------------------------
PROCEDURE exec_sql(p_sql VARCHAR2) IS
BEGIN
  msg('SQL:'||p_sql);
  EXECUTE IMMEDIATE p_sql;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows processed.');
END exec_sql;
--------------------------------------------------------------------------------------------------------------
--Procedure to delete existing metadata by PART_ID
--added 
--------------------------------------------------------------------------------------------------------------
PROCEDURE del_partdata 
(p_part_id VARCHAR2)
IS
  k_action CONSTANT VARCHAR2(48) := 'DEL_PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  msg('Delete pre-existing list of '||p_part_id||' global indexes');
  DELETE FROM gfc_part_indexes 
  WHERE part_id LIKE p_part_id;
  l_num_rows := SQL%ROWCOUNT;
  DELETE FROM gfc_part_indexes WHERE recname IN (SELECT recname FROM gfc_part_tables WHERE part_id LIKE p_part_id);
  l_num_rows := l_num_rows + SQL%ROWCOUNT;
  DELETE FROM gfc_part_indexes WHERE NOT recname IN (SELECT recname FROM gfc_part_tables);
  l_num_rows := l_num_rows + SQL%ROWCOUNT;
  msg(TO_CHAR(l_num_rows)||' rows deleted.');

  msg('Delete pre-existing list of partitioned '||p_part_id||' tables');
  DELETE FROM gfc_part_tables 
  WHERE part_id LIKE p_part_id;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

  msg('Delete pre-existing '||p_part_id||' range partitioning metadata');
  DELETE FROM gfc_part_ranges
  WHERE part_id LIKE p_part_id;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

  msg('Delete pre-existing '||p_part_id||' list partitioning metadata');
  DELETE FROM gfc_part_lists
  WHERE part_id LIKE p_part_id;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

  msg('Delete pre-existing '||p_part_id||' range-v-list partition mapping metadata');
  DELETE FROM gfc_part_range_lists
  WHERE part_id LIKE p_part_id;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

  commit;

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END del_partdata;
--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE AUDIT METADATA
--------------------------------------------------------------------------------------------------------------
--AUD              monthly partitioning, no archive/purge policy --not used any more.
--AUDMP6M   monthly partition, don't archive purge after 6 months
--AUDM3M1Y  monthly partition, archive after 3 months, purge after 1 year
--AUDM1Y2Y  monthly partition, archive after 1 year, purge after a 2nd year
--AUDW1Y2Y  weekly partition, archive after 1 year, purge after a 2nd year
--AUDMA3Y   monthly partition, archive after 3 years, never purge
--------------------------------------------------------------------------------------------------------------
PROCEDURE aud_partdata IS
  k_action CONSTANT VARCHAR2(48) := 'AUD_PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

--delete any preexisting audit data
del_partdata('AUD%');
--------------------------------------------------------------------------------------------------------------
--monthly partitions archive after 3 months, purge after 1 year
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for AUDM3M1Y');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag) 
SELECT  r.recname, 'AUDM3M1Y'
,       'AUDIT_STAMP', 'R', 'A'
FROM    psrecdefn r
WHERE   r.recname IN('AUDIT_AUTHITEM'  --Added by KH 01.06.2012
                    ,'AUDIT_CLASSDEFN' --Added by KH 01.06.2012
                    ,'AUDIT_PSOPRDEFN' --partitioned 28.8.2009, because volume increasing 
                    ,'AUDIT_ROLECLASS' 
                    ,'AUDIT_ROLEDEFN'  
                    ,'AUDIT_ROLEUSER'  
                    );
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--monthly partitions just purge after 6 months
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for AUDMP6M');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag) 
SELECT  r.recname, 'AUDMP6M'
,          'AUDIT_STAMP', 'R', 'D'
FROM    psrecdefn r
WHERE   r.recname IN('AUDIT_COMPENSTN'
--                  ,'AUDIT_EE_STKHLD' --small
--                  ,'AUDIT_GPABSCMTS' --small
--                  ,'AUDIT_JPMJITEMS' --small
--                  ,'AUDIT_JPMPROFIL' --small
                    ,'AUDIT_NAMES'
                    ,'AUDIT_PERORGASG'   --Added by KH 01.06.2012
--                  ,'AUDIT_PERORGINS' --small
--                  ,'AUDIT_PERSDTAEF' --small
                    ,'AUDIT_PERSON'
                    ,'AUDIT_SCH_TBL'     --Added by KH 01.06.2012
--                  ,'AUDIT_WMS_PSDIR' --small
                    ,'AUDIT_WMSBSSCHD'
--                  ,'AUDIT_WMSECLWIT' --small
                    );
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--monthly partitions archive after 1 year, purge after 2
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for AUDM1Y2Y');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag) 
SELECT  r.recname, 'AUDM1Y2Y'
,        'AUDIT_STAMP', 'R', 'A'
FROM    psrecdefn r
WHERE   r.recname IN(
--                   'AUDIT_ABS_HIST' --small
--                  ,'AUDIT_ABSHISTDT' --small
--                  ,'AUDIT_BENPRGPTC' --small (small comment addded by KH 01.06.2012)
--                  ,'AUDIT_CAR_PLAN' --small
                     'AUDIT_GPABSEVNT' --originally partitioned 21.8.2009, for benefit of WMS_APPAY_01
                    ,'AUDIT_GPABSEVTJ' --originally partitioned 28.8.2009, because volume increasing
                    ,'AUDIT_GPACMUSER'
--                  ,'AUDIT_GPEECODTL' --small
--                  ,'AUDIT_GPEECOHDR' --small
--                  ,'AUDIT_GPEELOAND' --small
--                  ,'AUDIT_GPEELOANH' --small
                    ,'AUDIT_GPEENI'
--                  ,'AUDIT_GPEESS' --small
--                  ,'AUDIT_GPEESSHOL' --small
--                  ,'AUDIT_GPEESTLOA' --small
                    ,'AUDIT_GPEETAX'
--                  ,'AUDIT_GPGLDSDTL' --small
--                  ,'AUDIT_GPGLDSPIN' --small
--                  ,'AUDIT_GPNETDIST' --small
                    ,'AUDIT_GPNTDSTDL'
                    ,'AUDIT_GPNTDSTDT'  -- added by KH 01.06.2012
                    ,'AUDIT_GPPIMNLDT'
                    ,'AUDIT_GPPYEOVRD'
                    ,'AUDIT_GPPYESOVR'
                    ,'AUDIT_GPPYOVSOV'
--                  ,'AUDIT_GPRCPPYDT' --small
                    ,'AUDIT_GPRSLTACM'
--                  ,'AUDIT_WMSGIBESI' --small
--                  ,'AUDIT_WMSGIBETX' --small
                    ,'PSAUDIT');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--weekly partitions archive after 1 year, purge after 2
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for AUDW1Y2Y');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag) 
SELECT  r.recname, 'AUDW1Y2Y'
,        'AUDIT_STAMP', 'R', 'A'
FROM    psrecdefn r
WHERE   r.recname IN('AUDIT_TLRPTTIME' --previously weekly partitioned
                    ,'AUDIT_TL_PAY_TM' -- added by KH 13.06.2012 - previously weekly partitioned
                    );
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--monthly partitions in monthly tablespaces archive after 3 years and retain
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for AUDMA3Y');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag) 
SELECT  r.recname, 'AUDMA3Y'
,        'AUDIT_STAMP', 'R', 'A'
FROM    psrecdefn r
WHERE   r.recname IN('AUDIT_JOB'
--                  ,'AUDIT_PERS_NID' --small
                    );
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--apply rebuild filter in line with purge policy to table rebuild definition so that if we every rebuild the
--table we do not rebuild data that we would purge anyway
-----------------------------------------------------------------------------------------------------------
msg('Set tablespace and purge criteria for Audit Records');
UPDATE gfc_part_tables
SET    tab_tablespace = 'AUD6MTAB'
,      idx_tablespace = 'AUD6MIDX' 
,      criteria = 'WHERE audit_stamp >= TRUNC(ADD_MONTHS(SYSDATE,-6),''MM'')'
WHERE  part_id IN('AUDMP6M');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

UPDATE gfc_part_tables
SET    tab_tablespace = 'AUD1YTAB'
,      idx_tablespace = 'AUD1YIDX' 
,      criteria = 'WHERE audit_stamp >= TRUNC(ADD_MONTHS(SYSDATE,-12),''MM'')'
WHERE  part_id IN('AUDM3M1Y');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

UPDATE gfc_part_tables
SET    tab_tablespace = 'AUD2YTAB'
,      idx_tablespace = 'AUD2YIDX' 
,      criteria = 'WHERE audit_stamp >= TRUNC(ADD_MONTHS(SYSDATE,-24),''MM'')'
WHERE  part_id IN('AUDM1Y2Y','AUDW1Y2Y');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

UPDATE gfc_part_tables
SET    tab_tablespace = 'AUD_TAB'
,      idx_tablespace = 'AUD_IDX' 
,      criteria = ''
WHERE  part_id IN('AUDMA3Y');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

-----------------------------------------------------------------------------------------------------------
--set archive schema and storage options for archive tables
--note we are not setting default tablespaces
-----------------------------------------------------------------------------------------------------------
msg('Set audit flags and storage options for audit records');
UPDATE gfc_part_tables x
SET    x.arch_schema    = CASE x.arch_flag WHEN 'A' THEN 'PSARCH' 
                                           WHEN 'D' THEN 'PSARCH' 
                                           ELSE '' END
,      x.tab_storage    = 'PCTUSED 99 PCTFREE 0'
,      x.idx_storage    = 'PCTFREE 0 COMPRESS 1'
WHERE  x.part_id LIKE 'AUD%';
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategry
--------------------------------------------------------------------------------------------------------------
--just purge after 6 months
msg('Insert range partitioning metadata for AUDMP6M');
INSERT INTO gfc_part_ranges 
(         part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,    tab_storage,   arch_flag)
SELECT  y.part_id, y.part_no, y.part_name, y.part_value, y.tab_tablespace,  y.idx_tablespace, y.tab_storage, y.arch_flag
FROM    (
        SELECT  'AUDMP6M' part_id
        ,       TO_NUMBER(TO_CHAR(mydate,'YYMM')) part_no
        ,       TO_CHAR(mydate,'YYMM') part_name
        ,       'TO_DATE('''||TO_CHAR(ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1),'YYYYMMDD')||''',''YYYYMMDD'')' part_value
        ,       'AUD6MTAB' tab_tablespace
        ,       'AUD6MIDX' idx_tablespace
        ,       CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
        ,       CASE WHEN ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1)<ADD_MONTHS(SYSDATE,-6) THEN 'D' ELSE 'N' END arch_flag --purge
        FROM    (
                SELECT  a.from_dt+b.n mydate
                FROM    (
                        SELECT k_min_audit_date from_dt
                        FROM dual 
                        ) a
                ,       (
                        SELECT rownum-1 n
                        FROM dual
                        CONNECT BY LEVEL <= (k_audit_max_part_value-k_min_audit_date+1)
                        ) b
                ) x
        WHERE mydate >= k_min_audit_date
        GROUP BY TO_CHAR(mydate,'YYMM')
        HAVING MIN(mydate) < k_audit_max_part_value 
        ) y
        LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
        LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--monthly partitions - archive after 3 months, purge after 1 year
msg('Insert range partitioning metadata for AUDM3M1Y');
INSERT INTO gfc_part_ranges 
(         part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,     tab_storage,   arch_flag)
SELECT  y.part_id, y.part_no, y.part_name, y.part_value, y.tab_tablespace,  y.idx_tablespace,  y.tab_storage, y.arch_flag
FROM    (
        SELECT  'AUDM3M1Y' part_id
        ,       TO_NUMBER(TO_CHAR(mydate,'YYMM')) part_no
        ,       TO_CHAR(mydate,'YYMM') part_name
        ,       'TO_DATE('''||TO_CHAR(ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1),'YYYYMMDD')||''',''YYYYMMDD'')' part_value
        ,       'AUD1YTAB' tab_tablespace
        ,       'AUD1YIDX' idx_tablespace
        ,       CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
        ,       CASE WHEN ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1)<ADD_MONTHS(SYSDATE,-12) THEN 'D' --note sequence of condititons
                     WHEN ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1)<ADD_MONTHS(SYSDATE,-3) THEN 'A' 
                     ELSE 'N' END arch_flag --purge
        FROM    (
                SELECT  a.from_dt+b.n mydate
                FROM    (
                        SELECT k_min_audit_date from_dt
                        FROM dual 
                        ) a
                ,       (
                        SELECT rownum-1 n
                        FROM dual
                        CONNECT BY LEVEL <= (k_audit_max_part_value-k_min_audit_date+1)
                        ) b
                ) x
        WHERE mydate >= k_min_audit_date
        GROUP BY TO_CHAR(mydate,'YYMM')
        HAVING MIN(mydate) < k_audit_max_part_value 
        ) y
        LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
        LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--monthly partitions - archive after 1 year, purge after 2 year
msg('Insert range partitioning metadata for AUDM1Y2Y');
INSERT INTO gfc_part_ranges 
(           part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,     tab_storage,   arch_flag)
SELECT    y.part_id, y.part_no, y.part_name, y.part_value, y.tab_tablespace,  y.idx_tablespace,  y.tab_storage, y.arch_flag
FROM        (
        SELECT 'AUDM1Y2Y' part_id
        ,      TO_NUMBER(TO_CHAR(mydate,'YYMM')) part_no
        ,      TO_CHAR(mydate,'YYMM') part_name
        ,      'TO_DATE('''||TO_CHAR(ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1),'YYYYMMDD')||''',''YYYYMMDD'')' part_value
        ,      'AUD2YTAB' tab_tablespace
        ,      'AUD2YIDX' idx_tablespace
        ,      CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
        ,      CASE WHEN ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1)<ADD_MONTHS(SYSDATE,-24) THEN 'D' --note sequence of condititons
                    WHEN ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1)<ADD_MONTHS(SYSDATE,-12) THEN 'A' 
                    ELSE 'N' END arch_flag --purge
        FROM   (
               SELECT a.from_dt+b.n mydate
               FROM        (
                        SELECT k_min_audit_date from_dt
                        FROM dual 
                        ) a
               ,        (
                        SELECT rownum-1 n
                        FROM dual
                        CONNECT BY LEVEL <= (k_audit_max_part_value-k_min_audit_date+1)
                        ) b
               ) x
        WHERE mydate >= k_min_audit_date
        GROUP BY TO_CHAR(mydate,'YYMM')
        HAVING MIN(mydate) < k_audit_max_part_value 
        ) y
        LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
        LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--weekly partitions - archive after 1 year, purge after 2 year
-- 17.10.2012 CR1152 Kay Humphries Amend 'TRUNC(SYSDATE,'MM')' to 'TRUNC(SYSDATE,'IW')' in the tab_storage line
--                                 so that all partitions that are closed as of the start of the current week will be compressed.
msg('Insert range partitioning metadata for AUDW1Y2Y');
INSERT INTO gfc_part_ranges 
(           part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,    tab_storage,   arch_flag)
SELECT    y.part_id, y.part_no, y.part_name, y.part_value, y.tab_tablespace,  y.idx_tablespace, y.tab_storage, y.arch_flag
FROM        (
        SELECT  'AUDW1Y2Y' part_id
        ,       TO_NUMBER(TO_CHAR(mydate,'IYIW')) part_no
        ,       TO_CHAR(mydate,'IYIW') part_name
        ,       'TO_DATE('''||TO_CHAR(ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1),'YYYYMMDD')||''',''YYYYMMDD'')' part_value
        ,       'AUD2YTAB' tab_tablespace
        ,       'AUD2YIDX' idx_tablespace
        ,       CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'IW') THEN 'COMPRESS' ELSE '' END tab_storage
        ,       CASE WHEN ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1)<ADD_MONTHS(SYSDATE,-24) THEN 'D' --note sequence of condititons
                     WHEN ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1)<ADD_MONTHS(SYSDATE,-12) THEN 'A' 
                     ELSE 'N' END arch_flag --purge
        FROM        (
                SELECT a.from_dt+b.n mydate
                FROM         (
                         SELECT k_min_audit_date from_dt
                         FROM dual 
                         ) a
               ,         (
                         SELECT rownum-1 n
                         FROM dual
                         CONNECT BY LEVEL <= (k_audit_max_part_value-k_min_audit_date+1)
                         ) b
                ) x
        WHERE mydate >= k_min_audit_date
        GROUP BY TO_CHAR(mydate,'IYIW')
        HAVING MIN(mydate) < k_audit_max_part_value 
        ) y
        LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
        LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--monthly partitions, archive after 3 years, and retain
msg('Insert range partitioning metadata for AUDMA3Y');
INSERT INTO gfc_part_ranges 
(           part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,    tab_storage,   arch_flag)
SELECT    y.part_id, y.part_no, y.part_name, y.part_value, y.tab_tablespace,  y.idx_tablespace, y.tab_storage, y.arch_flag
FROM        (
        SELECT 'AUDMA3Y' part_id
        ,      TO_NUMBER(TO_CHAR(mydate,'YYMM')) part_no
        ,      TO_CHAR(mydate,'YYMM') part_name
        ,      'TO_DATE('''||TO_CHAR(ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1),'YYYYMMDD')||''',''YYYYMMDD'')' part_value
        ,      'AUD_TAB' tab_tablespace
        ,      'AUD_IDX' idx_tablespace
        ,      CASE WHEN MAX(mydate) < TRUNC(SYSDATE,'MM') THEN 'COMPRESS' ELSE '' END tab_storage
        ,      CASE WHEN ADD_MONTHS(TRUNC(MAX(mydate),'MM'),1)<ADD_MONTHS(SYSDATE,-36) THEN 'A' 
                    ELSE 'N' END arch_flag --purge
        FROM   (
               SELECT a.from_dt+b.n mydate
               FROM   (
                      SELECT k_min_audit_date from_dt
                      FROM dual 
                      ) a
                ,     (
                      SELECT rownum-1 n
                      FROM dual
                      CONNECT BY LEVEL <= (k_audit_max_part_value-k_min_audit_date+1)
                      ) b
                ) x
        WHERE mydate >= k_min_audit_date
        GROUP BY TO_CHAR(mydate,'YYMM')
        HAVING MIN(mydate) < k_audit_max_part_value 
        ) y
        LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
        LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--archive/purge partitions should be compressed if built
-----------------------------------------------------------------------------------------------------------
msg('Set compression attributes on audit range partitions');
UPDATE gfc_part_ranges
SET    tab_storage = 'COMPRESS'
WHERE  part_id LIKE 'AUD%'
AND    arch_flag IN('A','D')
AND    tab_storage IS NULL;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

UPDATE gfc_part_ranges
SET    tab_storage = tab_storage||' COMPRESS'
WHERE  part_id LIKE 'AUD%'
AND    arch_flag IN('A','D')
AND    NOT UPPER(tab_storage) LIKE '%COMPRESS%'
AND    tab_storage IS NOT NULL;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

-----------------------------------------------------------------------------------------------------------
--End Audit Archiving Metadata
-----------------------------------------------------------------------------------------------------------
 commit;
 msg('Audit metadata load complete');
 dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END aud_partdata;
--------------------------------------------------------------------------------------------------------------



-----------------------------------------------------------------------------------------------------------
--set archive flags to suppress build of fully partitioned sub-partitions
--if all CAL_RUN_IDs in a partition have already been moved to GP_CAL_RUN_HST then they parition can be dropped/not built
-----------------------------------------------------------------------------------------------------------
PROCEDURE gp_part_archflag IS
 k_action CONSTANT VARCHAR2(48) := 'GP_PART_ARCHFLAG';
 l_module VARCHAR2(48);
 l_action VARCHAR2(32);

 l_num_archived  NUMBER;
 l_num_finalized NUMBER;
 l_arch_flag     VARCHAR2(1);
BEGIN
 dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
 dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);
 msg('GP List Sub-Partition Archive Flags');
 FOR i IN (
   SELECT *
   FROM   gfc_part_lists
   WHERE  part_id = 'GP'
   AND    part_no < 9999
   ORDER  BY part_no
 ) LOOP
  SELECT COUNT(*) --count all finalised 
  INTO   l_num_finalized  
  FROM   ps_gp_cal_run
  WHERE  run_finalized_ind = 'Y'
  AND    INSTR(i.list_value,cal_run_id)>0;

  SELECT COUNT(*) --count all archived
  INTO   l_num_archived
  FROM   ps_gp_cal_run_hst
  WHERE  INSTR(i.list_value,cal_run_id)>0;

  IF l_num_finalized = l_num_archived AND l_num_archived>0 THEN 
   l_arch_flag := 'D';
  ELSE
   l_arch_flag := 'N';
  END IF;    
  msg(i.part_name||':'||l_num_finalized||' finalised,'
                      ||l_num_archived||' archived -> archive flag='||l_arch_flag);
  UPDATE gfc_part_lists
  SET    arch_flag = l_arch_Flag
  WHERE  part_id = 'GP'
  AND    part_no = i.part_no
  AND    part_name = i.part_name;
 END LOOP;
 dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END gp_part_archflag;

-----------------------------------------------------------------------------------------------------------
--set subpartiton storage option - though cannot set it directly in DDL
--if all finalised CAL_RUN_IDs periods are 1 month old then they can be compressed
-----------------------------------------------------------------------------------------------------------
PROCEDURE gp_subpart_compress IS
 k_action CONSTANT VARCHAR2(48) := 'GP_SUBPART_COMPRESS';
 l_module VARCHAR2(48);
 l_action VARCHAR2(32);

 l_num_1monthold NUMBER; --number of CAL_RUN_IDs in partition closed for 1 month or more
 l_num_finalized NUMBER; --number of CAL_RUN_IDs in partition finalized
 l_tab_storage   VARCHAR2(10); --working storage variable to hold partition storage option
BEGIN
 dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
 dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);
 msg('GP List Sub-Partition Storage Options');
 FOR i IN (
   SELECT *
   FROM   gfc_part_lists
   WHERE  part_id = 'GP'
   AND    part_no < 9999
   ORDER  BY part_no
 ) LOOP
  SELECT COUNT(*) --count all finalised 
  INTO   l_num_finalized  
  FROM   ps_gp_cal_run
  WHERE  run_finalized_ind = 'Y'
  AND    INSTR(i.list_value,cal_run_id)>0;

  BEGIN
   SELECT COUNT(DISTINCT r.cal_run_id) --count finalised over a month ago
   INTO   l_num_1monthold
   FROM   ps_gp_cal_run r, ps_gp_cal_run_dtl d
   WHERE  r.cal_run_id = d.cal_run_id
   AND    r.run_finalized_ind = 'Y'
   AND    INSTR(i.list_value,r.cal_run_id)>0
   HAVING MAX(d.prd_end_dt) < ADD_MONTHS(SYSDATE,-1);
  EXCEPTION
   WHEN NO_DATA_FOUND THEN l_num_1monthold :=0;
  END;

  IF l_num_finalized = l_num_1monthold AND l_num_1monthold>0 THEN 
   l_tab_storage := 'COMPRESS';
  ELSE
   l_tab_storage := 'NOCOMPRESS';
  END IF;
  msg(i.part_name||':'||l_num_finalized||' finalised,'||l_num_1monthold||' closed for a month -> storage option='||l_tab_storage);
  UPDATE gfc_part_lists
  SET    tab_storage = l_tab_storage
  WHERE  part_id = 'GP'
  AND    part_no = i.part_no
  AND    part_name = i.part_name;
 END LOOP;
 dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END gp_subpart_compress;


--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE GP METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE gp_partdata IS
  k_action CONSTANT VARCHAR2(48) := 'GP_PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('GP');

--20.3.2013 v1.11 TL_PAYABLE_TIME moved to TL partitioning scheme
--msg('Delete pre-existing metadata for global non-partitioned function-based index PSZTL_PAYABLE_TIME');
--DELETE FROM gfc_ps_indexdefn
--WHERE recname = 'TL_PAYABLE_TIME';
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
--DELETE FROM gfc_ps_keydefn
--WHERE recname = 'TL_PAYABLE_TIME';
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

--------------------------------------------------------------------------------------------------------------
--insert data to describe temporary tables
--country specific tables for installed country extentions only will be added
--------------------------------------------------------------------------------------------------------------
msg('Insert list of GP temporary tables');
INSERT INTO gfc_temp_tables                 
SELECT  r.recname
FROM    gfc_installed_gp r
WHERE   r.installed_gp != 'N'
AND     r.rectype IN(0,7) --only normal tables can be partitioned
AND     (r.recname IN( /*payroll calculation work tables*/
                'GP_CAL_TS_WRK',
                'GP_CANC_WRK',
                'GP_CANCEL_WRK',   /*new in 8.4*/
                'GP_DB2_SEG_WRK',  /*new in 8.4*/
                'GP_DEL_WRK',
                'GP_DEL2_WRK',     /*new in 8.4*/
                'GP_EXCL_WRK',
                'GP_FREEZE_WRK',   /*new in 8.4*/
                'GP_HST_WRK',
                'GP_JOB2_WRK',     /*13.2.2008 - added at Kelly*/
                'GP_MSG1_WRK'    ,'GP_MSG3_WRK',
                'GP_NEW_RTO_WRK' ,
                'GP_OLD_RTO_WRK' ,
                'GP_PI_HDR_WRK',
                'GP_PYE_HIST_WRK', /*13.2.2008 - added at Kelly*/
                'GP_PYE_HIS2_WRK', /*13.2.2008 - added at Kelly*/
                'GP_PYE_ITER_WRK', /*new in 8.4*/
                'GP_PYE_ITR2_WRK', /*new in 8.4*/
                'GP_PYE_RCLC_WRK', /*new in hcm9.0 - added 21.5.2008*/
                'GP_PYE_STAT_WRK',
                'GP_PYE_STA2_WRK', /*13.2.2008 - added at Kelly*/
                'GP_RTO_CRD_WRK', /*20.2.2008 added at kelly*/
                'GP_RTO_MCU_WRK', /*3.10.2010 added at Morrisons*/
                'GP_RTO_PRC_WRK' ,
                'GP_RTO_TRG_WRK1', /*new in 8.4*/
                'GP_RTO_TRGR_WRK',
                'GP_SEG_WRK',
                'GP_TLPTM_WRK',
                'GP_TLSNT_WRK',
                'GP_TLTRC_WRK',
                'GP_TL_PIGEN_WRK',
                'GP_TL_PIHDR_WRK',
                'GP_TL_TRG_WRK',
                /*pin packager*/
                'GP_PKG_ELEM_WRK',
                /*GL*/
                'GP_ACC_LINE_STG', --added 5.3.2009
                'GP_GL_AMT1_TMP' ,'GP_GL_AMT2_TMP',
                'GP_GL_DATA_TMP' ,
                'GP_GL_DNF_TMP'  ,
                'GP_GL_MAPI_TMP' ,
                'GP_GL_OLD_TMP'  ,
                'GP_GL_SEG_TMP'  ,
                'GP_GL_SEGV_TMP' ,
                'GP_GL_STO6_TMP' ,
                'GP_GL_S7N8_TMP' ,
                /*Banking*/
                'GP_NET_PAY1_TMP','GP_NET_PAY2_TMP','GP_NET_PAY3_TMP',
                'GP_NET_DST1_TMP','GP_NET_DST2_TMP',
                'GP_PAYMENT_TMP' ,'GP_PAYMENT2_TMP',
                'GP_REV_DLTA_TMP', /*added 9.6.2004*/
                'GP_SRC_BNK1_TMP','GP_SRC_BNK2_TMP',
                /*Swiss Banking*/
                'GPCH_BK_TMP1','GPCH_BK_TMP2',
                'GPCH_BK_PMTTYPE',
                /*reporting*/
                'GPCH_BL_PRINT_T',
                'GPCH_BL_PRT',
                'GPCH_RP_AL1',
                'GPCH_RP_AL01',
                'GPCH_RP_AL03'  , 'GPCH_RP_AL03_1',
                'GPCH_RP_AL07_1', 'GPCH_RP_AL07_2', 'GPCH_RP_AL07_3',
                'GPCH_RP_AL08',
                'GPCH_RP_AL81'  , 'GPCH_RP_AL82'   , 'GPCH_RP_AL83',
                'GPCH_RP_TX01A' ,
                'GPCH_RP_TX06'  , 'GPCH_RP_TX06_01',
                'GPCH_RP_TX61'  , 'GPCH_RP_TX62'   , 'GPCH_RP_TX63',
                'GPCH_RP_FK1A','GPCH_RP_FK2A',
                'GPCH_RP_0001_01',
                'GPCH_SRC_BNK',
                'GPCHAL021_TMP','GPCHAL022_TMP','GPCHAL023_TMP',
                'GPCHAL024_TMP',
                'GPCHAL031_TMP',
                'GPCHAL051_TMP','GPCHAL052_TMP',
                'GPCHAL071_TMP','GPCHAL072_TMP','GPCHAL073_TMP','GPCHAL074_TMP','GPCHAL075_TMP',
                'GPCHAL101_TMP','GPCHAL102_TMP',
                'GPCHSI061_TMP',
                'GPCHST021_TMP','GPCHST022_TMP','GPCHST023_TMP',
                'GPCHTX011_TMP','GPCHTX012_TMP',
                'GPCHTX021_TMP',
                'GPCHTX061_TMP','GPCHTX062_TMP','GPCHTX063_TMP','GPCHTX064_TMP',
                'GPGB_PSLIP_ED_D','GPGB_PSLIP_BL_D', /*gpgb_pslip can now be run stream 4.2.2004*/
                'GPGB_PSLIP_ED_W','GPGB_PSLIP_BL_W',
                --element summary report
               'WMS_GP_ELN_TMP','WMS_GP_RSLT_ED','WMS_GP_RSLT_AC')
OR r.recname IN(
                SELECT t.recname
                FROM   psaeappltemptbl t   
                ,      psaeappldefn a   
                WHERE  a.ae_applid = t.ae_applid   
                AND    a.ae_disable_restart = 'Y' --restart is disabled   
                AND    a.ae_applid IN('GP_PMT_PREP','GP_GL_PREP','GPGB_PSLIP','GPGB_PSLIP_X','GPGB_EDI'
                                      ,'GPGB_RTI') /*added RTI 14.8.2013*//*limited to just GP AE processes*/
                ))
MINUS
SELECT recname 
FROM gfc_temp_tables
;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--insert data to specify the tables to be partitioned
--country specific tables for installed country extentions only will be added
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for GP tables range partitioned on EMPLID');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'GP'
,        'EMPLID', 'R'
FROM    gfc_installed_gp r
WHERE   r.installed_gp != 'N'
AND     r.rectype = 0 /*only normal tables can be partitioned*/
AND     (   r.recname IN(
                        'GP_AUDIT_TBL',           /*added 21.5.2008*/
                        'GP_ABS_EVENT',           /*absence - added 3.10.2003*/
                        'GP_GL_DATA',             /*gl transfer table*/
--                      'GP_GRP_LIST_RUN',        /*new in 8.4*/
                        'GP_ITER_TRGR',
                        'GP_MESSAGES',
                        'GP_PAYMENT',             
                        'GP_PI_GEN_HDR',          /*13.2.2008-added for kelly*/
                        'GP_PI_GEN_DATA',         /*postitive input*/
                        'GP_PI_GEN_REF',          /*13.2.2008-added for kelly-TL postitive input*/
                        'GP_PI_GEN_SOVR',         /*14.11.2008-added for abbey, 6.7.2012 partitioned at Morrisons*/
                        'GP_PI_MNL_DATA',         /*postitive input*/
                        'GP_PI_MNL_SOVR',         /*postitive input*/
                        'GP_PYE_ITER_LST',        /*13.2.2008-added for kelly*/
                        'GP_PYE_OVRD',
                        'GP_PYE_PRC_STAT',
                        'GP_PYE_SEG_STAT',
                        'GP_RCP_PYE_DTL',         /*added 7.6.2004 for gp_pmt_prep*/
                        'GP_RSLT_ABS',            /*payroll calculation results*/
                        'GP_RSLT_ACUM',           /*payroll calculation results*/
                        'GP_RSLT_DELTA',          /*payroll calculation results*/
                        'GP_RSLT_ERN_DED',        /*payroll calculation results*/
                        'GP_RSLT_PI_DATA',        /*payroll calculation results*/
                        'GP_RSLT_PI_SOVR',        /*payroll calculation results*/
                        'GP_RSLT_PIN',            /*payroll calculation results*/
                        'GP_RTO_TRG_CTRY',        /*8.1 + 8.3 only*/
                        'GP_RTO_TRGR',            /*deadlock problem*/
                        'GPCH_BK_XFER_EE',        /*bank transfer*/
                        'GPCH_TX_DATA',           /*tax data table - added 19.6.2003 - to improve scan and reduce latch contention*/
                        'GPGB_ABS_EVT_JR',
                        'JOB',                    /*hr data */
--                      'COMPENSATION',           /*hr compensation data added 18.1.2010*/
                        'X_PYE_OVRD_ET',          /*customer table*/
                        'GPGB_PAYMENT',           /*added 12.3.2004*/
                        'GPGB_PSLIP_P_ED',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_PSLIP_P_BL',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_PSLIP_P_HR',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_PSLIP_P_FT',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_EDI_RSLT',          /*13.1.2010-result table for GPGB_EDI process*/
                        'GPGB_RTI_RSLT',          /*14.8.2013-result table for GPGB_RTI process*/
--                      'TL_PAYABLE_TIME',        /*13.2.2008-added for kelly-TL, 20.3.2013 v1.11 moved TL partition scheme*/
                        'WMS_GB_EDI_RSLT'         /*13.1.2010-custom edi result table*/
                        ) 
        OR r.recname IN(SELECT  recname
                        FROM    ps_gp_wa_array
                        )
        );
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


--------------------------------------------------------------------------------------------------------------
--specify list subpartitioned tables
--------------------------------------------------------------------------------------------------------------
msg('Specify GP tables to be list subpartitioned on CAL_RUN_ID');
UPDATE gfc_part_tables
SET    subpart_type = 'L'
,      subpart_column = 'CAL_RUN_ID'
,      hash_partitions = 0
WHERE  recname IN('GP_RSLT_ACUM', 'GP_RSLT_PIN'
--,'GP_GL_DATA' --not worth subpartitioning at Kelly
--,'GP_PYE_SEG_STAT' -- subpartitioning does not work well with retro queries at kelly
--,'GP_PYE_PRC_STAT' -- subpartitioning does not work well with retro queries at kelly
--,'GP_RSLT_PI_SOVR', 'GP_RSLT_PI_DATA' --14.2.2008 removed at kelly
);
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');


--UPDATE gfc_part_tables
--SET        subpart_type = 'L'
--,        subpart_column = 'SRC_CAL_RUN_ID'
--,        hash_partitions = 0
--WHERE         recname IN('GP_PI_GEN_DATA') --no point subpartitioning because rows deleted during processing 
--AND 1=2 -- and it causes performance problems in GPPDPDM5_S_PIGSOVR
--;

--------------------------------------------------------------------------------------------------------------
--set storage options on partitioned objects
--------------------------------------------------------------------------------------------------------------
msg('Specify tablespaces for partitioned GP tables');
UPDATE gfc_part_tables
SET    tab_tablespace = 'GPAPP'   /*default PSFT tablespace*/
,      idx_tablespace = 'PSINDEX' /*default PSFT tablespace*/
,      tab_storage = 'PCTUSED 95 PCTFREE 1'
,      idx_storage = 'PCTFREE 0 '||
	CASE recname --13.06.2013 added compression attributes to indexes where most time spent in payroll
      WHEN 'GP_RSLT_ACUM'    THEN 'COMPRESS 6' --44147s/wk--
      WHEN 'GP_RSLT_PIN'     THEN 'COMPRESS 7' --13487s/wk--
--------------------------------------------------------------------------------------------------------------
--candidates for further compresssion in the future
--------------------------------------------------------------------------------------------------------------
--    WHEN 'GP_PI_GEN_DATA'  THEN 'COMPRESS 5' --10180s/wk--index A 3/3
--    WHEN 'GP_PYE_PRC_STAT  THEN 'COMPRESS 4' -- 8987s/wk--index a 2/2, B 4/5, C 4/5
--    WHEN 'GP_PI_GEN_REF'   THEN 'COMPRESS 7' -- 5952s/wk--
--    WHEN 'GP_PYE_SEG_STAT  THEN 'COMPRESS 4' -- 5280s/wk--index A 2/2
--    WHEN 'GP_RSLT_ERN_DED' THEN 'COMPRESS 5' -- 4260s/wk--
--------------------------------------------------------------------------------------------------------------
      ELSE 'NOCOMPRESS' --no index compression by default
      END  
WHERE  part_id = 'GP';
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');


msg('Specify storage option for GP_PYE_SEG_STAT');
UPDATE gfc_part_tables
SET    tab_storage = 'PCTUSED 80 PCTFREE 15'
WHERE  recname IN('GP_PYE_SEG_STAT');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');


--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
msg('Insert metadata for global non-partitioned index on partitioned GP tables.');
INSERT INTO gfc_part_indexes
(recname, indexid, part_id, part_type, part_column, subpart_type, subpart_column, hash_partitions)
SELECT   t.recname
,        i.indexid
,        t.part_id
,        t.part_type
,        t.part_column
,        'N' subpart_type
,        '' subpart_column
,        t.hash_partitions
FROM     gfc_part_tables t
,        psindexdefn i
WHERE    t.recname = i.recname
AND      t.subpart_type IN('L','R')
AND      t.part_id = 'GP'
AND NOT EXISTS(
        SELECT 'x'
        FROM pskeydefn k, psrecfielddb f
        WHERE f.recname = i.recname
        AND   k.recname = f.recname_parent
        AND   k.indexid = i.indexid
        AND   k.fieldname = t.subpart_column
        AND   k.keyposn <= 3 --partitioning key in first three columns of index
        );
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


--------------------------------------------------------------------------------------------------------------
--added 31.10.2012 specify reverse of archiving criteria on live tables
--------------------------------------------------------------------------------------------------------------
MERGE INTO gfc_part_tables u
USING (
SELECT a.recname, a.wms_exceptions
,      'WHERE '||CASE WHEN f.fieldname LIKE '%CAL_RUN_ID%' THEN f.fieldname ELSE 'CAL_RUN_ID' END
       ||' NOT IN(SELECT cal_run_id FROM ps_gp_cal_run_hst)' criteria
FROM   ps_wms_archobjrec a
,      gfc_part_tables p
,      psrecfielddb f
WHERE  a.psarch_object IN('WMS_GP_A2P2')
AND    a.psarch_basetable != 'Y'
AND    p.recname = a.recname
AND    p.part_id = 'GP'
AND    f.recname = a.recname
AND    f.fieldname = CASE WHEN a.wms_exceptions = ' ' THEN 'CAL_RUN_ID' ELSE a.wms_exceptions END
) s
ON (u.recname = s.recname)
WHEN MATCHED THEN UPDATE 
SET u.criteria = s.criteria;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

--------------------------------------------------------------------------------------------------------------
--INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
--VALUES ('GP_PYE_PRC_STAT','B','GP','N',' ', 'COMPRESS 3')
--/

--------------------------------------------------------------------------------------------------------------
msg('Insert metadata for global non-partitioned indexes on PS_JOB');
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','0','GP','N',' ', 'COMPRESS 1');
l_num_rows := SQL%ROWCOUNT;
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','1','GP','N',' ', 'COMPRESS 1');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','2','GP','N',' ', 'COMPRESS 1');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','3','GP','N',' ', 'COMPRESS 1');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','4','GP','N',' ', 'COMPRESS 1');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
--ASG745022 Index E leading on LOCATION
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('JOB','E','GP','N',' ');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
msg(TO_CHAR(l_num_rows)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
msg('Insert metadata for global non-partitioned indexes on GP_ABS_EVENT');
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('GP_ABS_EVENT','A','GP','N',' ');
l_num_rows := SQL%ROWCOUNT;
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('GP_ABS_EVENT','C','GP','N',' ');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
msg(TO_CHAR(l_num_rows)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategy
--------------------------------------------------------------------------------------------------------------
msg('Create GP range partitioning metadata from GP stream definition');
INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value)
SELECT 'GP', strm_num
,      LTRIM(TO_CHAR(strm_num,'000')) part_name
,      NVL(LEAD(''''||emplid_from||'''',1) OVER (ORDER BY strm_Num),'MAXVALUE') part_value
FROM   ps_gp_strm;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


-----------------------------------------------------------------------------------------------------------
--set tablespaces for GP range partitions
-----------------------------------------------------------------------------------------------------------
msg('Specify tablespaces for GP list subpartitions');
UPDATE  gfc_part_ranges
SET     tab_tablespace = 'GPSTRM'||LTRIM(TO_CHAR(MOD(part_no-1,32)+1,'00'))||'TAB'
,       idx_tablespace = 'GPSTRM'||LTRIM(TO_CHAR(MOD(part_no-1,32)+1,'00'))||'IDX'
WHERE part_id IN('GP');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

-----------------------------------------------------------------------------------------------------------
--insert data to list partitions
--2007 onwards
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
--lunar monthly partitions for UK lunar monthy payees
-----------------------------------------------------------------------------------------------------------
--26.7.2012  DMK Gibraltar pay periods no longer included in list partitioning because too small.
--10.01.2013 DMK List subpartitions on GP result tables will now only contain UL pay periods,
--               UW periods will go to the default list partition
-----------------------------------------------------------------------------------------------------------
-- was previously 
-- ''''||LTRIM(TO_CHAR(y.year,'0000'))||'UL'||LTRIM(TO_CHAR(  p.period  ,'00'))||''','||
-- ''''||LTRIM(TO_CHAR(y.year,'0000'))||'UW'||LTRIM(TO_CHAR(4*p.period-3,'00'))||''','||
-- ''''||LTRIM(TO_CHAR(y.year,'0000'))||'UW'||LTRIM(TO_CHAR(4*p.period-2,'00'))||''','||
-- ''''||LTRIM(TO_CHAR(y.year,'0000'))||'UW'||LTRIM(TO_CHAR(4*p.period-1,'00'))||''','||
-- ''''||LTRIM(TO_CHAR(y.year,'0000'))||'UW'||LTRIM(TO_CHAR(4*p.period-0,'00'))||''''
-----------------------------------------------------------------------------------------------------------
msg('Insert list partition definition for UK Lunbar pay periods');

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
SELECT  'GP'
,       year+period/100+1/1000 part_no
,       LTRIM(TO_CHAR(y.year,'0000'))||'L'||LTRIM(TO_CHAR(p.period,'00')) part_name /*L was U*/
,       ''''||LTRIM(TO_CHAR(y.year,'0000'))||'UL'||LTRIM(TO_CHAR(  p.period  ,'00'))||''''
FROM    (
        SELECT  rownum as period
        FROM    dba_objects
        WHERE   rownum <= 14 --up to 14 periods per year
        ) p
,       (
        SELECT  k_gp_first_year+(rownum-1) as year 
        FROM    dba_objects
        WHERE   rownum <= k_gp_last_year-k_gp_first_year+1 --years of list partitions 
        ) y
WHERE   period <= DECODE(y.year,2023,14,13) --fourteeth lunar period in 2023
AND     LTRIM(TO_CHAR(y.year,'0000'))||LTRIM(TO_CHAR(p.period,'00')) >= '200809' --suppress build of unused GP lists
ORDER BY 1,2,3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--10.01.2013 DMK UW data moved to default list partition so no need to add W53 metadata
--msg('Add Week 53 to list partitions definition for 2013 and 2023');
--need to add specific years where W53
--UPDATE  gfc_part_lists a
--SET     a.list_value = a.list_value ||','''||SUBSTR(a.part_name,1,4)||'UW53'''
--WHERE   a.part_id = 'GP'
--AND     a.part_name = (
--                SELECT MAX(b.part_name)
--                FROM   gfc_part_lists b
--                WHERE  a.part_id = 'GP'
--                AND    SUBSTR(a.part_name,1,5) = SUBSTR(b.part_name,1,5))
--AND     (a.part_name LIKE '2013U_' OR
--        a.part_name LIKE '2023U_') -- and others
--AND     a.list_value like '%UW52%';
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');
-----------------------------------------------------------------------------------------------------------

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
VALUES ('GP',9999,'Z_OTHERS','DEFAULT');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

----------------------------------------------------------------------------------------------------
--tablespaces for list partitions
-----------------------------------------------------------------------------------------------------------
msg('Specify tablespaces for GP list subpartitions');
UPDATE   gfc_part_lists a
SET      tab_tablespace = 'GP'||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4)),'0000'))||'L'||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,6,2)),'00'))||'TAB'
,        idx_tablespace = 'GP'||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4)),'0000'))||'L'||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,6,2)),'00'))||'IDX'
WHERE    part_id = 'GP'
and      SUBSTR(a.part_name,5,1) IN('U','L')
AND      part_no < 9999;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

gp_part_archflag; --set archive flags for GP subpartitions
gp_subpart_compress; --set compression attribute for GP subpartitions

-----------------------------------------------------------------------------------------------------------
--statistics options for list subpartitioned tables
-----------------------------------------------------------------------------------------------------------
msg('Specify histogram collection policy for GP list subpartitioned tables');
UPDATE  gfc_part_tables
SET     method_opt = 'FOR ALL COLUMNS SIZE 1, FOR COLUMNS SIZE 254 CAL_RUN_ID, CAL_ID, ORIG_CAL_RUN_ID'
WHERE   part_id IN('GP')
AND     subpart_type = 'L';
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

-----------------------------------------------------------------------------------------------------------
--mapping between ranges and lists
-----------------------------------------------------------------------------------------------------------
msg('Insert GP range-v-list partition mapping metadata');
INSERT INTO gfc_part_range_Lists
(part_id, range_name, list_name)
SELECT r.part_id, r.part_name, l.part_name
FROM   gfc_part_ranges r
,      gfc_part_lists l
WHERE  l.part_id = r.part_id
AND    l.part_id IN('GP');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

commit;
dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END gp_partdata;
--------------------------------------------------------------------------------------------------------------



--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE SECSET AND SECTREE METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE sec_partdata IS
  k_action CONSTANT VARCHAR2(48) := 'SEC_PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('SEC%'); --SECSET, SECTREE
--------------------------------------------------------------------------------------------------------------
--insert data to specify the tables to be partitioned
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for SECTREE tables list partitioned on SETID');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, tab_tablespace, idx_tablespace) 
SELECT  r.recname, 'SECTREE'
,	'SETID', 'L'
,	'PTTREE','PSINDEX'
FROM    psrecdefn r
WHERE   r.recname IN('PSTREENODE');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

msg('Insert record metadata for SECSET tables list partitioned on SCRTY_SET_CD');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, tab_tablespace, idx_tablespace) 
SELECT  r.recname, 'SECSET'
,	'SCRTY_SET_CD', 'L'
,	'HRAPP','PSINDEX'
FROM    psrecdefn r
WHERE   r.recname IN('SJT_CLASS_ALL');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');



-----------------------------------------------------------------------------------------------------------
--insert data to list partitions
-----------------------------------------------------------------------------------------------------------
msg('Insert List partitioning metadata for SECSET tables');
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
SELECT	'SECSET'
,	rownum
, 	SCRTY_SET_CD
, 	''''||SCRTY_SET_CD||''''
FROM	(
	SELECT	DISTINCT SCRTY_SET_CD
	FROM 	PS_SJT_CLASS_ALL
	WHERE	SCRTY_SET_CD LIKE 'WMS%'
	OR	SCRTY_SET_CD LIKE 'PPL%'
	OR	SCRTY_SET_CD LIKE 'SCALR'
        )
ORDER BY 1,2,3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

UPDATE gfc_part_lists
set part_name = 'SPACE'
where part_name = ' '
and   part_id IN('SECSET');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
VALUES ('SECSET',9999,'Z_OTHERS','DEFAULT');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


----------------------------------------------------------------------------------------------------------- 
msg('Insert List partitioning metadata for SECTREE tables');
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
SELECT 'SECTREE'
,      rownum
,      SETID
,      ''''||SETID||''''
FROM	 (
       SELECT DISTINCT SETID
       FROM   PSTREENODE
       WHERE  SETID != ' '
       ORDER BY 1
       )
ORDER BY 3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
VALUES ('SECTREE',9999,'Z_OTHERS','DEFAULT');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--insert data to describe temporary tables
--country specific tables for installed country extentions only will be added
--------------------------------------------------------------------------------------------------------------
msg('Insert list of Security temporary tables');
INSERT INTO gfc_temp_tables                 
SELECT  r.recname
FROM    psrecdefn r
WHERE   r.rectype IN(0,7) --only normal tables can be partitioned
AND     r.recname IN(
        'SJTPRJ_TMP','SJTPRJ2_TMP' --SCRTY_SJTUPD
        )
MINUS
SELECT recname 
FROM gfc_temp_tables;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

  commit;
  msg('SECTREE and SECSET metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END sec_partdata;
--------------------------------------------------------------------------------------------------------------



--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE GP METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE gparch_partdata IS
  k_action CONSTANT VARCHAR2(48) := 'GPARCH_PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);

  l_list_value VARCHAR2(1000);
  l_prd_bgn_dt DATE;
  l_prd_end_dt DATE;
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
 dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
 dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

 del_partdata('GPARCH%');
--------------------------------------------------------------------------------------------------------------
--insert data to specify the tables to be partitioned
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for partitioned GP Archive tables');
--15.1.2013 added arch_flag to enable historical purge of GPARCH tables
INSERT INTO gfc_part_tables
(recname, part_id, part_type, part_column, subpart_type, subpart_column
, src_table_name, criteria, tab_storage, idx_storage, arch_flag) 
SELECT DISTINCT a.hist_recname, 'GPARCH' 
,	'R', CASE WHEN f.fieldname LIKE '%CAL_RUN_ID%' THEN f.fieldname ELSE 'CAL_RUN_ID' END part_column
, 	'L', CASE WHEN f.fieldname LIKE '%CAL_RUN_ID%' THEN f.fieldname ELSE 'CAL_RUN_ID' END subpart_column
, 	DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) 
,	'WHERE '||
	CASE WHEN f.fieldname LIKE '%CAL_RUN_ID%' THEN f.fieldname ELSE 'CAL_RUN_ID' END
	||' IN(SELECT cal_run_id FROM ps_gp_cal_run_hst)' criteria
,	'PCTFREE 0 PCTUSED 99 COMPRESS' 
, 	'PCTFREE 0 '
||    CASE a.hist_recname --13.06.2013 added index specfic compression parameters
      WHEN 'GP_ACM_U_AJ_HST' THEN 'COMPRESS 6'
      WHEN 'GP_GL_DATA_HST'  THEN 'COMPRESS 10'
      WHEN 'GP_PAYMENT_HST'  THEN 'COMPRESS 6'
      WHEN 'GP_PY_PR_ST_HST' THEN 'COMPRESS 3'
      WHEN 'GP_PY_SG_ST_HST' THEN 'COMPRESS 4'
      WHEN 'GP_RSL_PI_D_HST' THEN 'COMPRESS 6'
      WHEN 'GP_RSL_PI_S_HST' THEN 'COMPRESS 6'
      WHEN 'GP_RSLT_ABS_HST' THEN 'COMPRESS 6'
      WHEN 'GP_RSLT_ACM_HST' THEN 'COMPRESS 6' 
      WHEN 'GP_RSLT_DLT_HST' THEN 'COMPRESS 6'
      WHEN 'GP_RSLT_E_D_HST' THEN 'COMPRESS 7'
      WHEN 'GP_RSLT_PIN_HST' THEN 'COMPRESS 7' 
      WHEN 'GPGB_EE_EXC_HST' THEN 'NOCOMPRESS'
      WHEN 'GPGB_EE_R_HST'   THEN 'COMPRESS 4'
      WHEN 'GPGB_PMNT_HST'   THEN 'COMPRESS 4'
      WHEN 'WMS_GP_A_E_HST'  THEN 'NOCOMPRESS'
      WHEN 'WMS_GBEDI_R_HST' THEN 'NOCOMPRESS' --added 23.7.2013
      WHEN 'WMS_GPGBERT_HST' THEN 'NOCOMPRESS'
      WHEN 'WMS_GPGBLNWAHST' THEN 'NOCOMPRESS' --added 23.7.2013
      WHEN 'WMS_GPPIGDT_HST' THEN 'COMPRESS 7'
      WHEN 'WMS_GPPIGHR_HST' THEN 'COMPRESS 3'
      WHEN 'WMS_GPPIGRF_HST' THEN 'COMPRESS 8'
      WHEN 'WMS_GPPIGSV_HST' THEN 'COMPRESS 7'
      ELSE '' --no compression by default
      END 
,     'D' --15.1.2013 added arch_flag to enable historical purge of GPARCH tables
FROM  ps_wms_archobjrec a
,	psrecdefn r
,	psdbfield f
WHERE a.psarch_object in('WMS_GP_A2P2')
and	r.recname = a.recname
and	f.fieldname = CASE WHEN a.wms_exceptions = ' ' THEN 'CAL_RUN_ID' ELSE a.wms_exceptions END
and	a.psarch_basetable != 'Y'
and	NOT r.recname IN('GP_CL_RN_DL_HST')
and	a.hist_recname != ' ' --added 31.8.2012 to allow for tables that are purged but not archived
;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--5.9.2012-this statement was originally created to add metadata for tables PIN_NUM when we were only going to 
--archive some pins and simply purge the rest.  That is not now going to happen, all pins will be archived
--------------------------------------------------------------------------------------------------------------
--msg('Insert record metadata for partitioned GP Archive tables - Pins');
--INSERT INTO gfc_part_tables
--(recname, part_id, part_type, part_column, subpart_type, subpart_column
--, src_table_name, criteria, tab_storage, idx_storage) 
--SELECT DISTINCT a.hist_recname, 'GPARCH' 
--,	'R', CASE WHEN f.fieldname LIKE '%CAL_RUN_ID%' THEN f.fieldname ELSE 'CAL_RUN_ID' END part_column
--, 	'L', CASE WHEN f.fieldname LIKE '%CAL_RUN_ID%' THEN f.fieldname ELSE 'CAL_RUN_ID' END subpart_column
--, 	DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) 
--,	'WHERE '||
--	CASE WHEN f.fieldname LIKE '%CAL_RUN_ID%' THEN f.fieldname ELSE 'CAL_RUN_ID' END
--	||' IN(SELECT cal_run_id FROM ps_gp_cal_run_hst) AND '||
--	CASE WHEN f.fieldname LIKE '%PIN%NUM%' THEN f.fieldname ELSE 'PIN_NUM' END
--	||' IN(SELECT pin_num FROM ps_wms_gp_pin WHERE wms_archive_flag = ''Y'')' criteria
--,	'PCTFREE 0 PCTUSED 99 COMPRESS' 
--, 	'PCTFREE 0 COMPRESS' 
--FROM  ps_wms_archobjrec a
--,	psrecdefn r
--,	psdbfield f
--WHERE a.psarch_object in('WMS_GP_A2P2H8_PIN','WMS_GP_P2_PIN')
--and	r.recname = a.recname
--and	f.fieldname = CASE WHEN a.wms_exceptions = ' ' THEN 'PIN_NUM' ELSE a.wms_exceptions END
--and	a.psarch_basetable != 'Y'
--and	a.hist_recname != ' ' --added 31.8.2012 to allow for tables that are purged but not archived
--;
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategy - one range partition per tax year
--------------------------------------------------------------------------------------------------------------
msg('Insert annual range partitioning metadata for GP Archive tables');
--up to last 8 complete UK tax years, but not last complete tax year, so list 7 years
INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value
, tab_tablespace, idx_tablespace 
--,tab_storage, idx_storage
, arch_flag)
SELECT 'GPARCH'
,	year part_no
, 	LTRIM(TO_CHAR(year,'0000')) part_name
, 	''''||LTRIM(TO_CHAR(year+1,'0000'))||'''' part_value
,	'GP'||LTRIM(TO_CHAR(year,'0000'))||'TAB' tab_tablespace
,	'GP'||LTRIM(TO_CHAR(year,'0000'))||'IDX' idx_tablespace
--,	'PCTFREE 0 COMPRESS' tab_storage
--,	'PCTFREE 0' idx_storage
,	CASE WHEN year < TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE,-3)-5,'YYYY'))-8 THEN 'D' ELSE 'N' END arch_flag
FROM (
	SELECT 	k_gp_first_year+rownum-1 year --first partition is 2008
	FROM	dual
	CONNECT BY LEVEL <= TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE,-3)-5,'YYYY'))-k_gp_first_year-1 --not further than tax year before last
	);
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--lunar monthly partitions for UK weekly and UK lunar monthy payees
--We can use actual GP config to work out what weeks corrspond to what lunar months
--populate list data from actual finalised periods
-----------------------------------------------------------------------------------------------------------
msg('Create GP Archive list partitioning metadata');
l_num_rows := 0;
FOR i IN ( --first get all UL Lunar periods
  select 'GPARCH' part_id
  , 	r.part_no+SUBSTR(c.cal_run_id,7)/100 part_no
  , 	c.cal_run_id, c.cal_run_id part_name
  ,	''''||c.cal_run_id||'''' list_value
  FROM	(
	SELECT 	cal_run_id
	FROM	ps_gp_cal_run
	WHERE	cal_run_id LIKE '20__'||'UL%'
	AND	run_finalized_ind = 'Y'
	UNION 
	SELECT	cal_run_id
	FROM	ps_gp_cal_run_hst
	WHERE	cal_run_id LIKE '20__'||'UL%'
	AND	run_finalized_ind = 'Y'
	) c
  ,	gfc_part_ranges r
  WHERE	r.part_id = 'GPARCH'
  and	c.cal_run_id LIKE r.part_name||'%'
  and	c.cal_run_id >= '2008UL09'
  ORDER BY 1
) LOOP 

  --get period begin/end dates
  SELECT MIN(prd_bgn_dt), MAX(prd_end_dt)
  INTO   l_prd_bgn_dt, l_prd_end_dt
  FROM (
   SELECT cal_run_id, prd_bgn_dt, prd_end_dt 
   FROM ps_gp_cal_run_dtl
   UNION 
   SELECT cal_run_id, prd_bgn_dt, prd_end_dt
   FROM ps_GP_CL_RN_DL_HST
  )
  WHERE  cal_run_id = i.cal_run_id;

  l_list_value := i.list_value;
  FOR j IN( --find UK weekly calendars within period of UK Lunar period
   SELECT cal_run_id
   FROM   ps_gp_cal_run_dtl
   WHERE  prd_bgn_dt BETWEEN l_prd_bgn_dt AND l_prd_end_dt
   AND    prd_end_dt BETWEEN l_prd_bgn_dt AND l_prd_end_dt
   AND    cal_run_id LIKE '____UW__' --UK weekly periods
   AND    cal_run_id != i.cal_run_id
   UNION
   SELECT cal_run_id
   FROM   ps_GP_CL_RN_DL_HST
   WHERE  prd_bgn_dt BETWEEN l_prd_bgn_dt AND l_prd_end_dt
   AND    prd_end_dt BETWEEN l_prd_bgn_dt AND l_prd_end_dt
   AND    cal_run_id LIKE '____UW__' --UK weekly periods
   AND    cal_run_id != i.cal_run_id
  ) LOOP
   l_list_value := l_list_value||','''||j.cal_run_id||'''';
  END LOOP;

  msg('Partition: '||i.part_name||'='||l_list_value);
  INSERT INTO gfc_part_lists
  (part_id, part_no, part_name, list_value)
  VALUES (i.part_id, i.part_no, i.part_name, l_list_value);
  l_num_rows := l_num_rows + SQL%ROWCOUNT;
END LOOP;

--add a bucket subpartition for all range partitons
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('GPARCH',9999,'Z_OTHERS','DEFAULT');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
msg(TO_CHAR(l_num_rows)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--mapping between all ranges and all lists.  Note that initially build is N indicating that nothing is built.
-----------------------------------------------------------------------------------------------------------
msg('Insert GPARCH range-v-list partition mapping metadata');
INSERT INTO gfc_part_range_Lists
(part_id, range_name, list_name, build)
SELECT r.part_id, r.part_name, l.part_name, 'N'
FROM   gfc_part_ranges r
,      gfc_part_lists l
WHERE  l.part_id = r.part_id
AND    l.part_id IN('GPARCH');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--mark range/list combinations that are needed.  Used ranges and default partition.  The end result is a 
--range partition for each tax year and within that list partitions for each UK lunar period with
--corresponding UK weekly data.  All other CAL_RUN_IDs go into the default list sub-partition within the 
--range.  This will have Gib and Migration calendars.  There shouldn't be anything else.
-----------------------------------------------------------------------------------------------------------
msg('Mark GPARCH range-v-list partition mapping metadata for required combinations');
UPDATE gfc_part_range_lists
SET   build = 'Y'
WHERE list_name = 'Z_OTHERS'
OR    SUBSTR(list_name,1,4) = range_name;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

-----------------------------------------------------------------------------------------------------------
--delete range/list combinations that are not needed
-----------------------------------------------------------------------------------------------------------
msg('Delete GPARCH range-v-list partition mapping metadata that is not required');
DELETE FROM gfc_part_range_lists
WHERE build = 'N';
msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

commit;
dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END gparch_partdata;
--------------------------------------------------------------------------------------------------------------





--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE GP METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE gppin_partdata IS
  k_action CONSTANT VARCHAR2(48) := 'GPPIN_PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('GPRPT%'); --delete part IDs GPRPTPIN and GPRPTAC
--------------------------------------------------------------------------------------------------------------
--Partition GP Reporting Tables by PIN
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for GP reporting table WMS_GPPIN_RPT1');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type)
VALUES
('WMS_GPPIN_RPT1', 'GPRPTPIN','PIN_NUM', 'L');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

msg('Insert record metadata for GP reporting table WMS_GPACUM_RPT1');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type)
VALUES
('WMS_GPACUM_RPT1', 'GPRPTAC','PIN_NUM', 'L');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

msg('Set build criteria, source table names, tablespaces and table storage options for GP reporting tables from GPRPTGEN metadata');
UPDATE gfc_part_tables a
SET (criteria,src_table_name) = (
	SELECT 'WHERE 1=1 '||condition_text 
	,	DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
	FROM 	ps_wms_gp_rpt_rec b 
	,	psrecdefn r
	WHERE 	b.tgt_recname = a.recname
	AND	r.recname = b.src_recname)
,     tab_tablespace = 'GPAPP'   /*default PSFT tablespace*/
,     idx_tablespace = 'PSINDEX' /*default PSFT tablespace*/
,     tab_storage = 'PCTUSED 5 PCTFREE 0' /*dmk 03.06.2013 override PCTFREE back to 0, PCTUSED low so blocks do not contain 2 periods*/
,     method_opt = 'FOR ALL COLUMNS SIZE 1'
WHERE part_id IN('GPRPTPIN','GPRPTAC');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

msg('Set index storage and compression options for GP reporting tables');
--Compress first 4 columns: PIN_NUM, EMPLID, CAL_RUN_ID, EMPL_RCD
--But leave the rest uncompressed: GP_PAYGROUP. CAL_ID, ORIG_CAL_RUN_ID, RSLT_SEG_NUM, EMPL_RCD_ACUM, ACM_FROM_DT, ACM_THRU_DT, SLICE_BGN_DT, SEQ_NUM8
UPDATE gfc_part_tables
SET	 idx_storage = 'PCTFREE 5 COMPRESS 4' /*dmk 20.06.2013 5% free to let one period be added before purge frees more space*/ 
WHERE  part_id = 'GPRPTAC';
l_num_rows := SQL%ROWCOUNT;

--Compress first 3 columns: PIN_NUM, EMPLID, CAL_RUN_ID
--But leave the rest uncompressed: GP_PAYGROUP, CAL_ID, ORIG_CAL_RUN_ID, RSLT_SEG_NUM, INSTANCE, SLICE_END_DT, SLICE_BGN_DT, EMPL_RCD
UPDATE gfc_part_tables
SET	 idx_storage = 'PCTFREE 5 COMPRESS 3' /*dmk 20.06.2013 5% free to let one period be added before purge frees more space*/
WHERE  part_id = 'GPRPTPIN';
l_num_rows := l_num_rows+SQL%ROWCOUNT;
msg(TO_CHAR(l_num_rows)||' rows updated.');


--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategy for PINs
--------------------------------------------------------------------------------------------------------------

msg('Insert list partitioning metadata for GP reporting tables');
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
SELECT	t.part_id
,	row_number() over (partition by part_id order by p.pin_num)
,	LTRIM(TO_CHAR(p.pin_num))
, 	p.pin_num
FROM	gfc_part_tables t
,	ps_wms_gp_rpt_pin a
,	ps_gp_pin p
WHERE	t.part_id IN('GPRPTPIN','GPRPTAC')
AND	a.tgt_recname = t.recname
AND	a.pin_nm = p.pin_nm;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

msg('Set tablespaces for GP reporting table list partitions');
UPDATE gfc_part_lists
SET    tab_tablespace = 'GPRPTTAB'
,   	 idx_tablespace = 'GPRPTIDX'
WHERE  part_id IN('GPRPTPIN','GPRPTAC');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

commit;
dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END gppin_partdata;
--------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE SCHEDULE METADATA
--------------------------------------------------------------------------------------------------------------
--SCH  - schedule tables, mostly range partitions 
--------------------------------------------------------------------------------------------------------------
PROCEDURE sch_partdata IS
  k_action CONSTANT VARCHAR2(48) := 'SCH_PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('SCH');

--msg('Delete pre-existing metadata for global non-partitioned indexes on SCH tables');
--DELETE FROM gfc_part_indexes
--WHERE recname IN('SCH_DEFN_DTL', 'SCH_ASSIGN');
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

--msg('Delete pre-existing SCH partitioning metadata');
--DELETE FROM gfc_part_ranges
--WHERE part_id IN('SCH');
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
--COMMIT;

--------------------------------------------------------------------------------------------------------------
--insert data to specify the tables to be partitioned
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for SCH tables range partitioned on DUR');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, criteria) 
SELECT  r.recname, 'SCH'
,       'DUR', 'R'
,       'WHERE dur >= TRUNC(SYSDATE-(56*7),''IW'')' --20.3.2013 v1.11 added build criteria
FROM    psrecdefn r
WHERE   r.recname IN('SCH_MNG_SCH_TBL'
                    ,'SCH_ADHOC_DTL'
                    );
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

msg('Insert record metadata for SCH tables range partitioned on EFFDT');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'SCH'
,        'EFFDT', 'R'
FROM    psrecdefn r
WHERE   r.recname IN('SCH_DEFN_DTL'
                    ,'SCH_DEFN_SHFT' --added 3.6.2009 - for WMS_SBMT_SH-WS
                    ,'SCH_DEFN_TBL'
                    ,'SCH_ASSIGN' --added 28.7.2011 - for archiving
                    ,'SCH_DEFN_ROTATN');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

msg('Specify tablespaces on SCH tables');
UPDATE   gfc_part_tables x
SET      x.tab_tablespace = 'SCHTAB' --26.3.2013 override tablespace at table level
,        x.idx_tablespace = 'SCHIDX' --26.3.2013 override tablespace at table level
,        x.tab_storage    = 'PCTUSED 90 PCTFREE 1'
,        x.idx_storage    = 'PCTFREE 0'
WHERE    x.part_id IN('SCH');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

msg('Specify storage options on SCH_ADHOC_DTL tables');
UPDATE   gfc_part_tables x
SET      x.tab_storage    = 'INITRANS 4 '||x.tab_storage
,        x.idx_storage    = 'INITRANS 4 '||x.idx_storage
WHERE    x.recname IN('SCH_ADHOC_DTL');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
msg('Insert metadata for global non-partitioned indexes on SCH tables');
--2011.08.29 - global non-partitioned index, though considering other options.
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('SCH_DEFN_DTL','_','SCH','N',' ','PCTFREE 1 COMPRESS 4');
l_num_rows := SQL%ROWCOUNT;
--2011.09.16 - global non-partitioned unique index
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('SCH_ASSIGN','_','SCH','N',' ','PCTFREE 1 COMPRESS 2');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
msg(TO_CHAR(l_num_rows)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategry
--------------------------------------------------------------------------------------------------------------
msg('Insert range partitioning metadata for SCH tables');
INSERT INTO gfc_part_ranges 
(        part_id,   part_no,   part_name,   part_value,   tab_storage,   idx_storage,   arch_flag)
SELECT y.part_id, y.part_no, y.part_name, y.part_value, y.tab_storage, y.idx_storage, y.arch_flag
FROM        (
        SELECT   'SCH' part_id
        ,        TO_NUMBER(TO_CHAR(mydate,'IYIW')) part_no
        ,        TO_CHAR(mydate,'IYIW') part_name
        ,        'TO_DATE('''||TO_CHAR(TRUNC(MAX(mydate),'IW')+7,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
-----------------------------------------------------------------------------------------------------------
--20.3.2013 v1.11 SCH range partitions moved from TL to new SCH tablespaces
--      ,        'SCH'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'TAB' tab_tablespace
--      ,        'SCH'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'IDX' idx_tablespace 
-----------------------------------------------------------------------------------------------------------
        ,        CASE WHEN MAX(mydate)<TRUNC(ADD_MONTHS(SYSDATE,-6),'IW') THEN 'PCTFREE 0 PCTUSED 99 COMPRESS' ELSE 'PCTFREE 10' END tab_storage --compress after 6 months --added 6.7.2012
        ,        CASE WHEN MAX(mydate)<TRUNC(ADD_MONTHS(SYSDATE,-6),'IW') THEN 'PCTFREE 0' END idx_storage
-----------------------------------------------------------------------------------------------------------
--20.3.2013 v1.11 we now purge rather than archive after 56 complete weeks
-----------------------------------------------------------------------------------------------------------
--      ,        CASE WHEN MAX(mydate)<TRUNC(SYSDATE-(56*7),'IW') THEN 'A' ELSE 'N' END arch_flag --archiving
        ,        CASE WHEN MAX(mydate)<TRUNC(SYSDATE-(56*7),'IW') THEN 'D' ELSE 'N' END arch_flag --purging
-----------------------------------------------------------------------------------------------------------
        FROM    (
                SELECT  a.from_dt+b.n mydate
                from    (
                        select k_min_sch_date from_dt
                        FROM dual
                        ) a
                ,       (
                        select rownum-1 n
                        FROM dual
                        CONNECT BY LEVEL <= (k_sch_max_part_value-k_min_sch_date+1)
                        ) b
                ) x
        WHERE mydate >= k_min_sch_date
        GROUP BY TO_CHAR(mydate,'IYIW')
        HAVING MIN(mydate) < k_sch_max_part_value
        ) y
-----------------------------------------------------------------------------------------------------------
--26.3.2013-all SCH table partitions will be stored together in same new tablespaces.
--      LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
--      LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
-----------------------------------------------------------------------------------------------------------
ORDER BY 1,2,3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value)
VALUES ('SCH', 9999, 9999, 'MAXVALUE');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--Schedule Metadata
-----------------------------------------------------------------------------------------------------------
msg('Set archive flags SCH metadata');
UPDATE gfc_part_tables
SET    arch_flag = 'D' --20.3.2013 v1.11 changed from A to D because we now purge rather than archive
,      arch_schema = 'PSARCH'
WHERE  part_id IN('SCH');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

msg('Set noarchive condition for some schedule tables');
UPDATE gfc_part_tables
SET    criteria         = 'WHERE end_effdt >= TRUNC(SYSDATE-(56*7),''IW'') OR effdt >= TRUNC(SYSDATE-(56*7),''IW'')'
,      noarch_condition = 'end_effdt >= TRUNC(SYSDATE-(56*7),''IW'') AND effdt < TRUNC(SYSDATE-(56*7),''IW'')'
WHERE  recname IN('SCH_DEFN_TBL','SCH_ASSIGN');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

--20.3.2013 v1.11 removed unnnecesary update
--msg('Clear any noarchive condition for other schedule tables');
--UPDATE gfc_part_tables
--SET    noarch_condition = ''
--WHERE  recname IN('SCH_ADHOC_DTL','SCH_MNG_SCH_TBL');
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

msg('Set noarchive condition for some schedule tables');
UPDATE gfc_part_tables
--20.3.2013 v1.11 added build criteria to metadata--source table alias=s
SET    criteria         = 'WHERE s.effdt >= TRUNC(SYSDATE-(56*7),''IW'') '
                                ||      'OR (s.effdt < TRUNC(SYSDATE-(56*7),''IW'') '
                                ||'AND (s.setid, s.sch_adhoc_ind, s.schedule_id, s.effdt) '
                                ||'IN(SELECT y.setid, y.sch_adhoc_ind, y.schedule_id, y.effdt '
                                ||'FROM sysadm.ps_sch_defn_tbl y '
                                ||'WHERE y.end_effdt >= TRUNC(SYSDATE-(56*7),''IW'') '
                                ||      'AND y.effdt <  TRUNC(SYSDATE-(56*7),''IW''))) ' 
,      noarch_condition = 'EXISTS(SELECT 1 FROM sysadm.ps_sch_defn_tbl y WHERE x.setid = y.setid AND x.sch_adhoc_ind = y.sch_adhoc_ind '
                                ||'AND x.schedule_id = y.schedule_id AND x.effdt = y.effdt '
                                ||'AND y.end_effdt >= TRUNC(SYSDATE-(56*7),''IW'') '
                                ||'AND y.effdt < TRUNC(SYSDATE-(56*7),''IW'')) ' --added 7.10.11 to eliminate partitions
WHERE  recname IN('SCH_DEFN_SHFT' ,'SCH_DEFN_ROTATN', 'SCH_DEFN_DTL');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');


-----------------------------------------------------------------------------------------------------------
--End SCH Metadata
-----------------------------------------------------------------------------------------------------------
  commit;
  msg('SCH metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END sch_partdata;
--------------------------------------------------------------------------------------------------------------



--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE TL METADATA
--------------------------------------------------------------------------------------------------------------
--TL
--TLTZ - TL timezone table.  GMT and CET each in separate partitions, everything else in the default partition
--TLCAL - TL Calendar table.  One range partition per year from 2008-2020
--------------------------------------------------------------------------------------------------------------
PROCEDURE tl_partdata IS
  k_action CONSTANT VARCHAR2(48) := 'TL_PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('TL%'); --delete part IDs TL, TLTZ, TLCAL

--delete any preexisting audit data
--20.3.2013 v1.11 moved TL_PAYABLE_TIME to TL partitioning scheme 
msg('Delete pre-existing metadata for global non-partitioned function-based index PSZTL_PAYABLE_TIME');
DELETE FROM gfc_ps_indexdefn
WHERE recname = 'TL_PAYABLE_TIME';
msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
DELETE FROM gfc_ps_keydefn
WHERE recname = 'TL_PAYABLE_TIME';
msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
commit;
--------------------------------------------------------------------------------------------------------------
--insert data to specify the tables to be partitioned
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata fpr TL tables range partitioned on DUR');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type, arch_flag, arch_schema, criteria) 
SELECT  r.recname, 'TL'
,       'DUR', 'R', 'D'
,       'PSARCH' --1.5.2013 added archive schema
,       'WHERE dur >= TRUNC(SYSDATE-(108*7),''IW'')'
FROM    psrecdefn r
WHERE   r.recname IN('TL_FRCS_PYBL_TM' --added 3.6.2009 - for WMS_SBMT_SH-BE
                    ,'TL_RPTD_TIME'
                    ,'TL_PAYABLE_TIME' --20.3.2013 v1.11 moved from GP partitioning to TL
                    ,'TL_EXCEPTION'    --added 29.7.2010 - for WMS_SUP_EXCP
                    );
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

msg('Specify hash-subpartition on EMPLID on TL tables');
UPDATE gfc_part_tables x
SET    x.tab_storage    = 'PCTUSED 90 PCTFREE 10'
,      subpart_type = 'H'
,      subpart_column = 'EMPLID'
,      hash_partitions = 16
WHERE  x.part_id = 'TL'
AND    x.recname IN('TL_RPTD_TIME'
--added 29.7.2010 - for WMS_SUP_EXCP but never implemented-removed 27.3.2013
--                 ,'TL_EXCEPTION'   
--                 ,'WMS_TL_EXCEPNS' 
                   );
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

msg('Specify tablespaces on TL tables');
UPDATE gfc_part_tables x
SET    x.tab_tablespace = (SELECT y.ddlspacename 
                           FROM   psrectblspc y 
                           WHERE  y.recname = x.recname
                           AND    y.dbtype = (SELECT MAX(y2.dbtype)
                                              FROM   psrectblspc y2
                                              WHERE  y2.recname = x.recname
                                              AND    y2.dbtype IN(' ','2'))) /*default PSFT tablespace*/
,      x.idx_tablespace = 'PSINDEX' /*default PSFT tablespace*/
,      x.tab_storage    = 'PCTUSED 90 PCTFREE 1'
,      x.idx_storage    = 'PCTFREE 1' --20.8.2013 changed from 0 to 1 
WHERE  x.part_id IN('TL');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

msg('Insert record metadata for TL Timezone table: TL_DSTOFFSET');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'TLTZ'
,       'TIMEZONE', 'L'
FROM    psrecdefn r
WHERE   r.recname IN('TL_DSTOFFSET'); --added 3.6.2009
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for TL_TR_STATUS');
BEGIN
INSERT INTO gfc_part_tables
(recname, part_id, part_type, part_column, idx_storage) 
VALUES
('TL_TR_STATUS','N','N','EMPLID','COMPRESS 1');
EXCEPTION WHEN OTHERS THEN NULL; --ignore errors on this insert
END;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
msg('Insert metadata to specify global non-partitioned indexes on TL_PAYABLE_TIME');
--------------------------------------------------------------------------------------------------------------
--Global Indexes on TL_PAYABLE_TIME
--------------------------------------------------------------------------------------------------------------
l_num_rows := 0;
--8.4.2013 Unique Index _ on EMPLID, EMPL_RCD, DUR, SEQ_NBR
--INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
--VALUES ('TL_PAYABLE_TIME','_','TL','R','APPRV_PRCS_DTTM')
--/
--l_num_rows := l_num_rows + SQL%ROWCOUNT;

--Index A on SEQ_NBR only, so do not partition
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_PAYABLE_TIME','A','TL','N',' ');
l_num_rows := l_num_rows + SQL%ROWCOUNT;

--Index B on EMPLID, EMPL_RCD, FROZEN_DATE to be range partitioned on FROZEN_DATE
--8.4.2013 retain GP partitioning on EMPLID now table TL range partitioned on DUR
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_PAYABLE_TIME','B','GP','R','EMPLID');
l_num_rows := l_num_rows + SQL%ROWCOUNT;

--20.3.2013-Index C leads on TRC, EMPLID, PAYABLE_STATUS, DUR - retain GP partitioning on EMPLID
--19.4.2013 make index global non-partitioned because index prefixed on TRC
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_PAYABLE_TIME','C','TL','N',' ');
l_num_rows := l_num_rows + SQL%ROWCOUNT;

--Index D on DEPTID, DUR, PAYABLE_STATUS, RT_SOURCE - not partitioned.  
--8.4.2013 payable status is updated by payroll
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_PAYABLE_TIME','D','GP','N','EMPLID');
l_num_rows := l_num_rows + SQL%ROWCOUNT;

--Index E Not built - leads on CHARTFIELD1, so suppress partitioning in case anyone makes it usable
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_PAYABLE_TIME','E','TL','N',' ');
l_num_rows := l_num_rows + SQL%ROWCOUNT;

--8.4.2013 Index F on EMPLID, EMPL_RCD, APPRV_PRCS_DTTM use GP partitioning which will also suit APPAY
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_PAYABLE_TIME','F','GP','R','EMPLID');
l_num_rows := l_num_rows + SQL%ROWCOUNT;

--20.8.2013 Index G on EMPLID, EMPL_RCD, PAYABLE_STATUS, DUR use GP partitioning 
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_PAYABLE_TIME','G','GP','R','EMPLID');
l_num_rows := l_num_rows + SQL%ROWCOUNT;

INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage, name_suffix)
VALUES ('TL_PAYABLE_TIME','Z','TL','N',' ', 'PCTFREE 1','_SPARSE');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
msg(TO_CHAR(l_num_rows)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
msg('Insert metadata to specify global non-partitioned function based index on TL_PAYABLE_TIME');
--------------------------------------------------------------------------------------------------------------
INSERT INTO gfc_ps_indexdefn (recname, indexid, subrecname, subindexid, platform_ora, custkeyorder, uniqueflag)
VALUES ('TL_PAYABLE_TIME','Z','TL_PAYABLE_TIME','Z', 1, 0, 0);
l_num_rows := SQL%ROWCOUNT;

--(recname,indexid,keyposn,fieldname)
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Z',1,'DECODE(PAYABLE_STATUS,''NA'',''NA'',NULL)',1);
l_num_rows := l_num_rows + SQL%ROWCOUNT;
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Z',2,'DECODE(PAYABLE_STATUS,''NA'',EMPLID,NULL)',1);
l_num_rows := l_num_rows + SQL%ROWCOUNT;
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Z',3,'DECODE(PAYABLE_STATUS,''NA'',DUR,NULL)',1);
l_num_rows := l_num_rows + SQL%ROWCOUNT;
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Z',4,'DECODE(PAYABLE_STATUS,''NA'',EMPL_RCD,NULL)',1);
l_num_rows := l_num_rows + SQL%ROWCOUNT;
msg(TO_CHAR(l_num_rows)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
msg('Insert metadata to specify global non-partitioned function based index on TL_RPTD_TIME');
--------------------------------------------------------------------------------------------------------------
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_RPTD_TIME','0','TL','N',' ');
l_num_rows := SQL%ROWCOUNT;
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_RPTD_TIME','A','TL','N',' ');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_RPTD_TIME','C','TL','N',' ');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_RPTD_TIME','D','TL','N',' ');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
--2009.11.26 this index is going back to local partition - its not ideal, but there are some hints required first
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, hash_partitions, idx_storage)
--VALUES ('TL_RPTD_TIME','E','TL','H','EMPLID', 16, 'COMPRESS 1') --2010.05.20 --WMS_ST_LOAD Step02e hinted to force this
VALUES ('TL_RPTD_TIME','E','TL','N',' ', NULL, 'COMPRESS 1'); --2011.05.04 --remove hash subpartitioning
l_num_rows := l_num_rows + SQL%ROWCOUNT;
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_RPTD_TIME','F','TL','N',' ');
l_num_rows := l_num_rows + SQL%ROWCOUNT;
msg(TO_CHAR(l_num_rows)||' rows inserted.');
--------------------------------------------------------------------------------------------------------------
msg('Insert metadata to specify global non-partitioned index on TL_EXCEPTION');
--------------------------------------------------------------------------------------------------------------
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_EXCEPTION','C','TL','R','ACTION_DTTM');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
--------------------------------------------------------------------------------------------------------------
msg('Insert metadata to specify global partitioned index on TL_TR_STATUS');
--------------------------------------------------------------------------------------------------------------
--2011.10.9 - globally partition unique index on TL_TR_STATUS
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, hash_partitions, idx_storage)
VALUES ('TL_TR_STATUS','_','N','H','EMPLID', 16, 'COMPRESS 1');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategry
--------------------------------------------------------------------------------------------------------------
msg('Insert range partition metadata for TL tables');
INSERT INTO gfc_part_ranges 
(       part_id,   part_no,   part_name,   part_value,   tab_tablespace,    idx_tablespace,    tab_storage,   arch_flag)
SELECT  y.part_id, y.part_no, y.part_name, y.part_value, y.tab_tablespace, y.idx_tablespace, y.tab_storage, y.arch_flag
FROM    (
        SELECT   'TL' part_id
        ,        TO_NUMBER(TO_CHAR(mydate,'IYIW')) part_no
        ,        TO_CHAR(mydate,'IYIW') part_name
        ,        'TO_DATE('''||TO_CHAR(TRUNC(MAX(mydate),'IW')+7,'YYYYMMDD')||''',''YYYYMMDD'')' part_value
        ,        'TL'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'TAB' tab_tablespace
        ,        'TL'||TO_CHAR(MAX(mydate),'YYYY')||'M'||TO_CHAR(MAX(mydate),'MM')||'IDX' idx_tablespace
        ,        CASE WHEN MAX(mydate)<TRUNC(ADD_MONTHS(SYSDATE,-6),'IW') THEN 'PCTFREE 0 PCTUSED 99 COMPRESS' ELSE 'PCTFREE 10' END tab_storage --compress after 6 months --added 6.7.2012
        ,        CASE WHEN MAX(mydate)<TRUNC(ADD_MONTHS(SYSDATE,-6),'IW') THEN 'PCTFREE 0' END idx_storage
-----------------------------------------------------------------------------------------------------------
--20.3.2013 v1.11 we now purge after 108 complete weeks
-----------------------------------------------------------------------------------------------------------
--      ,        'N' arch_flag --archiving
        ,        CASE WHEN MAX(mydate)<TRUNC(SYSDATE-(108*7),'IW') THEN 'D' ELSE 'N' END arch_flag --purging
-----------------------------------------------------------------------------------------------------------
        FROM     (
                 SELECT  a.from_dt+b.n mydate
                 from    (
                         SELECT k_min_tl_date from_dt
                         FROM dual
                         ) a
                 ,       (
                         select rownum-1 n
                         FROM dual
                         CONNECT BY LEVEL <= (k_tl_max_part_value-k_min_tl_date+1)
                        ) b
                ) x
        WHERE mydate >= k_min_tl_date
        GROUP BY TO_CHAR(mydate,'IYIW')
        HAVING MIN(mydate) < k_tl_max_part_value
        ) y
        LEFT OUTER JOIN dba_tablespaces t on t.tablespace_name = y.tab_tablespace
        LEFT OUTER JOIN dba_tablespaces i on i.tablespace_name = y.idx_tablespace
ORDER BY 1,2,3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value)
VALUES
('TL', 9999, 9999, 'MAXVALUE');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


--------------------------------------------------------------------------------------------------------------
--TLZ data originally prepart for list partitioning on SETID, but never used
--------------------------------------------------------------------------------------------------------------
--DELETE FROM gfc_part_ranges
--WHERE part_id IN('TLZ');
--
--INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 1  , 'CENTR' ,'''CENTRz'',MAXVALUE');
--INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 2  , 'DISTR' ,'''DISTRz'',MAXVALUE');
--INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 3  , 'MANUF' ,'''MANUFz'',MAXVALUE');
--INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 4  , 'SHARE' ,'''SHAREz'',MAXVALUE');
--INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 5.2, 'STORE2','''STOREz'',''3''');
--INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES ('TLZ', 999, 'STORE3','MAXVALUE,MAXVALUE');
--
-----------------------------------------------------------------------------------------------------------
--insert data to list partition TL tables by day of week.  Never actually used
-----------------------------------------------------------------------------------------------------------
--msg('Delete pre-existing TL list partitioning metadata');
--DELETE FROM gfc_part_lists
--WHERE part_id = 'TL';
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
--
--msg('Insert list partitioning metadata for TL');
--INSERT INTO gfc_part_lists
--(part_id, part_no, part_name, list_value)
--SELECT 'TL'
--,      rownum
--,      LTRIM(TO_CHAR(rownum,'0'))
--,      ''''||LTRIM(TO_CHAR(rownum,'0'))||''''
--FROM dual                        
--CONNECT BY LEVEL <= 7
--ORDER BY 1,2,3;
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
--
-----------------------------------------------------------------------------------------------------------
--Partitioning of TL Timezone table
-----------------------------------------------------------------------------------------------------------
msg('Insert List partitioning metadata for TL Timezone table: TL_DSTOFFSET');
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
SELECT 'TLTZ'
,      rownum
,      timezone
,      ''''||timezone||''''
FROM   pstimezone
WHERE  timezone IN('GMT','WEST')
ORDER BY 1,2,3;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
VALUES ('TLTZ',9999,'Z_OTHERS','DEFAULT');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

----------------------------------------------------------------------------------------------------------- 
--mapping between ranges and lists 
----------------------------------------------------------------------------------------------------------- 
msg('Insert TL range-v-list partition mapping metadata');
INSERT INTO gfc_part_range_Lists 
(part_id, range_name, list_name) 
SELECT r.part_id, r.part_name, l.part_name 
FROM   gfc_part_ranges r 
,      gfc_part_lists l 
WHERE  l.part_id = r.part_id 
AND    l.part_id = 'TL';
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--TL Calendar Metadata
-----------------------------------------------------------------------------------------------------------
msg('Insert record metadata for TLCAL - TL_CALENDAR');
INSERT INTO gfc_part_tables
(recname, part_id, part_type, part_column, subpart_type, subpart_column, tab_tablespace, idx_tablespace) 
SELECT  r.recname, 'TLCAL'
,        'R', 'START_DT'
,        'L', 'PERIOD_ID'
,        'TLAPP','PSINDEX'
FROM    psrecdefn r
WHERE   r.recname IN('TL_CALENDAR');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


-----------------------------------------------------------------------------------------------------------
msg('Insert list partitioning metadata for TLCAL');
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value, idx_storage)
SELECT 'TLCAL', rownum, part_name, list_value, '**PCTFREE**'
FROM (
SELECT DISTINCT 
       TRANSLATE(period_id,'-','_') part_name
,      ''''||period_id||'''' list_value
FROM  ps_tl_calendar
ORDER BY 1
);
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
VALUES ('TLCAL',9999,'Z_OTHERS','DEFAULT');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--14.7.2012 partitioning now driven directly from ps_tl_calendar but starting with 2007
msg('Insert range partitioning metadata for TLCAL');
INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value)
SELECT  y.part_id, y.part_no, y.part_name, y.part_value
FROM    (
        SELECT  'TLCAL' part_id
        ,       TO_NUMBER(TO_CHAR(mydate,'yyyy')) part_no
        ,       TO_CHAR(mydate,'yyyy') part_name
        ,       'TO_DATE('''||TO_CHAR(ADD_MONTHS(mydate,12),'YYYYMMDD')||''',''YYYYMMDD'')' part_value
        FROM    (
                SELECT DISTINCT TRUNC(START_DT,'YYYY') mydate
                FROM   ps_tl_calendar
                WHERE  start_dt >= TO_DATE('20070101','yyyymmdd')
                ) 
        ) y
ORDER BY 1,2,3
;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
msg('Insert TLCAL range-v-list partition mapping metadata');
INSERT INTO gfc_part_range_Lists 
(part_id, range_name, list_name) 
SELECT r.part_id, r.part_name, l.part_name 
FROM   gfc_part_ranges r 
,      gfc_part_lists l 
WHERE  l.part_id = r.part_id 
AND    l.part_id = 'TLCAL';
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--End TL Metadata
-----------------------------------------------------------------------------------------------------------
  commit;
  msg('TL metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END tl_partdata;
--------------------------------------------------------------------------------------------------------------



--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--procedure to call all other partdata procedures
--------------------------------------------------------------------------------------------------------------
PROCEDURE partdata IS
  k_action CONSTANT VARCHAR2(48) := 'PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

msg('Truncating metadata tables');
exec_sql('TRUNCATE TABLE gfc_part_tables');
exec_sql('TRUNCATE TABLE gfc_part_ranges');
exec_sql('TRUNCATE TABLE gfc_temp_tables');
exec_sql('TRUNCATE TABLE gfc_part_lists');
exec_sql('TRUNCATE TABLE gfc_part_range_lists');
exec_sql('TRUNCATE TABLE gfc_part_indexes');

aud_partdata;
gp_partdata;
gparch_partdata;
gppin_partdata;
sec_partdata;
sch_partdata;
tl_partdata;

--------------------------------------------------------------------------------------------------------------
--set default tablespaces
--------------------------------------------------------------------------------------------------------------
msg('Set default table tablespace where not already set');
UPDATE gfc_part_tables x
SET    x.tab_tablespace = (SELECT y.ddlspacename 
                           FROM   psrectblspc y 
                           WHERE  y.recname = x.recname
                           AND    y.dbtype = (SELECT MAX(y2.dbtype)
                                              FROM   psrectblspc y2
                                              WHERE  y2.recname = x.recname
                                              AND    y2.dbtype IN(' ','2'))) /*default PSFT tablespace*/
WHERE	 x.tab_tablespace IS NULL;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

msg('Set default index tablespace where not already set');
UPDATE 	gfc_part_tables x
SET	x.idx_tablespace = 'PSINDEX' /*default PSFT tablespace*/
WHERE	x.idx_tablespace IS NULL;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END partdata;
-----------------------------------------------------------------------------------------------------------

END gfc_partdata;
/
set termout on
show errors

EXECUTE gfc_partdata.partdata;
spool off

