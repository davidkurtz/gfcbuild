-------------------------------------------------------------------------------------
--
-- script        : gfc_partdata_pkg.PROJ.sql
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
set echo on pages 999
spool gfc_partdata_pkg
--------------------------------------------------------------------------------------------------------------

set timi on serveroutput on echo on termout on
@@psownerid
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--header of package that populates GFC metadata tables
--------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE &&ownerid..gfc_partdata AS
--------------------------------------------------------------------------------------
-- date           author            version   reference           description
--------------------------------------------------------------------------------------
-- 09.09.2012     DMK               1.01                          Create new script to create package PL/SQL procedure (gfc_partdata) 
-- 06.11.2013                       1.02                          Add Temporary tables
-- 05.02.2014     DMK               1.03                          PS_LEDGER index becomes global non-partitioned index
-- 14.07.2014     DMK               1.04                          FY 2012 is in a single partition
--------------------------------------------------------------------------------------
--procedure to populate audit data
--------------------------------------------------------------------------------------------------------------
PROCEDURE partdata;        --head procedure that calls others
--PROCEDURE proj_partdata;     --GL meta data
--PROCEDURE gtt_data;        --Global Temporary Tables
--PROCEDURE index_comp;      --set index compression in PeopleTools tables   
--PROCEDURE comp_attrib;     --procedure to apply compression attributes to existing tables
--------------------------------------------------------------------------------------------------------------
END gfc_partdata;
/

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--body of package that populates GFC metdata tables
--------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY gfc_partdata AS
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--Other constants that should not be changed
--------------------------------------------------------------------------------------------------------------
k_datetime_format CONSTANT VARCHAR2(25) := 'hh24:mi:ss dd.mm.yyyy'; --date format picture for message stamps
k_date_format     CONSTANT VARCHAR2(8)  := 'YYYYMMDD'; --date format picture for partitions
k_month_format    CONSTANT VARCHAR2(6)  := 'YYYYMM'; --month format picture for partitions
k_year_format     CONSTANT VARCHAR2(4)  := 'YYYY'; --year format picture for partitions
k_module          CONSTANT VARCHAR2(64) := $$PLSQL_UNIT; --name of package for instrumentation
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
  k_action CONSTANT VARCHAR2(64) := 'DEL_PARTDATA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
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
  DELETE FROM gfc_part_subparts
  WHERE part_id LIKE p_part_id;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

  commit;

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END del_partdata;
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE GTT METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE gtt_data IS
  k_action CONSTANT VARCHAR2(64) := 'GTT_DATA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);
-----------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--insert data to describe temporary tables
--country specific tables for installed country extentions only will be added
--------------------------------------------------------------------------------------------------------------
msg('Insert list of GP temporary tables');
INSERT INTO gfc_temp_tables                 
WITH a AS ( 
SELECT  t.recname, a.ae_disable_restart 
,       SUM(a.temptblinstances) temptblinstances 
FROM    psaeappltemptbl t 
,       psaeappldefn a 
WHERE   a.ae_applid = t.ae_applid 
--AND     a.ae_disable_restart = 'N' /*restart is enabled*/ 
GROUP BY t.recname, a.ae_disable_restart
)
SELECT  r.recname
FROM    psrecdefn r
  LEFT OUTER JOIN a 
    ON r.recname = a.recname 
WHERE   r.rectype = '7'
AND     NVL(a.ae_disable_restart,'Y') = 'Y' /*restart is enabled*/
AND     r.recname IN( 
   /*rectype=7*/ 'ALC_AMT_G', 'ALC_BASF_G', 'ALC_BASV_G', 'ALC_BASX_G', 'ALC_BULED_G', 'ALC_BU_G', 'ALC_CLOG_G', 'ALC_GL_BS_G', 'ALC_GL_B_G', 'ALC_GL_OS_G', 
   'ALC_GL_O_G', 'ALC_GL_P_G', 'ALC_GL_TB_G', 'ALC_GL_TS_G', 'ALC_GL_TX_G', 'ALC_GL_T_G', 'ALC_GRSTP_G', 'ALC_JHDR_G', 'ALC_OFFV_G', 'ALC_PC_BS_G', 'ALC_PC_B_G', 
   'ALC_PC_P_G', 'ALC_PC_TB_G', 'ALC_PC_TS_G', 'ALC_PC_T_G', 'ALC_POOLF_G', 'ALC_POOLV_G', 'ALC_POOLX_G', 'ALC_RNDLN_G', 'ALC_STEP_G', 'ALC_TARGV_G', 'BKC_ALL_G', 
   'BKC_CLSE_G', 'BKC_OPEN_G', 'BUGRP_G', 'CFVSET_G', 'CFV_SEL_G', 'CFV_SET_G', 'CF_G', 'CLO_ACCT_G', 'CLO_BALGR_G', 'CLO_JHDR_G', 'CLO_JLN2_G', 'CLO_JLN_G', 
   'CLO_LED2_G', 'CLO_LED_G', 'CLO_RE_G', 'COMB_EXP_G', 'COMB_S30_G', 'FS_CE_CFS_G', 'FS_CE_CFV_G', 'FS_GRRULE_G', 'FT_BUGRP_G', 
   'FT_BU_G', 'GLJ_DOCS_G', 'GLJ_POSA_G', 'JED_ADJS_G', 'JED_BA2_G', 'JED_BAL_G', 'JED_JHD2_G', 'JED_JHDR_G', 'JED_JL2_G', 'JED_JLN_G', 'JED_SVT_G', 'JED_VAT_G', 
   'JHDR_SEL_G', 'JP_BULD_G', 'JP_JH_G', 'JP_JL_G', 'JP_PTB1_G', 'JP_PTB2_G', 'JP_PTB_G', 'JRN_HIU_G', 'JRN_IUW2_G', 'JRN_IUW3_G', 'JRN_IUW4_G', 'JRN_IUW5_G', 
   'JRN_IUWK_G', 'JRN_LIU_G', 'LOADER_BU_G', 'MC_ACCT_G', 'MC_LEDGER_G', 'MC_RATE_G', 'MC_SUBTYP_G', 'MC_TSEL_G', 'MC_WRK1_G', 'MC_WRK_G', 'RTBL_JLN_G', 
   'RTB_PST1_G', 'RTB_PST2_G', 'RTB_PST_G', 'TSEL_B_G', 'TSEL_P_G', 'TSEL_R30_G', 'TSE_JLNE_G')
MINUS
SELECT recname 
FROM gfc_temp_tables
;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--End GTT Metadata
-----------------------------------------------------------------------------------------------------------
  commit;
  msg('GTT metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END gtt_data;
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE WL METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE wl_partdata IS
  k_action CONSTANT VARCHAR2(64) := 'WL_PARTDATA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('WL'); --delete part ID GL

  msg('Populating list of WL Partitioned Tables');
  INSERT INTO gfc_part_tables
  (recname, part_id, part_type, part_column, subpart_type, subpart_column
--, tab_tablespace, idx_tablespace, tab_storage, idx_storage
--, method_opt
  )
  VALUES('PSWORKLIST', 'WL', 'R', 'INSTSTATUS', 'N', '' 
--,'GLLARGE', 'PSINDEX', 'PCTUSED 90 PCTFREE **PCTFREE**', 'PCTFREE **PCTFREE**'
--, 'FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 254 LEDGER FISCAL_YEAR ACCOUNTING_PERIOD BOOK_CODE CURRENCY_CD BUSINESS_UNIT ACCOUNT PROJECT_ID'
  );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
  msg('Insert metadata for global non-partitioned index on partitioned WL tables.');
  INSERT INTO gfc_part_indexes
  (recname, indexid, part_id, part_type, part_column, subpart_type, subpart_column, hash_partitions)
  SELECT   t.recname
  ,        i.indexid
  ,        t.part_id
  ,        'N' part_type
  ,        t.part_column
  ,        'N' subpart_type
  ,        '' subpart_column
  ,        t.hash_partitions
  FROM     gfc_part_tables t
  ,        psindexdefn i
  WHERE    t.recname = i.recname
  AND      t.part_type != 'N'
  AND      t.part_id IN('WL')
  AND NOT EXISTS(
        SELECT 'x'
        FROM pskeydefn k, psrecfielddb f
        WHERE f.recname = i.recname
        AND   k.recname = f.recname_parent
        AND   k.indexid = i.indexid
        AND   k.fieldname IN(t.part_column,t.subpart_column)
        AND   k.keyposn <= 3 --partitioning key in first three columns of index
        );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


  l_num_rows := 0;
  msg('Populating WL Range Partitions');
  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) 
  VALUES('WL',2,'SELECT_OPEN' ,'''2''');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;
  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value, tab_storage, idx_storage) 
  VALUES('WL',9,'WORKED_CANC','MAXVALUE','PCTFREE 1 PCTUSED 90','PCTFREE 1');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;

  msg(TO_CHAR(l_num_rows)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--mapping between ranges and lists
-----------------------------------------------------------------------------------------------------------
  msg('Insert GL range-v-list partition mapping metadata');
  INSERT INTO gfc_part_subparts
  (part_id, part_name, subpart_name)
  SELECT r.part_id, r.part_name, l.part_name
  FROM   gfc_part_ranges r
  ,      gfc_part_lists l
  WHERE  l.part_id = r.part_id
  AND    l.part_id IN('WL');

  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
-----------------------------------------------------------------------------------------------------------
--End WL Metadata
-----------------------------------------------------------------------------------------------------------
  commit;
  msg('WL metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END wl_partdata;
--------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
PROCEDURE index_comp IS
  k_action CONSTANT VARCHAR2(64) := 'INDEX_COMP';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  msg('Setting Index Compression in DDL Overrides on Record');

  update pslock    set version = version + 1 where objecttypename IN('SYS','RDM');
  update psversion set version = version + 1 where objecttypename IN('SYS','RDM');
  
  update psrecdefn 
  set    version = (SELECT version FROM psversion WHERE objecttypename = 'RDM')
  ,      lastupddttm = SYSDATE
  ,      lastupdoprid = k_module
  where  recname IN('LEDGER','LEDGER_BUDG');
  
  delete from psidxddlparm p
  where  recname IN('LEDGER','LEDGER_BUDG')
  and    parmname IN('PCTFREE','INIT','NEXT');
  
  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN 'A' THEN ' COMPRESS 5'
    WHEN 'B' THEN ' COMPRESS 5'
    WHEN 'C' THEN ' COMPRESS 7'
    WHEN 'D' THEN ' COMPRESS 4'
    WHEN 'E' THEN ' COMPRESS 1'
    WHEN 'F' THEN ' COMPRESS 9'
    WHEN 'G' THEN ' COMPRESS 1'
    WHEN '_' THEN ' COMPRESS 3'
  END
  from  psindexdefn
  where recname = 'LEDGER'
  and   platform_ora = 1;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 4'
  END
  from  psindexdefn
  where recname = 'LEDGER_BUDG'
  and   platform_ora = 1 ;
 
  update psindexdefn i
  set    ddlcount = (SELECT COUNT(DISTINCT platformid) 
                     FROM   psidxddlparm d
                     WHERE  d.recname = i.recname
                     AND    d.indexid = i.indexid)
  where recname IN('LEDGER','LEDGER_BUDG');

  commit;

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END index_comp;
--------------------------------------------------------------------------------------------------------------
--procedure to truncate metdata tables
--------------------------------------------------------------------------------------------------------------
PROCEDURE truncmeta IS
  k_action CONSTANT VARCHAR2(64) := 'TRUNCMETA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  msg('Truncating metadata tables');
  exec_sql('TRUNCATE TABLE gfc_part_tables');
  exec_sql('TRUNCATE TABLE gfc_part_ranges');
  exec_sql('TRUNCATE TABLE gfc_temp_tables');
  exec_sql('TRUNCATE TABLE gfc_part_lists');
  exec_sql('TRUNCATE TABLE gfc_part_subparts');
  exec_sql('TRUNCATE TABLE gfc_part_indexes');

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END truncmeta;
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE proj_resource metadata
--------------------------------------------------------------------------------------------------------------
PROCEDURE proj_partdata IS
  k_action CONSTANT VARCHAR2(64) := 'PROJ_PARTDATA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
  l_num_rows INTEGER; --variable to hold number of rows processed

BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('PROJ'); --delete part ID PROJ

  msg('Populating list of PROJ Partitioned Tables');
  INSERT INTO gfc_part_tables
  (recname, part_id, part_type, part_column, subpart_type, subpart_column
--, tab_tablespace, idx_tablespace, tab_storage, idx_storage
  , method_opt
  )
  VALUES('PROJ_RESOURCE', 'PROJ', 'R', 'PROJECT_ID', 'N', '' 
--,'PCLARGE', 'PSINDEX', 'PCTUSED 90 PCTFREE **PCTFREE**', 'PCTFREE **PCTFREE**'
  , ''
  );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
  msg('Insert metadata for global non-partitioned index on partitioned PROJ tables.');
  INSERT INTO gfc_part_indexes
  (recname, indexid, part_id, part_type, part_column, subpart_type, subpart_column, hash_partitions)
  SELECT   t.recname
  ,        i.indexid
  ,        t.part_id
  ,        'N' part_type
  ,        t.part_column
  ,        'N' subpart_type
  ,        '' subpart_column
  ,        t.hash_partitions
  FROM     gfc_part_tables t
  ,        psindexdefn i
  WHERE    t.recname = i.recname
  AND      t.part_id IN('PROJ')
  AND NOT EXISTS(
        SELECT 'x'
        FROM   pskeydefn k, psrecfielddb f
        WHERE  f.recname = i.recname
        AND    k.recname = f.recname_parent
        AND    k.indexid = i.indexid
        AND    k.fieldname = t.part_Column
        AND    k.keyposn <= 3 --partitioning key in first three columns of index
        );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


--------------------------------------------------------------------------------------------------------------
-- Partition Ranges
--------------------------------------------------------------------------------------------------------------
  msg('Populating PROJ Range Partitions');
  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value)
  WITH n AS (
    SELECT /*+MATERIALIZE*/ 115+level n FROM DUAL CONNECT BY level <= 25
  ), x as (
  SELECT TO_CHAR(n.n) part_value 
  FROM n
  WHERE n <= 140
  UNION ALL SELECT '110' FROM DUAL
  UNION ALL SELECT '113' FROM DUAL
  UNION ALL SELECT '115' FROM DUAL
  )
  SELECT 'PROJ' part_id
  ,      row_number() over (order by part_value) part_No
  ,      part_value part_name
  ,      ''''||part_value||''''
  FROM   x;

  l_num_rows := SQL%ROWCOUNT;

  INSERT INTO gfc_part_ranges 
  (part_id, part_no, part_name, part_value)
  VALUES
  ('PROJ', 9999, 'MAX', 'MAXVALUE');

  l_num_rows := l_num_rows + SQL%ROWCOUNT;
  msg(TO_CHAR(l_num_rows)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--End PROJ Metadata
-----------------------------------------------------------------------------------------------------------
  commit;
  msg('PROJ metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END proj_partdata;
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE JRNL_LN metadata
--------------------------------------------------------------------------------------------------------------
PROCEDURE jrnlln_partdata IS
  k_action CONSTANT VARCHAR2(64) := 'JRNLLN_PARTDATA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
  l_num_rows INTEGER; --variable to hold number of rows processed

  l_begindate DATE;
  l_enddate   DATE; 
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('JRNLLN'); --delete part ID JRNLLN

  msg('Populating list of JRNLLN Partitioned Tables');
  INSERT INTO gfc_part_tables
  (recname, part_id, part_type, part_column, interval_expr, subpart_type, subpart_column
--, tab_tablespace, tab_storage, idx_storage
  , idx_tablespace
  , method_opt
  )
  VALUES('JRNL_LN', 'JRNLLN', 'I', 'JOURNAL_DATE', 'NUMTOYMINTERVAL(1, ''YEAR'')', 'N', '' 
--,'PCLARGE', 'PCTUSED 90 PCTFREE **PCTFREE**', 'PCTFREE **PCTFREE**'
  , 'SFJRNLIX1'
  , ''
  );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
  msg('Insert metadata for global non-partitioned index on partitioned JRNLLN tables.');
  INSERT INTO gfc_part_indexes
  (recname, indexid, part_id, part_type, part_column, subpart_type, subpart_column, hash_partitions)
  SELECT   t.recname
  ,        i.indexid
  ,        t.part_id
  ,        'N' part_type
  ,        t.part_column
  ,        'N' subpart_type
  ,        '' subpart_column
  ,        t.hash_partitions
  FROM     gfc_part_tables t
  ,        psindexdefn i
  WHERE    t.recname = i.recname
  AND      t.part_id IN('JRNLLN')
  AND NOT EXISTS(
        SELECT 'x'
        FROM   pskeydefn k, psrecfielddb f
        WHERE  f.recname = i.recname
        AND    k.recname = f.recname_parent
        AND    k.indexid = i.indexid
        AND    k.fieldname = t.part_Column
        AND    k.keyposn <= 3 --partitioning key in first three columns of index
        );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


--------------------------------------------------------------------------------------------------------------
-- Partition Ranges
--------------------------------------------------------------------------------------------------------------
  msg('Populating JRNLLN Range Partitions');

  l_begindate := TO_DATE('20140101',k_date_format); --first full size annual partition - prior years all in first partition
  l_enddate   := TRUNC(ADD_MONTHS(SYSDATE,15),'YYYY'); --create next year if within 3 months of end of year

  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value)
  VALUES 
  ('JRNLLN', 1, 'PRE2014', 'TO_DATE(''20140101'','''||k_date_format||''')');

  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value)
  WITH n AS (
    SELECT level+1 part_no
    ,      TO_CHAR(ADD_MONTHS(l_begindate,level*12)-1,k_year_format) offset_year_str
    ,      TO_CHAR(ADD_MONTHS(l_begindate,level*12),k_date_format) offset_date_str
    FROM DUAL
    CONNECT BY LEVEL <= CEIL(MONTHS_BETWEEN(l_enddate, l_begindate)/12)
  )
  SELECT 'JRNLLN', part_no, offset_year_str
  ,      'TO_DATE('''||offset_date_str||''','''||k_date_format||''')'
  FROM   n;
  l_num_rows := SQL%ROWCOUNT;

--INSERT INTO gfc_part_ranges 
--(part_id, part_no, part_name, part_value)
--VALUES
--('JRNLLN', 9999, 'MAX', 'MAXVALUE');
--l_num_rows := l_num_rows + SQL%ROWCOUNT;

  msg(TO_CHAR(l_num_rows)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--End JRNLLN Metadata
-----------------------------------------------------------------------------------------------------------
  commit;
  msg('JRNLLN metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END jrnlln_partdata;
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--procedure to apply compression attributes to tables
--------------------------------------------------------------------------------------------------------------
PROCEDURE comp_attrib IS
  k_action CONSTANT VARCHAR2(64) := 'COMP_ATTRIB';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
  l_sql CLOB;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  truncmeta;
  proj_partdata;

  msg('Checking Compression Attributes -v- Metadata');
  FOR i IN ( --table level compression where all the partitions are the same
with tp as (
SELECT table_name, compression, compress_for
,      count(*) num_parts
FROM   user_tab_partitions
GROUP BY table_name, compression, compress_for
), t as (
select tp.*, count(*) over (partition by table_name) compression_types
from tp
), x as (
select pt.part_id, pt.part_type, t.table_name
,      regexp_substr(pt.tab_storage,'(COMPRESS|NOCOMPRESS)[ [:alnum:]]*',1,1,'i') tab_storage
,      t.compression, t.compress_for
FROM   psrecdefn r
INNER JOIN t
  ON t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
  AND t.compression_types = 1
INNER JOIN gfc_part_tables pt
  ON r.recname = pt.recname
), y as (
SELECT x.*
, CASE WHEN compression = 'ENABLED' AND UPPER(tab_storage) = 'COMPRESS FOR '||x.compress_for THEN NULL --do nothing
       WHEN compression = 'ENABLED' AND compress_for = 'BASIC' AND UPPER(tab_storage) = 'COMPRESS' THEN NULL --do nothing
       WHEN compression IN('DISABLED','NONE') AND (UPPER(tab_storage) = 'NOCOMPRESS' OR tab_storage IS NULL) THEN NULL --do nothing
       WHEN tab_storage IS NULL AND x.compression = 'ENABLED' THEN 'NOCOMPRESS'
       ELSE x.tab_storage
  END cmd
FROM x
)
SELECT * FROM y
WHERE cmd IS NOT NULL
ORDER BY 1,2,3,4
) LOOP
    l_sql := 'ALTER TABLE '||i.table_name||' '||i.cmd;
    dbms_output.put_line(l_sql);
    EXECUTE IMMEDIATE l_sql;
  END LOOP;

  FOR i IN ( --partition level compression settings
with x as (
select pt.part_id, pt.part_type, t.table_name, pr.part_name, tp.partition_name
,      COALESCE(regexp_substr(pr.tab_storage,'(COMPRESS|NOCOMPRESS)[ [:alnum:]]*',1,1,'i')
               ,regexp_substr(pt.tab_storage,'(COMPRESS|NOCOMPRESS)[ [:alnum:]]*',1,1,'i')
               )  tab_storage
,      COALESCE(tp.compression, t.compression) compression
,      COALESCE(tp.compress_for, t.compress_for) compress_for
FROM   psrecdefn r
INNER JOIN user_tables t
  ON t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
INNER JOIN gfc_part_tables pt
  ON r.recname = pt.recname
INNER JOIN gfc_part_ranges pr
  ON  pt.part_type = 'R'
  AND pr.part_id = pt.part_id
INNER JOIN user_tab_partitions tp
ON tp.table_name = t.table_name
AND tp.partition_name = pt.recname||'_'||pr.part_name
), y as (
SELECT x.*
, CASE WHEN compression = 'ENABLED' AND UPPER(tab_storage) = 'COMPRESS FOR '||x.compress_for THEN NULL --do nothing
       WHEN compression = 'ENABLED' AND compress_for = 'BASIC' AND UPPER(tab_storage) = 'COMPRESS' THEN NULL --do nothing
       WHEN compression IN('DISABLED','NONE') AND (UPPER(tab_storage) = 'NOCOMPRESS' OR tab_storage IS NULL) THEN NULL --do nothing
       WHEN tab_storage IS NULL AND x.compression = 'ENABLED' THEN 'NOCOMPRESS'
       ELSE x.tab_storage
  END cmd
FROM x
)
SELECT * FROM y
WHERE cmd IS NOT NULL
ORDER BY 1,2,3,4
) LOOP
    l_sql := 'ALTER TABLE '||i.table_name||' MODIFY PARTITION '||i.partition_name||' '||i.cmd;
    dbms_output.put_line(l_sql);
    EXECUTE IMMEDIATE l_sql;
  END LOOP;
  
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END comp_attrib;
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--procedure to call all other partdata procedures
--------------------------------------------------------------------------------------------------------------
PROCEDURE partdata IS
  k_action CONSTANT VARCHAR2(64) := 'PARTDATA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  truncmeta;

  proj_partdata;
  jrnlln_partdata;
--wl_partdata;
--gtt_data;
--index_comp;

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
set termout on serveroutput on
show errors

EXECUTE gfc_partdata.partdata;
--EXECUTE gfc_partdata.comp_attrib;

spool off
