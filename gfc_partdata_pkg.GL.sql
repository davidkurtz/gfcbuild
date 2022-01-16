-------------------------------------------------------------------------------------
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
PROCEDURE gl_partdata;     --GL meta data
PROCEDURE gtt_data;        --Global Temporary Tables
PROCEDURE index_comp;  --set index compression in PeopleTools tables   
PROCEDURE comp_attrib; --procedure to apply compression attributes to existing tables
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
--------------------------------------------------------------------------------------------------------------
k_gl_first_year           CONSTANT INTEGER := 2014; --This is the first fiscal year 
k_gl_first_monthly_year   CONSTANT INTEGER := 2020; --This is the first fiscal year with monthly partitions
k_gl_last_year            CONSTANT INTEGER := 2022; --This is the last fiscal year 
--------------------------------------------------------------------------------------------------------------
k_glb_first_year          CONSTANT INTEGER := 2014; --This is the first fiscal year 
k_glb_first_monthly_year  CONSTANT INTEGER := 2020; --This is the first fiscal year with monthly partitions
k_glb_last_year           CONSTANT INTEGER := 2022; --This is the last fiscal year 
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--Other constants that should not be changed
--------------------------------------------------------------------------------------------------------------
k_datetime_format CONSTANT VARCHAR2(25) := 'hh24:mi:ss dd.mm.yyyy'; --date format picture for message stamps
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
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
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
--PROCEDURE TO POPULATE GL METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE gl_partdata IS
  k_action CONSTANT VARCHAR2(64) := 'GL_PARTDATA';
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('GL'); --delete part ID GL
  del_partdata('GLB'); --delete part ID GL

  msg('Populating list of GL Partitioned Tables');
  INSERT INTO gfc_part_tables
  (recname, part_id, part_type, part_column, subpart_type, subpart_column
--, tab_tablespace, idx_tablespace, tab_storage, idx_storage
  , method_opt
  )
  VALUES('LEDGER', 'GL', 'R', 'FISCAL_YEAR,ACCOUNTING_PERIOD', 'L', 'LEDGER' 
--,'GLLARGE', 'PSINDEX', 'PCTUSED 90 PCTFREE **PCTFREE**', 'PCTFREE **PCTFREE**'
  , 'FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 254 LEDGER FISCAL_YEAR ACCOUNTING_PERIOD BOOK_CODE CURRENCY_CD BUSINESS_UNIT ACCOUNT PROJECT_ID'
  );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

  msg('Populating list of GLB Partitioned Tables');
  INSERT INTO gfc_part_tables
  (recname, part_id, part_type, part_column, subpart_type, subpart_column
--, tab_tablespace, idx_tablespace, tab_storage, idx_storage
  , method_opt
  )
  VALUES('LEDGER_BUDG', 'GLB', 'R', 'FISCAL_YEAR,ACCOUNTING_PERIOD', 'L', 'LEDGER' 
--,'GLLARGE', 'PSINDEX', 'PCTUSED 90 PCTFREE **PCTFREE**', 'PCTFREE **PCTFREE**'
  , 'FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 254 LEDGER FISCAL_YEAR ACCOUNTING_PERIOD BOOK_CODE CURRENCY_CD BUSINESS_UNIT ACCOUNT PROJECT_ID'
  );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
  msg('Insert metadata for global non-partitioned index on partitioned GL and GLB tables.');
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
  AND      t.subpart_type IN('L','R')
  AND      t.part_id IN('GL','GLB')
  AND NOT EXISTS(
        SELECT 'x'
        FROM pskeydefn k, psrecfielddb f
        WHERE f.recname = i.recname
        AND   k.recname = f.recname_parent
        AND   k.indexid = i.indexid
        AND   k.fieldname = 'FISCAL_YEAR'
        AND   k.keyposn <= 3 --partitioning key in first three columns of index
        );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


  msg('Populating GL Range Partitions');
  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value, tab_storage, idx_storage)
  SELECT 	'GL' part_id
  , 		k_gl_first_year+rownum-1 part_no
  , 		LTRIM(TO_CHAR(k_gl_first_year+rownum-1)) part_name
  , 		LTRIM(TO_CHAR(k_gl_first_year+rownum))||',0'part_value
  , 		'PCTFREE 0 COMPRESS' tab_storage
  , 		'PCTFREE 0' idx_storage
  FROM 	dual
  WHERE    k_gl_first_year<k_gl_first_monthly_year
  CONNECT BY level <= k_gl_first_monthly_year-k_gl_first_year
  UNION ALL
  SELECT 
	'GL' part_id
  ,     y.y+p.part_no/100 part_no
  , 	LTRIM(TO_CHAR(y.y,'0000'))||'_'||p.part_name part_name
  ,	LTRIM(TO_CHAR(y.y+p.y))||','||p.part_value part_value
  ,	CASE WHEN TO_CHAR(y.y+p.y+p.part_value/100,'0000.00') < TO_NUMBER(TO_CHAR(sysdate,'YYYY'))+TO_NUMBER(TO_CHAR(SYSDATE,'MM'))/100
	     THEN 'PCTFREE 0 COMPRESS'
        END 
  ,	CASE WHEN TO_CHAR(y.y+p.y+p.part_value/100,'0000.00') < TO_NUMBER(TO_CHAR(sysdate,'YYYY'))+TO_NUMBER(TO_CHAR(SYSDATE,'MM'))/100
	     THEN 'PCTFREE 0'
        END 
  FROM	(
	SELECT  k_gl_first_monthly_year+rownum-1 y --monthly partitioned data starts in 2012
	FROM	dual
	CONNECT BY level <= k_gl_last_year-k_gl_first_monthly_year+1 --2 years of monthly partitioned data
	) y
  ,	(
	SELECT	rownum part_no
        ,       rownum+1 part_value --partition less than value
        , 	LTRIM(TO_CHAR(rownum,'00')) part_name --name of range partition
        ,       0 y --add to year
	FROM	dual
	CONNECT BY level <= 12 --12 monthly periods
	UNION ALL
	SELECT 	0, 1, 'BF', 0
	FROM	dual
	UNION ALL
	SELECT 	99, 0 --carry forward less than 
	, 	'CF'
	, 	1 --carry forward into next year
	FROM	dual
	) p;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

  msg('Populating GL Budget Range Partitions');
  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value, tab_storage, idx_storage)
  SELECT 
	'GLB' part_id
  ,     y.y+p.part_no/100 part_no
  , 	LTRIM(TO_CHAR(y.y,'0000'))||'_'||p.part_name part_name
  ,	LTRIM(TO_CHAR(y.y+p.y))||','||p.part_value part_value
  ,	CASE WHEN TO_CHAR(y.y,'0000') < TO_NUMBER(TO_CHAR(sysdate,'YYYY'))
	     THEN 'PCTFREE 0 COMPRESS'
        END 
  ,	CASE WHEN TO_CHAR(y.y,'0000') < TO_NUMBER(TO_CHAR(sysdate,'YYYY'))
	     THEN 'PCTFREE 0'
        END 
  FROM	(
	SELECT  k_glb_first_year+rownum-1 y --monthly partitioned data starts in 2012
	FROM	dual
	CONNECT BY level <= k_glb_last_year-k_glb_first_year+1 --2 years of monthly partitioned data
	) y
  ,	(
	SELECT	rownum part_no
        ,       MOD(rownum+1,13) part_value --partition less than value
        , 	LTRIM(TO_CHAR(rownum,'00')) part_name --name of range partition
        ,       FLOOR(rownum/12) y --add to year
	FROM	dual
	CONNECT BY level <= 12 --12 monthly periods
	) p
  WHERE p.part_no>=k_glb_first_month OR y.y>k_glb_first_year;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

  msg('Populating GL List Partitions');
  l_num_rows := 0;
  INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES('GL',1,'ACT_USD','''ACT_USD''');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;
  INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES('GL',2,'ACT_INT','''ACT_INT''');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;
  INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES('GL',3,'ACT_LC' ,'''ACT_LC''');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;
  INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES('GL',9,'Z_OTHERS','DEFAULT');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;

  INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES('GLB',1,'BUDGETS','''BUDGETS''');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;
  INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES('GLB',2,'BDTRAN_EU','''BDTRAN_EU''');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;
  INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES('GLB',3,'BDTRAN_US','''BDTRAN_US''');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;
  INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES('GLB',9,'Z_OTHERS','DEFAULT');
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
  AND    l.part_id IN('GL','GLB');

  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
-----------------------------------------------------------------------------------------------------------
--End GL Metadata
-----------------------------------------------------------------------------------------------------------
  commit;
  msg('GL metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END gl_partdata;
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE GTT METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE gtt_data IS
  k_action CONSTANT VARCHAR2(64) := 'GTT_DATA';
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
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
AND     a.ae_applid IN('FS_CEBD_PROC','FS_CEBD_STAO','FS_CEDT_ECFS','FS_CEDT_PROC'
                      ,'GL_JEDIT','GL_JEDIT2','GL_JEDIT_0','GL_JEDIT_CF0'
                      ,'GL_JEVAT_1','GL_JIUNIT','GL_JRNL_COPY','GL_JRNL_IMP')
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
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
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
  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) VALUES('WL',2,'SELECT_OPEN' ,'''2''');
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
--End GL Metadata
-----------------------------------------------------------------------------------------------------------
  commit;
  msg('GL metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END wl_partdata;
--------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
PROCEDURE index_comp IS
  k_action CONSTANT VARCHAR2(64) := 'INDEX_COMP';
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
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
  where  recname IN('LEDGER','LEDGER_BUDG','S_FT_ACCTDET','S_FT_CICA_SUML','S_FT_USEXP','S_FT_USGEO','S_FT_USIRP','S_FT_USSTAT','S_LEDGER_ACCTS','S_LRG_ACT_CA');
  
  delete from psidxddlparm p
  where  recname IN('LEDGER','LEDGER_BUDG','S_FT_ACCTDET','S_FT_CICA_SUML','S_FT_USEXP','S_FT_USGEO','S_FT_USIRP','S_FT_USSTAT','S_LEDGER_ACCTS','S_LRG_ACT_CA')
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
 
  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 6'
  END
  from  psindexdefn
  where recname = 'S_FT_ACCTDET'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 7'
  END
  from  psindexdefn
  where recname = 'S_FT_CICA_SUML'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 3'
  END
  from  psindexdefn
  where recname = 'S_FT_USEXP'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN 'C' THEN ' COMPRESS 4'
    WHEN '_' THEN ' COMPRESS 3'
  END
  from  psindexdefn
  where recname = 'S_FT_USGEO'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 3'
  END
  from  psindexdefn
  where recname = 'S_FT_USIRP'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN 'C' THEN ' COMPRESS 6'
    WHEN '_' THEN ' COMPRESS 4'
  END
  from  psindexdefn
  where recname = 'S_FT_USSTAT'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 5'
  END
  from  psindexdefn
  where recname = 'S_LEDGER_ACCTS'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN 'A' THEN ' COMPRESS 4'
    WHEN 'B' THEN ' COMPRESS 6'
    WHEN '_' THEN ' COMPRESS 7'
  END
  from  psindexdefn
  where recname = 'S_LRG_ACT_CA'
  and   platform_ora = 1 ;

  update psindexdefn i
  set    platform_ora = 0
  where  recname IN('LEDGER','LEDGER_BUDG','S_FT_ACCTDET','S_FT_CICA_SUML','S_FT_USEXP','S_FT_USGEO','S_FT_USIRP','S_FT_USSTAT','S_LEDGER_ACCTS','S_LRG_ACT_CA')
  and    platform_ora = 1
  and    indexid != '_';

  update psindexdefn i
  set    ddlcount = (SELECT COUNT(DISTINCT platformid) 
                     FROM   psidxddlparm d
                     WHERE  d.recname = i.recname
                     AND    d.indexid = i.indexid)
  where recname IN('LEDGER','LEDGER_BUDG','S_FT_ACCTDET','S_FT_CICA_SUML','S_FT_USEXP','S_FT_USGEO','S_FT_USIRP','S_FT_USSTAT','S_LEDGER_ACCTS','S_LRG_ACT_CA');

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
  gl_partdata;

  msg('Checking Compression Attributes -v- Metadta');
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
,      regexp_substr(pt.tab_storage,'(COMPRESS|NOCOMPRESS)[ [:alnum:]]+',1,1,'i') tab_storage
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
,      COALESCE(regexp_substr(pr.tab_storage,'(COMPRESS|NOCOMPRESS)[ [:alnum:]]+',1,1,'i')
               ,regexp_substr(pt.tab_storage,'(COMPRESS|NOCOMPRESS)[ [:alnum:]]+',1,1,'i')
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
  k_action CONSTANT VARCHAR2(48) := 'PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  truncmeta;

  gl_partdata;
  wl_partdata;
--gtt_data;
  index_comp;

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
EXECUTE gfc_partdata.comp_attrib;

spool off

