--------------------------------------------------------------------------------------
--
-- script        : 
--
-- created by    : DMK
-- creation date : 25.7.2012
--
-- description   : Reload partitioning/archiving metadata
--                 package PL/SQL procedure supercedes simple *-partdata.sql Scripts
--
-- dependencies:  package procedures: psftapi psft_ddl_lock
--
-- development & maintenance history
--
-- date           author            version   reference           description
-- ------------   ----------------- -------   ------------        --------------------
-- <date>         <author name>      <n.n>    IM/PM/RQxxxxxx      <description>
-- ------------   ----------------- -------   ------------        --------------------
-- 29.5.2012      DMK               1.00      CR1152              package x renamed to gfc_archive, procedure main renamed to arch_range_part, procedure fix renamed to fix_overarchive, added purge programs
-- 15.6.2012      DMK               1.01      CR1152              added compression matching/correction processing prior to exchange
--  9.7.2012      DMK               1.02      CR1152              added to enable DML checks
-- 18.7.2012      DMK               1.03      CR1152              enhanced instrumentation via dbms_application_info
-- 29.8.2012      DMK               1.04      CR1152              incorporate psftapi AE message logging utility
-- 29.8.2012      DMK               1.05      CR1152              added new program add_arch_parts to add missing partitions to archive tables
-- 31.8.2012      DMK               1.06      CR1152              add support for list-subpartitions to procedures to add missing partitions
-- 19.9.2012      DMK               1.07      CR1152              added call to psft_ddl_lock package to disable DDL trigger that prevents DDL changes in this session
--                                                                added purge_range_list_subpart to drop unwanted list subpartitions
-- 21.9.2012      DMK               1.08      CR1152              correction to wildcard handling in add_arch_parts and split_arch_parts
-- 15.10.2012     DMK               1.09      CR1152              NO_PUSH_PRED hint for DBA_TAB_PARTITIONS to prevent optimizer bug causing ORA-932
-- 16.10.2012     DMK               1.10      CR1152              correct positioning of QB_NAME hints
-- 11.02.2013     DMK               1.11      CR1152              Added procedure purge_range_part_noarch
--------------------------------------------------------------------------------------
clear screen
set serveroutput on echo on timi on
spool gfc_archive_pkg

-----------------------------------------------------------------------------------------------------------
--The archiving package requires the following explicit privileges in xchg_privs
-----------------------------------------------------------------------------------------------------------
--p_dmlcheck is used to enable/disable checking for duplicates and rows that belong in earlier partitions 
--during the exchange.  This checking has a significant performance overhead.  By default the checking is
--disabled for AUDIT tables, but enabled for all other tables.  This can be overridden by setting the 
--parameter to TRUE to enable checking and FALSE to disable it.
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE sysadm.gfc_archive AS
-----------------------------------------------------------------------------------------------------------
--call all archive programs
-----------------------------------------------------------------------------------------------------------
PROCEDURE main
(p_max_parts INTEGER  DEFAULT NULL
,p_recname   VARCHAR2 DEFAULT NULL
,p_partname  VARCHAR2 DEFAULT NULL
,p_exchvald  BOOLEAN  DEFAULT FALSE --validate partition exchange
,p_dmlcheck  BOOLEAN  DEFAULT NULL  --9.7.2012 added to enable DML checks
,p_online    BOOLEAN  DEFAULT TRUE  --10.2.2013 added to control purge range part
,p_testmode  BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--program to add missing range/list partitions
-----------------------------------------------------------------------------------------------------------
PROCEDURE add_arch_parts 
(p_recname   VARCHAR2 DEFAULT NULL
,p_testmode  BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--archives range partitioned table by exchanging partitions into similarly partitioned archive table
-----------------------------------------------------------------------------------------------------------
PROCEDURE arch_range_part
(p_max_parts INTEGER  DEFAULT NULL
,p_recname   VARCHAR2 DEFAULT NULL
,p_partname  VARCHAR2 DEFAULT NULL
,p_exchvald  BOOLEAN  DEFAULT FALSE --validate partition exchange
,p_dmlcheck  BOOLEAN  DEFAULT NULL  --9.7.2012 added to enable DML checks
,p_testmode  BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--29.5.2012 added purge range partitioned table by droping partitions in live schema
-----------------------------------------------------------------------------------------------------------
PROCEDURE purge_range_part
(p_max_parts INTEGER  DEFAULT NULL
,p_recname   VARCHAR2 DEFAULT NULL
,p_partname  VARCHAR2 DEFAULT NULL
,p_testmode  BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--10.2.2013 added purge range partitioned table with noarch condition
-----------------------------------------------------------------------------------------------------------
PROCEDURE purge_range_part_noarch
(p_max_parts INTEGER  DEFAULT NULL
,p_recname   VARCHAR2 DEFAULT NULL
,p_partname  VARCHAR2 DEFAULT NULL
,p_online    BOOLEAN  DEFAULT TRUE
,p_testmode  BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--19.9.2012 purge list sub-partitions marked for delete
-----------------------------------------------------------------------------------------------------------
PROCEDURE purge_range_list_subpart
(p_max_parts   INTEGER  DEFAULT NULL
,p_recname     VARCHAR2 DEFAULT NULL
,p_partname    VARCHAR2 DEFAULT NULL
,p_subpartname VARCHAR2 DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--29.5.2012 added purge range partitioned table by droping partitions in live archive schema
-----------------------------------------------------------------------------------------------------------
PROCEDURE purge_arch_range_part
(p_max_parts INTEGER  DEFAULT NULL
,p_recname   VARCHAR2 DEFAULT NULL
,p_partname  VARCHAR2 DEFAULT NULL
,p_testmode  BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--moves any rows that match the no-archive condition back to the original table owned by SYSADM
-----------------------------------------------------------------------------------------------------------
PROCEDURE fix_overarchive
(p_recname   VARCHAR2 DEFAULT NULL
,p_testmode  BOOLEAN  DEFAULT FALSE);
END gfc_archive;
/
show errors




-----------------------------------------------------------------------------------------------------------
--The archiving package body 
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY sysadm.gfc_archive AS
l_errno       INTEGER := -20000; /* set a valid default in case of error in trigger*/
l_msg         VARCHAR2(200) := 'Unexpected Error has occured';
e_generate_message EXCEPTION;
k_module  CONSTANT VARCHAR2(48) := $$PLSQL_UNIT;

-----------------------------------------------------------------------------------------------------------
--message severities.  Appears in message log.  Negative severity message not emitted to message log
-----------------------------------------------------------------------------------------------------------
k_numrows  INTEGER := 10; --report number of rows affected by DML
k_sql      INTEGER := 10; --SQL statements 
k_progress INTEGER := 10; --progress messages
k_error    INTEGER := 20; --error messages
k_debug    INTEGER := -1; --debug code
-----------------------------------------------------------------------------------------------------------
--returns number of rows in named table
-----------------------------------------------------------------------------------------------------------
FUNCTION num_rows
(p_table VARCHAR2
) RETURN INTEGER IS
  l_sql VARCHAR2(100);
  l_num_rows INTEGER;
BEGIN
  l_sql := 'SELECT COUNT(*) num_rows FROM '||p_table;
--dbms_output.put_line(p_table);
  EXECUTE IMMEDIATE l_sql INTO l_num_rows;
  dbms_output.put_line('Info: '||p_table||': '||l_num_rows||' rows');
  RETURN l_num_rows;
END num_rows;

-----------------------------------------------------------------------------------------------------------
--converts boolean to displayable varchar
-----------------------------------------------------------------------------------------------------------
FUNCTION display_bool
(p_bool BOOLEAN
) RETURN VARCHAR2 IS
BEGIN
  IF p_bool THEN 
    RETURN 'TRUE';
  ELSE
    RETURN 'FALSE';
  END IF;
END display_bool;

-----------------------------------------------------------------------------------------------------------
--prints message with leading timestamp
-----------------------------------------------------------------------------------------------------------
PROCEDURE msg
(p_msg      VARCHAR2
,p_severity INTEGER DEFAULT 0) IS
BEGIN
  IF p_severity >= 0 THEN
    sysadm.psftapi.message_log(p_message=>p_msg
                              ,p_severity=>p_severity
                              ,p_verbose=>TRUE);
  ELSE
    dbms_output.put_line(TO_CHAR(SYSDATE,'hh24:mi:ss dd.mm.yyyy')||':'||p_msg||'('||p_severity||')');
  END IF;
END msg;

-----------------------------------------------------------------------------------------------------------
--checks whether a table is empty by counting rows in the table
-----------------------------------------------------------------------------------------------------------
PROCEDURE check_empty
(p_table VARCHAR2
,p_testmode BOOLEAN DEFAULT FALSE) IS
  l_num_rows INTEGER;
  l_where    BOOLEAN DEFAULT FALSE;
  l_rownum   BOOLEAN DEFAULT FALSE;
BEGIN
  l_num_rows := num_rows(p_table);

  IF UPPER(p_table) LIKE '%WHERE%' THEN
    l_where := TRUE;
  END IF;
  IF UPPER(p_table) LIKE '%ROWNUM%' THEN
    l_rownum := TRUE;
  END IF;

  IF l_num_rows > 0 THEN
    IF l_rownum THEN
      l_msg := 'Table '||p_table||' has unexpected rows';
    ELSIF l_where THEN
      l_msg := 'Table '||p_table||' has '||l_num_rows||' unexpected rows.';
    ELSE
      l_msg := 'Table '||p_table||' has '||l_num_rows||' rows when it should be empty.';
    END IF;
    IF p_testmode THEN
      msg('Test Mode:'||l_msg,k_debug);
    ELSE
      RAISE e_generate_message;
    END IF;
  END IF;
END check_empty;

-----------------------------------------------------------------------------------------------------------
--executes a dynamic sql statement in a variable
-----------------------------------------------------------------------------------------------------------
PROCEDURE exec_sql
(p_sql      VARCHAR2
,p_testmode BOOLEAN DEFAULT FALSE
) IS
BEGIN
  IF p_testmode THEN NULL;
    msg('Test SQL: '||p_sql,k_sql);
  ELSE
    msg(p_sql,k_sql);
    EXECUTE IMMEDIATE p_sql;
  END IF;
END exec_sql;

-----------------------------------------------------------------------------------------------------------
--disable minimal stats aggregation so that stats aggregation occurs of table exchange
--see http://oracledoug.com/serendipity/index.php?/archives/1570-Statistics-on-Partitioned-Tables-Part-5.html
-----------------------------------------------------------------------------------------------------------
PROCEDURE minimal_stats_aggregation IS
BEGIN 
  EXECUTE IMMEDIATE 'alter session set "_minimal_stats_aggregation" = FALSE';
END minimal_stats_aggregation;

-----------------------------------------------------------------------------------------------------------
--set index tablespace --added 29.8.2012 as a part of add_arch_parts
-----------------------------------------------------------------------------------------------------------
PROCEDURE set_index_tablespace(p_recname   VARCHAR2
,p_part_name VARCHAR2
,p_setattrib BOOLEAN DEFAULT FALSE
,p_testmode  BOOLEAN DEFAULT FALSE) IS
  l_index_prefix VARCHAR2(3) := 'ARC';
  l_sql VARCHAR2(1000);

  k_action CONSTANT VARCHAR2(48) := 'SET_INDEX_TABLESPACE';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_recname);
  msg(k_action||'(recname=>'||p_recname||',part_name=>'||p_part_name||',testmode=>'||display_bool(p_testmode)||')',k_debug);

  FOR i IN(
    SELECT i.indexid
    ,      r.idx_tablespace part_tablespace
    ,      t.idx_tablespace index_tablespace
    FROM   sysadm.gfc_ps_indexdefn i
           LEFT OUTER JOIN sysadm.gfc_part_indexes pi --26.5.2010 only include partitioned indexes
           ON  pi.recname = i.recname
           AND pi.indexid = i.indexid
    ,      sysadm.gfc_part_tables t
    ,      sysadm.gfc_part_ranges r 
    WHERE  t.recname = p_recname
    AND    r.part_id = t.part_id
    AND    r.part_name = p_part_name
    AND    i.recname = t.recname
    AND    i.platform_ora = 1 --1.4.2009
    AND    NVL(pi.part_type,t.part_type) != 'N'
    ORDER BY 1
  ) LOOP
    l_sql := 'ALTER INDEX '||l_index_prefix||i.indexid||p_recname||' MODIFY DEFAULT ATTRIBUTES TABLESPACE ';
    IF p_setattrib AND i.part_tablespace IS NOT NULL THEN
      l_sql := l_sql||i.part_tablespace;
      exec_sql(l_sql,p_testmode);
    ELSIF i.index_tablespace IS NOT NULL THEN
      l_sql := l_sql||i.index_tablespace;
      exec_sql(l_sql,p_testmode);
    END IF;
  END LOOP;

  msg(k_module||'.'||k_action||' completed',k_debug);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END set_index_tablespace;

-----------------------------------------------------------------------------------------------------------
--function to evaluate PeopleSoft storage options --added 29.8.2012 as a part of add_partitions
-----------------------------------------------------------------------------------------------------------
FUNCTION tab_storage
(p_recname VARCHAR2
,p_storage VARCHAR2) RETURN VARCHAR2 IS
  l_storage VARCHAR2(1000 CHAR);
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  k_action CONSTANT VARCHAR2(48) := 'TAB_STORAGE';
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_recname);

  l_storage := p_storage;

  FOR i IN(
    SELECT '**'||d.parmname||'**' parmname
    ,      DECODE(NVL(r.parmvalue,' '),' ',d.parmvalue,r.parmvalue) parmvalue
    FROM   sysadm.psddldefparms d
    ,      sysadm.psrecddlparm r
    WHERE  d.statement_type = 1 /*create table*/
    AND    d.platformid = 2 /*oracle*/
    AND    d.sizing_set = 0 /*just because*/
    AND    r.recname(+) = p_recname
    AND    r.platformid(+) = d.platformid
    AND    r.sizingset(+) = d.sizing_set /*yes, sizingset without an underscore - psft got it wrong*/
    AND    r.parmname(+) = d.parmname
    AND    l_storage LIKE '%**'||d.parmname||'**%'
   ) LOOP
     l_storage := replace(l_storage,i.parmname,i.parmvalue);
  END LOOP;

  msg(k_module||'.'||k_action||' completed',k_debug);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
  RETURN l_storage;
END tab_storage;
-----------------------------------------------------------------------------------------------------------
--function to create list sub-partition clause of add/split partition commands
----------------------------------------------------------------------------------------------------------
FUNCTION tab_listparts(p_recname VARCHAR2
    ,p_part_id VARCHAR2, p_part_name VARCHAR2
    ,p_arch_flag VARCHAR2 DEFAULT NULL) RETURN VARCHAR2 IS --19.3.2010 added subpart
  l_part_def VARCHAR2(1000 CHAR);
  l_counter INTEGER := 0;

  k_action CONSTANT VARCHAR2(48) := 'TAB_LISTPARTS';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_recname);
  msg(k_action||'(recname=>'||p_recname||',part_id=>'||p_part_id||',part_name=>'||p_part_name||',arch_flag=>'||p_arch_flag||')',k_debug);

  --12.2.2008 restrict combinations of partitions
  FOR t IN(
    SELECT a.*
    FROM   sysadm.gfc_part_lists a, sysadm.gfc_part_range_lists b
    WHERE  a.part_id = p_part_id
    AND    b.part_id = a.part_id
    AND    b.range_name = p_part_name
    AND    b.list_name = a.part_name
    AND    b.build = 'Y' --if subpartition to be built in range partition
    AND    (a.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
    ORDER BY a.part_name
  ) LOOP
    
    IF l_counter = 0 THEN
      l_part_def := l_part_def||'(';
    ELSE
      l_part_def := l_part_def||',';
    END IF;
    l_part_def := l_part_def||'SUBPARTITION '||p_recname||'_'||p_part_name||'_'||t.part_name||' VALUES ('||t.list_value||')';
    IF t.tab_tablespace IS NOT NULL THEN
      l_part_def := l_part_def||' TABLESPACE '||t.tab_tablespace;
    END IF;

--3.7.2012 remove physical storage parameters from subpartition definition prior to Oracle 11.2
--  IF t.tab_storage IS NOT NULL AND l_oraver >= 11.2 THEN
--    l_part_def := l_part_def||' '||tab_storage(p_recname, t.tab_storage); --6.9.2007
--  END IF;

    l_counter := l_counter + 1;
  END LOOP;

  IF l_counter > 0 THEN
    l_part_def := l_part_def||')';
  END IF;

  msg(k_module||'.'||k_action||' completed',k_debug);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
  RETURN l_part_def;

END tab_listparts;

-----------------------------------------------------------------------------------------------------------
--program to add missing range partitions by splitting range partitions --added 29.8.2012
-----------------------------------------------------------------------------------------------------------
PROCEDURE split_arch_parts 
(p_recname   VARCHAR2 DEFAULT NULL
,p_testmode  BOOLEAN  DEFAULT FALSE) IS
  l_sql VARCHAR2(1000);
  l_part_def VARCHAR2(1);

  k_action CONSTANT VARCHAR2(48) := 'SPLIT_ARCH_PARTS';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_recname);
  msg(k_action||'(recname=>'||p_recname||',testmode=>'||display_bool(p_testmode)||')',k_debug);

  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);
  FOR i IN (
    SELECT t.table_name
    ,      pt.recname
    ,      pt.part_type, pt.subpart_type, pt.hash_partitions, pt.part_id
    ,      pr.part_no
    ,      pr.part_name      
    ,      pr.part_value
    ,      pr.tab_tablespace 
    ,      pr.idx_tablespace 
    ,      pr.tab_storage    
    ,      pr.idx_storage    
    ,      pr2.part_name      part_name2
    ,      pr2.tab_tablespace tab_tablespace2
    ,      pr2.idx_tablespace idx_tablespace2
    ,      pr2.tab_storage    tab_storage2
    ,      pr2.idx_storage    idx_storage2
    FROM   sysadm.gfc_part_tables pt
    ,      sysadm.gfc_part_ranges pr --the missing partition
    ,      sysadm.gfc_part_ranges pr2 --the partition that exists
    ,      dba_tables t
    ,      sysadm.psrecdefn r
    WHERE  pt.recname = r.recname
    AND    pt.arch_flag = 'A'
    AND    pt.part_type = 'R'
    AND    t.owner = pt.arch_schema
    AND    t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename)
    AND    t.partitioned = 'YES'
    AND    pr.part_id = pt.part_id
    AND    pr.arch_flag = 'A'
    AND    pr2.part_id = pr.part_id
    AND    pr2.part_no = (
              SELECT MIN(pr2a.part_no)
              FROM   sysadm.gfc_part_ranges pr2a
              ,      dba_tab_partitions tp2
              WHERE  pr2a.part_id = pr.part_id
              AND    pr2a.part_no > pr.part_no
              AND    tp2.table_owner = t.owner
              AND    tp2.table_name = t.table_name
              AND    tp2.partition_name = pt.recname||'_'||pr2a.part_name
              )
    AND NOT EXISTS(
           SELECT 'x'
           FROM   dba_tab_partitions tp
           WHERE  tp.table_owner = t.owner
           AND    tp.table_name = t.table_name 
           AND    tp.partition_name = pt.recname||'_'||pr.part_name
           )
    AND EXISTS(
           SELECT 'x'
           FROM   sysadm.gfc_part_ranges pr1
           ,      dba_tab_partitions tp1 --enhancement 23.4.2010
           WHERE  pr1.part_id = pr.part_id
           AND    pr1.part_no > pr.part_no
           AND    tp1.table_owner = t.owner
           AND    tp1.table_name = t.table_name
           AND    tp1.partition_name = pt.recname||'_'||pr1.part_name
           )
    AND    (pt.recname LIKE p_recname OR p_recname IS NULL)
    ORDER BY pr.part_no, pr.part_name  
  ) LOOP
    msg('Part:'||i.part_no||'/'||i.part_name,k_debug);

    --alter indexes to set tablespace to partition tablespace
    set_index_tablespace(p_recname=>i.recname
,p_part_name=>i.part_name
,p_setattrib=>TRUE
,p_testmode=>p_testmode);

    l_sql := 'ALTER TABLE '||i.table_name||' SPLIT PARTITION '||i.recname||'_'||i.part_name||' AT ('||i.part_value||') INTO';
--first partition in split
    l_sql := l_sql||' (PARTITION '||i.recname||'_'||i.part_name;
    IF i.tab_tablespace IS NOT NULL THEN
      l_sql := l_sql || ' TABLESPACE '||i.tab_tablespace;
    END IF;
    IF i.tab_storage IS NOT NULL THEN
      l_sql := l_sql || ' '||tab_storage(i.recname, i.tab_storage); --6.9.2007
    END IF;
    IF i.subpart_type = 'H' AND i.hash_partitions > 1 THEN
      FOR l_subpartition IN 1..i.hash_partitions LOOP
        IF l_subpartition = 1 THEN
          l_part_def := '(';
        ELSE
          l_part_def := ',';
        END IF;
        l_sql := l_sql||' '||l_part_def||'SUBPARTITION '||i.recname||'_'||i.part_name||'_'||LTRIM(TO_CHAR(l_subpartition,'00'));
      END LOOP;
      l_sql := l_sql ||')';
    ELSIF i.subpart_type = 'L' THEN
      l_sql := l_sql ||tab_listparts(p_recname   => i.recname
,p_part_id   => i.part_id
,p_part_name => i.part_name);
    END IF;
--second partition in split
    l_sql := l_sql||' ,PARTITION '||i.recname||'_'||i.part_name2;
    IF i.tab_tablespace2 IS NOT NULL THEN
      l_sql := l_sql || ' TABLESPACE '||i.tab_tablespace2;
    END IF;
    IF i.tab_storage2 IS NOT NULL THEN
      l_sql := l_sql || ' '||tab_storage(i.recname, i.tab_storage2); --6.9.2007
    END IF;
    IF i.subpart_type = 'H' AND i.hash_partitions > 1 THEN
      FOR l_subpartition IN 1..i.hash_partitions LOOP
        IF l_subpartition = 1 THEN
          l_part_def := '(';
        ELSE
          l_part_def := ',';
        END IF;
        l_sql := l_sql||' '||l_part_def||'SUBPARTITION '||i.recname||'_'||i.part_name2||'_'||LTRIM(TO_CHAR(l_subpartition,'00'));
      END LOOP;
      l_sql := l_sql ||')';
    ELSIF i.subpart_type = 'L' THEN
       NULL; --support for list subpartitions to be added here--function tab_listparts()
    END IF;
    l_sql := l_sql ||') UPDATE INDEXES';
    exec_sql(l_sql,p_testmode);

    --alter indexes to set tablespace to index tablespace
    set_index_tablespace(p_recname=>i.recname
,p_part_name=>i.part_name
,p_setattrib=>FALSE
,p_testmode=>p_testmode);
  END LOOP;
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);

  msg(k_module||'.'||k_action||' completed',k_debug);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END split_arch_parts;
-----------------------------------------------------------------------------------------------------------
--program to add missing range/list partitions --added 29.8.2012
--15.10.2012 added NO_PUSH_PRED hint to avoid optimizer error causing ORA-932 in SQL transformation
-----------------------------------------------------------------------------------------------------------
PROCEDURE add_arch_parts 
(p_recname   VARCHAR2 DEFAULT NULL
,p_testmode  BOOLEAN  DEFAULT FALSE) IS
  l_sql VARCHAR2(1000);
  l_part_def VARCHAR2(1);

  k_action CONSTANT VARCHAR2(48) := 'ADD_ARCH_PARTS';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_recname);
  msg(k_action||'(recname=>'||p_recname||',testmode=>'||display_bool(p_testmode)||')',k_debug);

  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);
  FOR i IN (
    SELECT /*+NO_PUSH_PRED(@SUB2 TP1@SUB2)*/ t.table_name
    ,      pt.recname
    ,      pt.part_type, pt.subpart_type, pt.hash_partitions, pt.part_id
    ,      pr.part_no
    ,      pr.part_name      
    ,      pr.part_value
    ,      pr.tab_tablespace 
    ,      pr.idx_tablespace 
    ,      pr.tab_storage    
    ,      pr.idx_storage    
    FROM   sysadm.gfc_part_tables pt
    ,      sysadm.gfc_part_ranges pr
    ,      dba_tables t
    ,      sysadm.psrecdefn r
    WHERE  pt.recname = r.recname
    AND    pt.arch_flag = 'A'
    AND    pt.part_type IN('R','L')
    AND    t.owner = pt.arch_schema
    AND    t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename)
    AND    t.partitioned = 'YES'
    AND    pr.part_id = pt.part_id
    AND    pr.arch_flag = 'A'
    AND NOT EXISTS(
           SELECT /*+QB_NAME(SUB1)*/ 'x'
           FROM   dba_tab_partitions tp
           WHERE  tp.table_owner = t.owner
           AND    tp.table_name = t.table_name 
           AND    tp.partition_name = pt.recname||'_'||pr.part_name
           )
    AND NOT EXISTS(
           SELECT /*+QB_NAME(SUB2)*/ 'x'
           FROM   sysadm.gfc_part_ranges pr1
           ,      dba_tab_partitions tp1 --enhancement 23.4.2010
           WHERE  pr1.part_id = pr.part_id
           AND    pr1.part_no > pr.part_no
           AND    tp1.table_owner = t.owner
           AND    tp1.table_name = t.table_name
           AND    tp1.partition_name = pt.recname||'_'||pr1.part_name
           )
    AND    (pt.recname LIKE p_recname OR p_recname IS NULL)
    ORDER BY part_no, part_name  
  ) LOOP
    msg('Part:'||i.part_no||'/'||i.part_name,k_debug);

    --alter indexes to set tablespace to partition tablespace
    set_index_tablespace(p_recname=>i.recname
                        ,p_part_name=>i.part_name
                        ,p_setattrib=>TRUE
                        ,p_testmode=>p_testmode);

    l_sql := 'ALTER TABLE '||i.table_name||' ADD PARTITION '||i.recname||'_'||i.part_name;
    IF i.part_type = 'R' THEN
      l_sql := l_sql || ' VALUES LESS THAN ('||i.part_value||')';
    ELSIF i.part_type = 'L' THEN /*List*/
      l_sql := l_sql || ' VALUES ('||i.part_value||')';
    END IF;
    IF i.tab_tablespace IS NOT NULL THEN
      l_sql := l_sql || ' TABLESPACE '||i.tab_tablespace;
    END IF;
    IF i.tab_storage IS NOT NULL THEN
      l_sql := l_sql || ' '||tab_storage(i.recname, i.tab_storage); --6.9.2007
    END IF;
    IF i.subpart_type = 'H' AND i.hash_partitions > 1 THEN
      FOR l_subpartition IN 1..i.hash_partitions LOOP
        IF l_subpartition = 1 THEN
          l_part_def := '(';
        ELSE
          l_part_def := ',';
        END IF;
        l_sql := l_sql||' '||l_part_def||'SUBPARTITION '||i.recname||'_'||i.part_name||'_'||LTRIM(TO_CHAR(l_subpartition,'00'));
      END LOOP;
      l_sql := l_sql ||')';
    ELSIF i.subpart_type = 'L' THEN
      l_sql := l_sql ||tab_listparts(p_recname   => i.recname
                                    ,p_part_id   => i.part_id
                                    ,p_part_name => i.part_name);
    END IF;
    exec_sql(l_sql,p_testmode);

    --alter indexes to set tablespace to index tablespace
    set_index_tablespace(p_recname=>i.recname
                        ,p_part_name=>i.part_name
                        ,p_setattrib=>FALSE
                        ,p_testmode=>p_testmode);
  END LOOP;
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);


  msg(k_module||'.'||k_action||' completed',k_debug);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END add_arch_parts;

-----------------------------------------------------------------------------------------------------------
--moves any rows that match the no-archive condition back to the original table owned by SYSADM
-----------------------------------------------------------------------------------------------------------
PROCEDURE fix_overarchive
      (p_recname   VARCHAR2 DEFAULT NULL
,p_testmode  BOOLEAN  DEFAULT FALSE) IS
  l_sql VARCHAR2(1000);
  l_query VARCHAR2(500);
  l_counter INTEGER;

  k_action CONSTANT VARCHAR2(48) := 'FIX_OVERARCHIVE';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_recname);
  msg(k_action||'(recname=>'||p_recname||',testmode=>'||display_bool(p_testmode)||')',k_debug);
  FOR x IN (
    SELECT /*+LEADING(p)*/ ta.owner arch_table_owner
    ,      ta.table_name arch_table_name
    ,      p.recname
    ,      p.noarch_condition
    ,      p.part_id
    ,      p.part_column
    ,      DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename) table_name
    FROM   sysadm.gfc_part_tables p
           LEFT OUTER JOIN sysadm.psrecdefn ra
             ON ra.recname = p.arch_recname
    ,      sysadm.psrecdefn r
    ,      dba_tables ta
    WHERE  r.recname = p.recname
    AND    p.arch_flag = 'A' --archive flag on part tables set to A
    AND    p.noarch_condition IS NOT NULL
    AND    ta.owner = COALESCE(p.arch_schema,p.override_schema,'SYSADM')
    AND    ta.table_name = COALESCE(p.arch_table_name
                                        ,DECODE(ra.sqltablename,' ','PS_'||ra.recname, ra.sqltablename)
                                        ,DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename))
    AND      (p.recname LIKE p_recname OR p_recname IS NULL)
    ORDER BY p.recname
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action, 
                                     action_name=>x.arch_table_owner||'.'||x.arch_table_name);
    l_sql := 'LOCK TABLE '||x.arch_table_owner||'.'||x.arch_table_name||' IN EXCLUSIVE MODE';
    exec_sql(l_sql,p_testmode);

    l_query := 'FROM '||x.arch_table_owner||'.'||x.arch_table_name||' X WHERE '||x.noarch_condition;

    l_sql := 'INSERT INTO sysadm.'||x.table_name||' SELECT * '||l_query;
    exec_sql(l_sql,p_testmode);
    l_counter := SQL%ROWCOUNT;
    msg(TO_CHAR(l_counter)||' rows inserted.',k_numrows);

    IF l_counter > 0 THEN
      l_sql := 'DELETE '||l_query;
      exec_sql(l_sql,p_testmode);
      msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.',k_numrows);
    END IF;

  END LOOP;
  msg(k_module||'.'||k_action||' completed',k_debug);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);

END fix_overarchive;

-----------------------------------------------------------------------------------------------------------
--archives range partitioned table by exchanging partitions into similarly partitioned archive table
-----------------------------------------------------------------------------------------------------------
PROCEDURE arch_range_part
(p_max_parts INTEGER  DEFAULT NULL
,p_recname   VARCHAR2 DEFAULT NULL
,p_partname  VARCHAR2 DEFAULT NULL
,p_exchvald  BOOLEAN  DEFAULT FALSE
,p_dmlcheck  BOOLEAN  DEFAULT NULL  --9.7.2012 added to enable DML checks
,p_testmode  BOOLEAN  DEFAULT FALSE) IS
  l_sql VARCHAR2(1000);
  l_criteria VARCHAR2(200);
  l_table VARCHAR2(200);
  l_counter INTEGER;
  l_high_value VARCHAR2(32767);
  l_validation VARCHAR2(10) := 'WITHOUT';
  l_dmlcheck BOOLEAN;
  k_action CONSTANT VARCHAR2(48) := 'ARCH_RANGE_PART';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                       ,action_name=>''||p_max_parts||'/'||p_recname||'/'||p_partname
                                       ||'/'||display_bool(p_exchvald)||'/'||display_bool(p_dmlcheck));
  msg(k_action||'(recname=>'||p_recname||',partname=>'||p_partname||',max_parts=>'||p_max_parts
              ||',exchvald=>'||display_bool(p_exchvald)||',dmlcheck=>'||display_bool(p_dmlcheck)||',testmode=>'||display_bool(p_testmode)||')',k_debug);

  gfc_archive.minimal_stats_aggregation; --disable minimal stats aggregation
  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);

  IF p_exchvald THEN
    l_validation := 'WITH';
  END IF;    

  FOR x IN (
    WITH x AS (
      SELECT /*+LEADING(p)*/ p1.table_owner
      ,      p1.table_name 
      ,      p1.partition_name
      ,      pr.part_no
      ,      pr.part_name
      ,      ta.owner arch_table_owner
      ,      tx.table_name xchg_table_name
      ,      ta.table_name arch_table_name
      ,      p2.partition_name arch_partition_name
      ,      p2.partition_position
      ,      p.recname
      ,      p.noarch_condition
      ,      p.part_id
      ,      p.part_column
      ,      tx.compression xchg_compression --15.6.2012 added
      ,      p1.compression live_compression --15.6.2012 added
      ,      p2.compression arch_compression --15.6.2012 added
      ,      ROW_NUMBER() OVER (ORDER BY p.recname, pr.part_no) as rownumber
      FROM   sysadm.gfc_part_tables p
             LEFT OUTER JOIN sysadm.psrecdefn ra
               ON ra.recname = p.arch_recname
      ,      sysadm.gfc_part_ranges pr --range meta data
      ,      sysadm.psrecdefn r --ps record defn
      ,      dba_tables tx --exchange table
      ,      dba_tables ta --psarch table
      ,      dba_tab_partitions p1 --sysadm partitions
      ,      dba_tab_partitions p2 --psarch partitions
      WHERE  ta.owner = COALESCE(p.arch_schema,p.override_schema,'SYSADM')
      AND    ta.table_name = COALESCE(p.arch_table_name
                                     ,DECODE(ra.sqltablename,' ','PS_'||ra.recname, ra.sqltablename)
                                     ,DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename))
      AND    r.recname = p.recname
      AND    pr.part_id = p.part_id
      AND    p.arch_flag = 'A' --archive flag on part tables set to A
      AND    pr.arch_flag IN('A','D') --archive flag on part ranges must be set to A or D
      AND    p.subpart_type = 'N' --only deal with subpartitioned tables
      AND    tx.owner = ta.owner
      AND    tx.table_name = 'XCHG_'||p.recname
      AND    p1.table_owner = 'SYSADM'
      AND    p1.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename)
      AND    p1.partition_name = r.recname||'_'||pr.part_name
      AND    p2.table_owner = ta.owner
      AND    p2.table_name = ta.table_name
      AND    p2.partition_name = COALESCE(p.arch_table_name,p.arch_recname,p.recname)||'_'||pr.part_name
--
      AND    (p.recname LIKE p_recname OR p_recname IS NULL)
      AND    (p2.partition_name LIKE p_partname OR p_partname IS NULL)
--testing criteria
--      AND       p.noarch_condition IS NULL --restrict to tables where no data is retained
      )
      SELECT x.* FROM x
      WHERE  (rownumber <= p_max_parts OR p_max_parts IS NULL OR p_max_parts<0) --restrict number of partitions processed
      ORDER BY recname, part_no
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                    ,action_name=>x.table_owner||'.'||x.table_name||'.'||x.partition_name);
    msg('About to archive '||x.table_owner||'.'||x.table_name||' partition '||x.partition_name
                           ||' via table '||x.arch_table_owner||'.'||x.xchg_table_name
                           ||' to '||x.arch_table_owner||'.'||x.arch_table_name||' partition '||x.arch_partition_name,k_progress); 


    --check exchange table is empty
    l_table := x.arch_table_owner||'.'||x.xchg_table_name;
    check_empty(l_table, p_testmode);

    --15.6.2012 added to set compression of exchange table to match partition
    IF x.live_compression != x.xchg_compression THEN
      l_sql  := 'ALTER TABLE '||x.arch_table_owner||'.'||x.xchg_table_name;
      IF x.live_compression = 'DISABLED' THEN
        l_sql  := l_sql||' NOCOMPRESS';
      ELSE
        l_sql  := l_sql||' COMPRESS';
      END IF;
      exec_sql(l_sql,p_testmode);
    END IF;

    --exchange partition in base table with xchange table
    l_sql := 'ALTER TABLE '||x.table_owner||'.'||x.table_name
                        ||' EXCHANGE PARTITION '||x.partition_name
                        ||' WITH TABLE '||x.arch_table_owner||'.'||x.xchg_table_name
                        ||' INCLUDING INDEXES WITHOUT VALIDATION UPDATE GLOBAL INDEXES'; --always done without validation
    exec_sql(l_sql,p_testmode);

    --check partition in app table is empty
    l_table := x.table_owner||'.'||x.table_name||' PARTITION('||x.partition_name||')';
    check_empty(l_table, p_testmode);

    --drop empty partition in app table.  Must be done before noarch condition is processed so rows go into next partition
    l_sql := 'ALTER TABLE '||x.table_owner||'.'||x.table_name
                        ||' DROP PARTITION '||x.partition_name
                        ||' UPDATE GLOBAL INDEXES';
    exec_sql(l_sql,p_testmode);

    --take any data that we need to preserve and put it back in the base table
    --or we could put an on-delete trigger on this table, and have it insert into the base table
    IF x.noarch_condition IS NOT NULL THEN
      l_counter := num_rows(x.arch_table_owner||'.'||x.xchg_table_name);

      l_sql := 'LOCK TABLE '||x.arch_table_owner||'.'||x.xchg_table_name||' IN EXCLUSIVE MODE';
      exec_sql(l_sql,p_testmode);
      l_sql := 'INSERT INTO '||x.table_owner||'.'||x.table_name
                        ||' SELECT * FROM '||x.arch_table_owner||'.'||x.xchg_table_name||' x'
                        ||' WHERE '||x.noarch_condition;
      exec_sql(l_sql,p_testmode);
      msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.',k_numrows);
      l_sql := 'DELETE FROM '||x.arch_table_owner||'.'||x.xchg_table_name||' x'
                        ||' WHERE '||x.noarch_condition;
      exec_sql(l_sql,p_testmode);
      msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.',k_numrows);
    END IF;

    --check partition in archive table is empty
    l_table := x.arch_table_owner||'.'||x.arch_table_name||' PARTITION('||x.arch_partition_name||')';
    check_empty(l_table, p_testmode);

    BEGIN
      --move rows that will exchange into the wrong partition
      --start by getting highest partition boundary
      SELECT p.high_value
      INTO   l_high_value
      FROM   dba_part_tables t, dba_tab_partitions p
      WHERE  t.owner = x.arch_table_owner
      AND    t.table_name = x.arch_table_name
      AND    t.owner = p.table_owner
      AND    t.table_name = p.table_name
      AND    t.partitioning_type = 'RANGE'
      AND    p.partition_position = (
        SELECT MAX(p1.partition_position)   
        FROM   dba_tab_partitions p1
        WHERE  p1.table_owner = p.table_owner
        AND    p1.table_name = p.table_name
        AND    p1.partition_position < x.partition_position);
    EXCEPTION
      WHEN no_data_found THEN 
        l_high_value := '';
    END;

    IF x.part_id LIKE 'AUD%' AND p_dmlcheck IS NULL THEN
     l_dmlcheck := FALSE; --dont check for dups and wrong part in audit
    ELSE
     l_dmlcheck := TRUE;
    END IF;

    IF p_dmlcheck THEN
     --check for rows in exchange table and also in archive table.  Start by getting list of key fields from PeopleTools
     l_counter := 0;
     FOR y IN (
            SELECT fieldname FROM sysadm.psrecfielddb WHERE recname = x.recname AND MOD(useedit,2) = 1
     ) LOOP
      IF l_counter = 0 THEN
       l_criteria := '';
       l_counter := 1;
      ELSE
       l_criteria := l_criteria||',';
      END IF;
      l_criteria := l_criteria||y.fieldname;
     END LOOP;

     --delete rows from archive table that share a key value with exchange table so exchange does not cause duplicate key value
     l_sql := 'DELETE /*+LEADING(X)*/ FROM '||x.arch_table_owner||'.'||x.arch_table_name
             ||' a WHERE ('||l_criteria||') IN (SELECT '||l_criteria||' FROM '||x.arch_table_owner||'.'||x.xchg_table_name||' x';
     IF l_high_value IS NOT NULL THEN
      l_sql := l_sql||' WHERE x.'||x.part_column||' < '||l_high_Value;
     END IF;
     l_sql := l_sql||')';
     exec_sql(l_sql,p_testmode);
     msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.',k_numrows);

     IF l_high_value IS NOT NULL THEN --insert rows from exchange table with values above high part value into arch table and delete from exchange table
       --REPLACE(l_high_Value,'''','''''')
       l_criteria := 'FROM '||x.arch_table_owner||'.'||x.xchg_table_name||' x WHERE x.'||x.part_column||' < '||l_high_Value;
       l_sql := 'INSERT INTO '||x.arch_table_owner||'.'||x.arch_table_name||' SELECT * '||l_criteria;
       exec_sql(l_sql,p_testmode);
       msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.',k_numrows);
       l_sql := 'DELETE '||l_criteria;
       exec_sql(l_sql,p_testmode);
       msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.',k_numrows);
     END IF;
    END IF;

    --15.6.2012 added to set compression of exchange table to match partition
    IF x.arch_compression != x.live_compression THEN
      l_sql  := 'ALTER TABLE '||x.arch_table_owner||'.'||x.arch_table_name||' MOVE PARTITION '||x.arch_partition_name;
      IF x.live_compression = 'DISABLED' THEN
        l_sql  := l_sql||' NOCOMPRESS';
      ELSE
        l_sql  := l_sql||' COMPRESS';
      END IF;
      exec_sql(l_sql,p_testmode);
    END IF;

    --exchange data from exchange staging table into archive table
    l_sql := 'ALTER TABLE '||x.arch_table_owner||'.'||x.arch_table_name
                        ||' EXCHANGE PARTITION '||x.arch_partition_name
                        ||' WITH TABLE '||x.arch_table_owner||'.'||x.xchg_table_name
                        ||' INCLUDING INDEXES '||l_validation||' VALIDATION UPDATE GLOBAL INDEXES';
    exec_sql(l_sql,p_testmode);

    --check that the exchange table is empty
    l_table := x.arch_table_owner||'.'||x.xchg_table_name;
    check_empty(l_table, p_testmode);

    msg('Partition '||x.part_name||' Archived',k_progress);   
--  dbms_output.put_line('.');   

  END LOOP;
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
  msg(k_module||'.'||k_action||' completed');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);

EXCEPTION
  WHEN e_generate_message THEN /*reraise custom exception*/
    sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
    msg(k_module||'.'||k_action||' end with exception',k_error);
    RAISE_APPLICATION_ERROR(l_errno,l_msg);
END arch_range_part;

-----------------------------------------------------------------------------------------------------------
--29.5.2012 added purge range partitioned table by droping partitions in live schema
-----------------------------------------------------------------------------------------------------------
PROCEDURE purge_range_part
(p_max_parts INTEGER  DEFAULT NULL
,p_recname   VARCHAR2 DEFAULT NULL
,p_partname  VARCHAR2 DEFAULT NULL
,p_testmode  BOOLEAN  DEFAULT FALSE) IS
  l_sql VARCHAR2(1000);
  l_criteria VARCHAR2(200);
  l_table VARCHAR2(200);
  l_counter INTEGER := 0;
  l_high_value VARCHAR2(32767);
  k_action CONSTANT VARCHAR2(48) := 'PURGE_RANGE_PART';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                       ,action_name=>''||p_max_parts||'/'||p_recname||'/'||p_partname);
  msg(k_action||'(recname=>'||p_recname||',partname=>'||p_partname||',max_parts=>'||p_max_parts
              ||',testmode=>'||display_bool(p_testmode)||')',k_debug);
  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);
      --
  FOR x IN (
      WITH x AS (
        SELECT /*+LEADING(p)*/ p1.table_owner
        ,         p1.table_name 
        ,         p1.partition_name
        ,         pr.part_no
        ,         pr.part_name
        ,         p.recname
        ,         p.noarch_condition
        ,         p.part_id
        ,         p.part_type
        ,         p.part_column
        ,         ROW_NUMBER() OVER (ORDER BY p.recname, pr.part_no) as rownumber
        FROM      sysadm.gfc_part_tables p
                    LEFT OUTER JOIN sysadm.psrecdefn ra
                    ON ra.recname = p.arch_recname
        ,         sysadm.gfc_part_ranges pr --range meta data
        ,         sysadm.psrecdefn r --ps record defn
        ,         dba_tab_partitions p1 --sysadm partitions
        WHERE     r.recname = p.recname
        AND       pr.part_id = p.part_id
        AND       p.arch_flag IN('A','D') --archive flag on part tables set to D
        AND       pr.arch_flag = 'D' --archive flag on part ranges must be set to D
        AND       p1.table_owner = 'SYSADM'
        AND       p1.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename)
        AND       p1.partition_name = r.recname||'_'||pr.part_name
        AND       p.noarch_condition IS NULL --restrict to tables where no data is retained in which case cannot just drop partitions
--
        AND       (p.recname LIKE p_recname OR p_recname IS NULL)
        AND       (p1.partition_name LIKE p_partname OR p_partname IS NULL)
      )
      SELECT x.* 
      ,      (SELECT COUNT(*)
              FROM   sysadm.gfc_part_indexes i
              WHERE  i.recname = x.recname
              AND    NOT (i.part_id = x.part_id
              AND  i.part_type = x.part_type
              AND  i.part_column = x.part_column)
             ) global_indexes
      FROM x
      WHERE  (rownumber <= p_max_parts OR p_max_parts IS NULL OR p_max_parts<0) --restrict number of partitions processed
      ORDER BY recname, part_no
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                    ,action_name=>x.table_owner||'.'||x.table_name||'.'||x.partition_name);
    msg('About to purge '||x.table_owner||'.'||x.table_name||' partition '||x.partition_name,k_progress);  

    IF x.noarch_condition IS NOT NULL THEN
      --check partition in app table doesn't have any rows that fall within NOARCH condition
      l_table := x.table_owner||'.'||x.table_name||' PARTITION('||x.partition_name||' WHERE ('||x.noarch_condition||') AND ROWNUM=1)';
      check_empty(l_table, p_testmode);
      --qwert: if the table does have noarch rows should we consider moving them into next partition?
    END IF;

    --drop empty partition in app table
    l_sql := 'ALTER TABLE '||x.table_owner||'.'||x.table_name
                           ||' DROP PARTITION '||x.partition_name;
    IF x.global_indexes > 0 THEN
          l_sql := l_sql   ||' UPDATE GLOBAL INDEXES';
    END IF;
    exec_sql(l_sql,p_testmode);

    msg('Partition '||x.part_name||' Purged',k_progress); --corrected 3.7.2012
--  dbms_output.put_line('.'); 
  END LOOP;
  msg(k_module||'.'||k_action||' completed');
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);

EXCEPTION
  WHEN e_generate_message THEN /*reraise custom exception*/
    sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
    msg(k_module||'.'||k_action||' end with exception',k_error);
    RAISE_APPLICATION_ERROR(l_errno,l_msg);
END purge_range_part;

-----------------------------------------------------------------------------------------------------------
--10.2.2013 added purge range partitioned table with noarch condition
-----------------------------------------------------------------------------------------------------------
PROCEDURE purge_range_part_noarch
(p_max_parts INTEGER  DEFAULT NULL
,p_recname   VARCHAR2 DEFAULT NULL
,p_partname  VARCHAR2 DEFAULT NULL
,p_online    BOOLEAN  DEFAULT TRUE
,p_testmode  BOOLEAN  DEFAULT FALSE) IS
  l_sql VARCHAR2(1000);
  l_criteria VARCHAR2(200);
  l_table VARCHAR2(200);
  l_counter INTEGER := 0;
  l_high_value VARCHAR2(32767);
  k_action CONSTANT VARCHAR2(48) := 'PURGE_RANGE_PART_NOARCH';
  l_num_rows INTEGER;
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                       ,action_name=>''||p_max_parts||'/'||p_recname||'/'||p_partname);
  msg(k_action||'(recname=>'||p_recname||',partname=>'||p_partname||',max_parts=>'||p_max_parts
              ||',online=>'||display_bool(p_online)||',testmode=>'||display_bool(p_testmode)||')',k_debug);
  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);

  FOR x IN (
      WITH x AS (
        SELECT /*+LEADING(p)*/ p1.table_owner
        ,        p1.table_name 
        ,        p1.partition_name
        ,        p1.compression     live_compression --15.6.2012 added
        ,        p2.partition_name  partition_name2
        ,        pr2.tab_tablespace tab_tablespace2
        ,        pr2.tab_storage    tab_storage2
        ,        pr.part_no
        ,        pr.part_name
        ,        p.recname
        ,        p.noarch_condition
        ,        p.part_id
        ,        p.part_type
        ,        p.part_column
        ,        tx.owner       xchg_table_owner
        ,        tx.table_name  xchg_table_name
        ,        tx.compression xchg_compression --15.6.2012 added
        ,        ROW_NUMBER() OVER (ORDER BY p.recname, pr.part_no) as rownumber
        FROM     sysadm.gfc_part_tables p
                   LEFT OUTER JOIN sysadm.psrecdefn ra
                   ON ra.recname = p.arch_recname
        ,        sysadm.gfc_part_ranges pr --range meta data
        ,        sysadm.gfc_part_ranges pr2 --range meta data
        ,        sysadm.psrecdefn r --ps record defn
        ,        dba_tab_partitions p1 --sysadm partitions
        ,        dba_tab_partitions p2 --sysadm partitions
        ,        dba_tables tx --exchange table
        WHERE    r.recname = p.recname
        AND      pr.part_id = p.part_id
        AND      pr2.part_id = p.part_id
        AND      pr2.part_id = pr.part_id
        AND      p.arch_flag IN('A','D') --archive flag on part tables set to D
        AND      pr.arch_flag = 'D' --archive flag on part ranges must be set to D
        AND      p1.table_owner = 'SYSADM'
        AND      p1.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename)
        AND      p1.partition_name = r.recname||'_'||pr.part_name
        AND      p2.table_owner = p1.table_owner
        AND      p2.table_name = p1.table_name
        AND      p2.partition_name = r.recname||'_'||pr2.part_name
        AND      pr2.part_no > pr.part_no
        AND      p2.partition_position = p1.partition_position+1
        AND      tx.owner = COALESCE(p.arch_schema,p.override_schema,'SYSADM')
        AND      tx.table_name = 'XCHG_'||p.recname
        AND      p.noarch_condition IS NOT NULL --restrict to tables where there is a noarchive criteria
--
        AND      (p.recname LIKE p_recname OR p_recname IS NULL)
        AND      (p1.partition_name LIKE p_partname OR p_partname IS NULL)
      )
      SELECT x.* 
      ,      (SELECT COUNT(*)
              FROM   sysadm.gfc_part_indexes i
              WHERE  i.recname = x.recname
              AND    NOT (i.part_id = x.part_id
              AND  i.part_type = x.part_type
              AND  i.part_column = x.part_column)
             ) global_indexes
      FROM x
      WHERE  (rownumber <= p_max_parts OR p_max_parts IS NULL OR p_max_parts<0) --restrict number of partitions processed
      ORDER BY recname, part_no
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                    ,action_name=>x.table_owner||'.'||x.table_name||'.'||x.partition_name);
    msg('About to purge '||x.table_owner||'.'||x.table_name||' partition '||x.partition_name||' with no archive condition:'||x.noarch_condition,k_progress);  

    --check exchange table is empty
    l_table := x.xchg_table_owner||'.'||x.xchg_table_name; 
    check_empty(l_table, p_testmode);

    --truncate the exchange table now that we have checked it is empty
    l_sql := 'TRUNCATE TABLE '||x.xchg_table_owner||'.'||x.xchg_table_name;
    exec_sql(l_sql,p_testmode);

    --set compression of exchange table to match partition
    IF x.live_compression != x.xchg_compression THEN
      l_sql  := 'ALTER TABLE '||x.xchg_table_owner||'.'||x.xchg_table_name;
      IF x.live_compression = 'DISABLED' THEN
        l_sql  := l_sql||' NOCOMPRESS';
      ELSE
        l_sql  := l_sql||' COMPRESS';
      END IF;
      exec_sql(l_sql,p_testmode);
    END IF;

    --lock exchange table in exclusive mode
    l_sql := 'LOCK TABLE '||x.xchg_table_owner||'.'||x.xchg_table_name||' IN EXCLUSIVE MODE';
    exec_sql(l_sql,p_testmode);

    --copy noarch rows into exchange table
    l_sql := 'INSERT /*+APPEND*/ INTO '||x.xchg_table_owner||'.'||x.xchg_table_name
           ||' SELECT * FROM '||x.table_owner||'.'||x.table_name||' PARTITION('||x.partition_name||') x'
           ||' WHERE '||x.noarch_condition;
    exec_sql(l_sql,p_testmode);
    l_num_rows := SQL%ROWCOUNT;
    commit;
    msg(TO_CHAR(l_num_rows)||' rows match no archive condition.');

    IF l_num_rows = 0 THEN
      --can just drop partition in app table
      l_sql := 'ALTER TABLE '||x.table_owner||'.'||x.table_name
                             ||' DROP PARTITION '||x.partition_name;
      IF x.global_indexes > 0 THEN
            l_sql := l_sql   ||' UPDATE GLOBAL INDEXES';
      END IF;
      exec_sql(l_sql,p_testmode);
      msg('Partition '||x.part_name||' Purged',k_progress); --corrected 3.7.2012

    ELSIF p_online THEN --handle the rows that must be preserved but assume system online
      --exchange partition in base table with xchange table, first partition just contains noarchive rows
      l_sql := 'ALTER TABLE '||x.table_owner||'.'||x.table_name
                             ||' EXCHANGE PARTITION '||x.partition_name
                             ||' WITH TABLE '||x.xchg_table_owner||'.'||x.xchg_table_name
                             ||' INCLUDING INDEXES WITHOUT VALIDATION UPDATE GLOBAL INDEXES'; --always done without validation
      exec_sql(l_sql,p_testmode);

      --merge first two partitions to second partiton with compression of second partition
      l_sql := 'ALTER TABLE '||x.table_owner||'.'||x.table_name
                             ||' MERGE PARTITIONS '||x.partition_name||', '||x.partition_name2
                             ||' INTO PARTITION '||x.partition_name2;
      IF x.tab_tablespace2 IS NOT NULL THEN
        l_sql := l_sql       ||' TABLESPACE '||x.tab_tablespace2;
      END IF;
      IF x.tab_storage2 IS NOT NULL THEN
        l_sql := l_sql       ||' '||tab_storage(x.recname, x.tab_storage2);
      END IF;
      l_sql := l_sql         ||' UPDATE INDEXES';
      exec_sql(l_sql,p_testmode);

    ELSE --handle the rows that must be preserved but assume system offline
      msg(''||l_num_rows||' rows match no archive condition');

      --drop first partition having preserved rows not to be archived   
      l_sql := 'ALTER TABLE '||x.table_owner||'.'||x.table_name
                             ||' DROP PARTITION '||x.partition_name;
      IF x.global_indexes > 0 THEN
            l_sql := l_sql   ||' UPDATE GLOBAL INDEXES';
      END IF;
      exec_sql(l_sql,p_testmode);

      --copy contents of exchange table back to live partition, but formerly second partition now new first partition
      l_sql := 'INSERT INTO '||x.table_owner||'.'||x.table_name
                             ||' SELECT * FROM '||x.xchg_table_owner||'.'||x.xchg_table_name||' x';
      exec_sql(l_sql,p_testmode);
      msg('Partition '||x.part_name||' Purged',k_progress); --corrected 3.7.2012

    END IF;

    --now clear out exchange table for next time
    l_sql:= 'TRUNCATE TABLE '||x.xchg_table_owner||'.'||x.xchg_table_name;
    exec_sql(l_sql,p_testmode);

  END LOOP;

  msg(k_module||'.'||k_action||' completed');
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);

EXCEPTION
  WHEN e_generate_message THEN /*reraise custom exception*/
    sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
    msg(k_module||'.'||k_action||' end with exception',k_error);
    RAISE_APPLICATION_ERROR(l_errno,l_msg);
END purge_range_part_noarch;

-----------------------------------------------------------------------------------------------------------
--19.9.2012 purge list sub-partitions marked for delete
-----------------------------------------------------------------------------------------------------------
PROCEDURE purge_range_list_subpart
(p_max_parts   INTEGER  DEFAULT NULL
,p_recname     VARCHAR2 DEFAULT NULL
,p_partname    VARCHAR2 DEFAULT NULL
,p_subpartname VARCHAR2 DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
  k_action CONSTANT VARCHAR2(48) := 'PURGE_LIST_SUBPART';
  l_sql VARCHAR2(1000);

  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_recname||'/'||p_partname||'/'||p_subpartname||'/'||p_max_parts||'/'||display_bool(p_testmode));
  msg(k_action||'(recname=>'||p_recname||',partname=>'||p_partname||',subpartname=>'||p_subpartname
              ||',max_parts=>'||p_max_parts
              ||',testmode=>'||display_bool(p_testmode)||')',k_debug);
  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);

  FOR x IN (
    SELECT s1.table_owner, s1.table_name, s1.partition_name, s1.subpartition_name
    FROM   sysadm.gfc_part_tables p
    ,      sysadm.gfc_part_ranges pr --ranges meta data
    ,      sysadm.gfc_part_lists pl --lists meta data
    ,      sysadm.psrecdefn r --ps record defn
    ,      dba_tab_subpartitions s1
    WHERE  r.recname = p.recname
    AND    p.arch_flag IN('A','D') --archive flag on part tables set to D
    AND    p.part_type = 'R'
    AND    p.subpart_type = 'L'
    AND    p.noarch_condition IS NULL --restrict to tables where no data is retain in which case cannot just drop partitions
    AND    pr.part_id = p.part_id
    AND    pl.part_id = p.part_id
    AND    pl.arch_flag = 'D' --archive flag on subpart ranges must be set to D
    AND    s1.table_owner = 'SYSADM'
    AND    s1.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename)
    AND    s1.partition_name = r.recname||'_'||pr.part_name
    AND    s1.subpartition_name = r.recname||'_'||pr.part_name||'_'||pl.part_name
    AND    (p.recname LIKE p_recname OR p_recname IS NULL)
    AND    (s1.partition_name LIKE p_partname OR p_partname IS NULL)
    AND    (s1.subpartition_name LIKE p_partname OR p_partname IS NULL)
    AND    (rownum <= p_max_parts OR p_max_parts IS NULL OR p_max_parts<0) --restrict number of partitions processed
    ORDER BY 1,2,3,4
  ) LOOP
    msg('About to purge '||x.table_owner||'.'||x.table_name||' subpartition '||x.subpartition_name,k_progress);  
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                    ,action_name=>x.table_owner||'.'||x.table_name||'.'||x.subpartition_name);

    --drop empty partition in app table
    l_sql := 'ALTER TABLE '||x.table_owner||'.'||x.table_name
                        ||' DROP PARTITION '||x.subpartition_name
                        ||' UPDATE GLOBAL INDEXES';
    exec_sql(l_sql,p_testmode);

  END LOOP;

  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
  msg(k_module||'.'||k_action||' completed');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END purge_range_list_subpart;

-----------------------------------------------------------------------------------------------------------
--29.5.2012 added purge range partitioned table by droping partitions in live archive schema
-----------------------------------------------------------------------------------------------------------
PROCEDURE purge_arch_range_part
(p_max_parts INTEGER  DEFAULT NULL
,p_recname   VARCHAR2 DEFAULT NULL
,p_partname  VARCHAR2 DEFAULT NULL
,p_testmode  BOOLEAN  DEFAULT FALSE) IS
  l_sql VARCHAR2(1000);
  l_criteria VARCHAR2(200);
  l_table VARCHAR2(200);
  l_counter INTEGER;
  l_high_value VARCHAR2(32767);
  k_action CONSTANT VARCHAR2(48) := 'PURGE_ARCH_RANGE_PART';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                       ,action_name=>''||p_max_parts||'/'||p_recname||'/'||p_partname);
  msg(k_action||'(recname=>'||p_recname||',partname=>'||p_partname||',max_parts=>'||p_max_parts
              ||',testmode=>'||display_bool(p_testmode)||')',k_debug);
  FOR x IN (
    WITH x AS (
      SELECT /*+LEADING(p)*/ p2.table_owner
      ,      p2.table_name 
      ,      p2.partition_name
      ,      pr.part_name
      ,      p.recname
      ,      pr.part_no
      ,      ROW_NUMBER() OVER (ORDER BY p.recname, pr.part_no) as rownumber
      FROM   sysadm.gfc_part_tables p
        LEFT OUTER JOIN sysadm.psrecdefn ra
        ON ra.recname = p.arch_recname
      ,      sysadm.gfc_part_ranges pr --range meta data
      ,      sysadm.psrecdefn r --ps record defn
      ,      dba_tab_partitions p2 --sysadm partitions
      WHERE  r.recname = p.recname
      AND    pr.part_id = p.part_id
      AND    p.arch_flag = 'A' --archive flag on part tables set to A indicating table is archive
      AND    pr.arch_flag = 'D' --archive flag on part ranges must be set to D indicate drop partitions that have been archived
      AND    p2.table_owner = COALESCE(p.arch_schema,p.override_schema,'SYSADM')
      AND    p2.table_name = COALESCE(p.arch_table_name
                                     ,DECODE(ra.sqltablename,' ','PS_'||ra.recname, ra.sqltablename)
                                     ,DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename))
      AND    p2.partition_name = COALESCE(p.arch_table_name,p.arch_recname,p.recname)||'_'||pr.part_name
--
      AND    (p.recname LIKE p_recname OR p_recname IS NULL)
      AND    (p2.partition_name LIKE p_partname OR p_partname IS NULL)
      )
    SELECT   * 
    FROM     x
    WHERE   (rownumber <= p_max_parts OR p_max_parts IS NULL OR p_max_parts<0) --restrict number of partitions processed
    ORDER BY recname, part_no
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                    ,action_name=>x.table_owner||'.'||x.table_name||'.'||x.partition_name);
    msg('About to purge '||x.table_owner||'.'||x.table_name||' partition '||x.partition_name,k_progress);  

    --drop empty partition in app table
    l_sql := 'ALTER TABLE '||x.table_owner||'.'||x.table_name
                           ||' DROP PARTITION '||x.partition_name
                           ||' UPDATE GLOBAL INDEXES';
    exec_sql(l_sql,p_testmode);


    msg('Partition '||x.part_name||' Purged',k_progress); --corrected 3.7.2012
--  dbms_output.put_line('.');   

  END LOOP;
  msg(k_module||'.'||k_action||' completed');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);

EXCEPTION
  WHEN e_generate_message THEN /*reraise custom exception*/
    msg(k_module||'.'||k_action||' end with exception',k_error);
    RAISE_APPLICATION_ERROR(l_errno,l_msg);
END purge_arch_range_part;

-----------------------------------------------------------------------------------------------------------
--call all archive programs
-----------------------------------------------------------------------------------------------------------
PROCEDURE main
(p_max_parts INTEGER  DEFAULT NULL
,p_recname   VARCHAR2 DEFAULT NULL
,p_partname  VARCHAR2 DEFAULT NULL
,p_exchvald  BOOLEAN  DEFAULT FALSE
,p_dmlcheck  BOOLEAN  DEFAULT NULL  --9.7.2012 added to enable DML checks
,p_online    BOOLEAN  DEFAULT TRUE  --10.2.2013 added to control purge range part
,p_testmode  BOOLEAN  DEFAULT FALSE --15.6.2012 added
) IS
  k_action CONSTANT VARCHAR2(48) := 'MAIN';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                  ,action_name=>''||p_max_parts||'/'||p_recname||'/'||p_partname
                                                  ||'/'||display_bool(p_exchvald)||'/'||display_bool(p_dmlcheck));

  msg(k_action||'(recname=>'||p_recname||',partname=>'||p_partname||',max_parts=>'||p_max_parts
              ||',dmlcheck=>'||display_bool(p_dmlcheck)||'),exchvald=>'||display_bool(p_exchvald)
              ||'),testmode=>'||display_bool(p_testmode)||')',k_debug);

  add_arch_parts
  (p_recname  =>p_recname   
  ,p_testmode =>p_testmode);
 
  --31.8.2012 commented out because no need yet in archive schema
  --split_arch_parts
  --(p_max_parts=>p_max_parts 
  --,p_recname  =>p_recname   
  --,p_testmode =>p_testmode);

  purge_range_part
  (p_max_parts=>p_max_parts 
  ,p_recname  =>p_recname   
  ,p_partname =>p_partname  
  ,p_testmode =>p_testmode);

  purge_range_part_noarch
  (p_max_parts=>p_max_parts 
  ,p_recname  =>p_recname   
  ,p_partname =>p_partname  
  ,p_online   =>p_online
  ,p_testmode =>p_testmode);

  arch_range_part
  (p_max_parts=>p_max_parts 
  ,p_recname  =>p_recname   
  ,p_partname =>p_partname  
  ,p_exchvald =>p_exchvald 
  ,p_dmlcheck =>p_dmlcheck
  ,p_testmode =>p_testmode);

  purge_arch_range_part
  (p_max_parts=>p_max_parts 
  ,p_recname  =>p_recname   
  ,p_partname =>p_partname  
  ,p_testmode =>p_testmode);

  msg(k_module||'.'||k_action||' completed');

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END main;

----------------------------------------------------------------------------------------------------
END gfc_archive;
/
----------------------------------------------------------------------------------------------------
show errors
spool off

