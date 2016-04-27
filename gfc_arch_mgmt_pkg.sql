-----------------------------------------------------------------------------------------------------------
--
-- script        : gfc_arch_mgmt_pkg.sql
--
-- created by    : DMK
-- creation date : 04.07.2012
--
-- description   : Space Management package to move/compress/update stats on partitions after archiving etc.
--                 Target tablespaces and storage options taken from meta data created for GFC_PS_PART package
--
-- dependencies:  package procedures: psftapi psft_ddl_lock
--
set echo on serveroutput on
spool gfc_arch_mgmt_pkg
-----------------------------------------------------------------------------------------------------------
--drop sysadm owned version of this package should it still exist
-----------------------------------------------------------------------------------------------------------
DROP PACKAGE sysadm.gfc_arch_mgmt;
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE sys.gfc_arch_mgmt AS
-----------------------------------------------------------------------------------------------------------
-- development and maintenance history
--
-- date           author            version   reference    description
-- ------------   ----------------- -------   ------------ --------------------
-- 04.07.2012     DMK               1.00      MOR          added p_updateind parameters to control UPDATE INDEXES clause
-- 05.07.2012     DMK               1.01      MOR          added parameters to specify partition or subpartititon
-- 06.07.2012     DMK               1.02      MOR          add locate_tab_hash_subparts program
-- 09.07.2012     DMK               1.03      MOR          added tab_stats_job and ind_stats_job programs to submit stats jobs, 
--                                                         removed submit_jobs program from package header
-- 10.07.2012     DMK               1.04      MOR          high_block_segment rewritten to handle 10g restriction on directly specifying subpartition storage options
-- 19.07.2012     DMK               1.05      MOR          tree_free_space procedure added
-- 29.08.2012     DMK               1.06      MOR          correction to block overhead estimate 2.num_rows/blocks
-- 19.09.2012     DMK               1.07      MOR          added call to psft_ddl_lock package to disable DDL trigger that prevents DDL changes in this session
-- 19.10.2012     DMK               1.10      MOR          high_block_segments, trim_free_space, submit_jobs programs removed from package now owned by SYSADM
-- 22.10.2012     DMK               1.11      MOR          added p_parallel parameters to control parallelism of move/rebuild operations
-- 23.10.2012     DMK               1.12      MOR          back to owned by SYS, added kswrdt to log to alert log/trace file. 
--                                                         Move the development and maintenance history section 
--                                                         to within the package itself. Removed example commands from the end of the script.
-- 16.01.2013     DMK               1.14      MOR          Default parallelism parameter to false, add last analyzed to logging, 
--                                                         remove absolute parallelism suppression logic, 
--                                                         Minimum number of blocks for parallel table compression calculated dynamically
-- 31.01.2013     DMK               1.15      MOR          Added USE_NL hints to access DBA_SEGMENTS and DBA_%PARTITIONS, corrections to parallelism, limitations to when to compress
-----------------------------------------------------------------------------------------------------------
--General note on parameters used in subprograms in this package
-----------------------------------------------------------------------------------------------------------
--p_part_id    - Name of partitioning strategy in meta-data for GFC_PSPART pacakge
--p_recname    - PeopleSoft record name (as opposed to Oracle table name) - uses LIKE pattern match
--p_partname   - Oracle partition name (or subpartition where appropriate and there is no separate subpartition parameter) - uses LIKE pattern match
--p_tablespace - Oracle tablespace name - uses LIKE pattern match
--p_updateind  - If true include UPDATE INDEXES clause in ALTER TABLE command, if false then do not
--p_statsjob   - If true the submit job to collect stats on partition that has just been moved/rebuilt
--p_max_parts  - Maximum number of partitions to be acted upon.  If null then no limit
--p_testmode   - If true do not submit SQL to database only echo to screen
-----------------------------------------------------------------------------------------------------------
--rebuilds table partitions which are either in the wrong tablespace, or have the wrong compression
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_tab_parts
      (p_part_id     VARCHAR2 DEFAULT '%'
      ,p_recname     VARCHAR2 DEFAULT '%'
      ,p_partname    VARCHAR2 DEFAULT '%'
      ,p_tablespace  VARCHAR2 DEFAULT '%'
      ,p_updateind   BOOLEAN  DEFAULT TRUE --added 4.7.2012 to control whether automatically update indexes
      ,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
      ,p_parallel    BOOLEAN  DEFAULT FALSE --added 22.10.2012 to control parallel move/rebuild of partitions, 16.1.2013 default false
      ,p_max_parts   INTEGER  DEFAULT NULL
      ,p_testmode    BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--rebuilds table partitions which are either in the wrong tablespace, or have the wrong compression
--NB subpartitions must have reasonably accurate statistics becauses compression is determined by comparing
--computed size of rows (average row length*num_rows/blocks) without compression with available blocksize 
--after allowing for free space.  Uncompressed blocks <100% compressed blocks typically over 400
--performance of driving query is not good
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_tab_subparts
      (p_part_id     VARCHAR2 DEFAULT '%'
      ,p_recname     VARCHAR2 DEFAULT '%'
      ,p_partname    VARCHAR2 DEFAULT '%' --added 5.7.2012 specific partition
      ,p_subpartname VARCHAR2 DEFAULT '%' --added 5.7.2012 specific subpartition
      ,p_tablespace  VARCHAR2 DEFAULT '%'
      ,p_updateind   BOOLEAN  DEFAULT TRUE --added 4.7.2012 to control whether automatically update indexes
      ,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
      ,p_parallel    BOOLEAN  DEFAULT FALSE --added 22.10.2012 to control parallel move/rebuild of partitions, 16.1.2013 default false
      ,p_max_parts   INTEGER  DEFAULT NULL
      ,p_testmode    BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--added 6.7.2012: like locate_tab_subparts, but for hash subpartitioned tables
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_tab_hash_subparts
      (p_part_id     VARCHAR2 DEFAULT '%'
      ,p_recname     VARCHAR2 DEFAULT '%'
      ,p_partname    VARCHAR2 DEFAULT '%'
      ,p_tablespace  VARCHAR2 DEFAULT '%'
      ,p_updateind   BOOLEAN  DEFAULT TRUE --added 4.7.2012 to control whether automatically update indexes
      ,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
      ,p_parallel    BOOLEAN  DEFAULT FALSE --added 22.10.2012 to control parallel move/rebuild of partitions, 16.1.2013 default false
      ,p_max_parts   INTEGER  DEFAULT NULL
      ,p_testmode    BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--rebuilds index partitions which are in the wrong tablespace
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_ind_parts
      (p_part_id     VARCHAR2 DEFAULT '%'
      ,p_recname     VARCHAR2 DEFAULT '%'
      ,p_partname    VARCHAR2 DEFAULT '%'
      ,p_tablespace  VARCHAR2 DEFAULT '%'
      ,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
      ,p_max_parts   INTEGER  DEFAULT NULL
      ,p_testmode    BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--rebuild index subpartitions 
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_ind_subparts
      (p_part_id     VARCHAR2 DEFAULT '%'
      ,p_recname     VARCHAR2 DEFAULT '%'
      ,p_partname    VARCHAR2 DEFAULT '%' --added 10.7.2012 specific partition
      ,p_subpartname VARCHAR2 DEFAULT '%' --added 10.7.2012 specific subpartition
      ,p_tablespace  VARCHAR2 DEFAULT '%'
      ,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
      ,p_max_parts   INTEGER  DEFAULT NULL
      ,p_testmode    BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--rebuild lob partitions in correct tablespaces
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_lob_parts
      (p_part_id     VARCHAR2 DEFAULT '%'
      ,p_recname     VARCHAR2 DEFAULT '%'
      ,p_partname    VARCHAR2 DEFAULT '%'
      ,p_tablespace  VARCHAR2 DEFAULT '%'
      ,p_updateind   BOOLEAN  DEFAULT TRUE --added 4.7.2012 to control whether automatically update indexes
      ,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
      ,p_parallel    BOOLEAN  DEFAULT FALSE --added 22.10.2012 to control parallel move/rebuild of partitions, 16.1.2013 default false
      ,p_max_parts   INTEGER  DEFAULT NULL
      ,p_testmode    BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--added 10.7.2012 to call all partition management programs in a single call
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_all_parts
      (p_part_id     VARCHAR2 DEFAULT '%'
      ,p_recname     VARCHAR2 DEFAULT '%'
      ,p_partname    VARCHAR2 DEFAULT '%'
      ,p_subpartname VARCHAR2 DEFAULT '%' 
      ,p_tablespace  VARCHAR2 DEFAULT '%'
      ,p_updateind   BOOLEAN  DEFAULT TRUE 
      ,p_statsjob    BOOLEAN  DEFAULT TRUE 
      ,p_parallel    BOOLEAN  DEFAULT FALSE --added 22.10.2012 to control parallel move/rebuild of partitions, 16.1.2013 default false
      ,p_max_parts   INTEGER  DEFAULT NULL
      ,p_testmode    BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
END gfc_arch_mgmt;
/


----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY sys.gfc_arch_mgmt AS
-----------------------------------------------------------------------------------------------------------
k_module      CONSTANT VARCHAR2(48) := $$PLSQL_UNIT;
-----------------------------------------------------------------------------------------------------------
--23.10.2012 parameters to dbms_system.kswrdt 
-----------------------------------------------------------------------------------------------------------
k_trace       CONSTANT INTEGER := 1; --write to session trace file only
k_alert       CONSTANT INTEGER := 2; --write to database alert log only
k_trace_alert CONSTANT INTEGER := 3; --write to both alert log and trace file
-----------------------------------------------------------------------------------------------------------
k_typical_compression  CONSTANT NUMBER  := 4.2; --typical table compression factor for use in calculating maximum parallelism
--compress threshold set to 110 because package uncompressed blocks sometimes give a compression factor of 102% but when compressed I never get less than 190%
k_compress_threshold   CONSTANT INTEGER := 99;  --changed 31.1.2013 --segments with compress factor < this value deemed to be uncompressed
k_decompress_threshold CONSTANT INTEGER := 101; --changed 31.1.2013 --segments with compression factor >= this value deemed to be compressed
k_min_blocks_threshold CONSTANT INTEGER := 16;  --added 31.1.2013 --minimum number of blocks that is worth compressing
-----------------------------------------------------------------------------------------------------------
--18.1.2013 Package global variable calculate from as product of Oracle initialisation parameters cpu_count 
--and parallel_threads_per_cpu used to limit specified parallelism and mimimise space wastage during compression
g_max_parallelism INTEGER; --calculate as 
-----------------------------------------------------------------------------------------------------------
--ORA-14327:Some index [sub]partitions could not be rebuilt 
--Occurs when PSFT_DDL_LOCK trigger enabled during parallel index rebuild
-----------------------------------------------------------------------------------------------------------
e_partition_index_rebuild_fail EXCEPTION;
PRAGMA EXCEPTION_INIT(e_partition_index_rebuild_fail,-14327);
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
--23.10.2012 added p_dest: 1=trace file, 2=alert log, 3=trace+alert log
-----------------------------------------------------------------------------------------------------------
PROCEDURE msg
(p_msg VARCHAR2
,p_dest NUMBER DEFAULT 1 --added 23.10.2012 by default write message to trace file
) IS
BEGIN
  dbms_output.put_line(TO_CHAR(SYSDATE,'hh24:mi:ss dd.mm.yyyy')||':'||p_msg); --emit log to client terminal
  sys.dbms_system.ksdwrt(p_dest, p_msg); --23.10.2012 write to session trace and/or alert log - no need to add timestamp
END msg;

-----------------------------------------------------------------------------------------------------------
--executes a dynamic sql statement in a variable
-----------------------------------------------------------------------------------------------------------
PROCEDURE exec_sql
(p_sql VARCHAR2
,p_testmode BOOLEAN DEFAULT FALSE
) IS
BEGIN
    IF p_testmode THEN NULL;
     msg('Test SQL: '||p_sql||';');
    ELSE
     msg(p_sql);
     EXECUTE IMMEDIATE p_sql;
    END IF;
END exec_sql;

-----------------------------------------------------------------------------------------------------------
--9.7.2012 added to submit asynchronous scheduled job to collect table partition stats
-----------------------------------------------------------------------------------------------------------
PROCEDURE tab_stats_job 
      (p_ownname     VARCHAR2
      ,p_tabname     VARCHAR2
      ,p_partname    VARCHAR2 DEFAULT NULL
      ,p_granularity VARCHAR2 DEFAULT NULL
      ,p_parallel    BOOLEAN  DEFAULT NULL --22.10.2012 added parameter
      ,p_testmode    BOOLEAN  DEFAULT FALSE) IS
  k_action CONSTANT VARCHAR2(48) := 'TAB_STATS_JOB';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_msg VARCHAR2(100);
  l_cmd VARCHAR2(1000);
  l_job_name VARCHAR2(30);
  e_job_already_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_job_already_exists,-27477);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                  ,action_name=>p_ownname||'/'||p_tabname||'/'||p_partname||'/'||p_granularity);
  msg(k_module||'.'||k_action||'(ownname=>'||p_ownname||',tabname=>'||p_tabname||',partname=>'||p_partname
              ||',granularity=>'||p_granularity||',testmode=>'||display_bool(p_testmode)||')');
  l_msg := 'Collect statistics on table '||LTRIM(lower(p_granularity)||' ')||p_ownname||'.'||p_tabname||'.'||p_partname;

  l_cmd := 'BEGIN sys.dbms_stats.gather_table_stats(ownname=>'''||p_ownname||''',tabname=>'''||p_tabname||'''';
  IF p_partname IS NOT NULL THEN
    l_cmd := l_cmd||',partname=>'''||p_partname||'''';
  END IF;
  l_cmd := l_cmd||',estimate_percent=>DBMS_STATS.AUTO_SAMPLE_SIZE';
  IF p_granularity IS NOT NULL THEN
    l_cmd := l_cmd||',granularity=>'''||p_granularity||'''';
  END IF;
  IF p_parallel THEN --22.10.2012 added
    l_cmd := l_cmd||',degree=>DBMS_STATS.DEFAULT_DEGREE';
  END IF;
  l_cmd := l_cmd||',method_opt=>''FOR ALL COLUMNS SIZE REPEAT'',cascade=>TRUE); END;';
  msg(l_cmd);
  l_job_name := SUBSTR(p_ownname||'_'||NVL(p_partname,p_tabname),1,30);

  IF NOT p_testmode THEN
    LOOP
      BEGIN
        sys.dbms_scheduler.create_job
        (job_name   => l_job_name
        ,job_type   => 'PLSQL_BLOCK'
        ,job_action => l_cmd
        ,start_date => SYSTIMESTAMP --run job immediately
        ,enabled    => TRUE --job is enabled
        ,auto_drop  => TRUE --request will be dropped when complete
        ,comments   => l_msg
        );
        EXIT;
      EXCEPTION 
        WHEN e_job_already_exists THEN 
          l_job_name := SUBSTR(l_job_name||'_',1,24)||TO_CHAR(SYSDATE,'HH24MISS');
      END;
    END LOOP;
    msg('Submitted Job '||l_job_name||': '||l_msg);
  END IF;
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END tab_stats_job;
-----------------------------------------------------------------------------------------------------------
--added 9.7.2012 to submit asynchronous scheduled job to collect index partition stats
-----------------------------------------------------------------------------------------------------------
PROCEDURE ind_stats_job 
      (p_ownname     VARCHAR2
      ,p_indname     VARCHAR2
      ,p_partname    VARCHAR2 DEFAULT NULL
      ,p_granularity VARCHAR2 DEFAULT NULL
      ,p_parallel    BOOLEAN  DEFAULT NULL --22.10.2012 added parameter
      ,p_testmode    BOOLEAN  DEFAULT FALSE) IS
  k_action CONSTANT VARCHAR2(48) := 'IND_STATS_JOB';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_msg VARCHAR2(100);
  l_cmd VARCHAR2(1000);
  l_job_name VARCHAR2(30);
  e_job_already_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_job_already_exists,-27477);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                  ,action_name=>p_ownname||'/'||p_indname||'/'||p_partname||'/'||p_granularity);
  msg(k_module||'.'||k_action||'(ownname=>'||p_ownname||',indname=>'||p_indname||',partname=>'||p_partname
              ||',granularity=>'||p_granularity||',testmode=>'||display_bool(p_testmode)||')');
  l_msg := 'Collect statistics on index '||ltrim(lower(p_granularity)||' ')||p_ownname||'.'||p_indname||'.'||p_partname;

  l_cmd := 'BEGIN sys.dbms_stats.gather_index_stats(ownname=>'''||p_ownname||''',indname=>'''||p_indname||'''';
  IF p_partname IS NOT NULL THEN
    l_cmd := l_cmd||',partname=>'''||p_partname||'''';
  END IF;
  l_cmd := l_cmd||',estimate_percent=>DBMS_STATS.AUTO_SAMPLE_SIZE); END;';
  IF p_granularity IS NOT NULL THEN
    l_cmd := l_cmd||',granularity=>'''||p_granularity||'''';
  END IF;
  IF p_parallel THEN --22.10.2012 added
    l_cmd := l_cmd||',degree=>DBMS_STATS.DEFAULT_DEGREE';
  END IF;
  msg(l_cmd);
  l_job_name := SUBSTR(p_ownname||'_'||NVL(p_partname,p_indname),1,30);

  IF NOT p_testmode THEN
    LOOP
      BEGIN
        sys.dbms_scheduler.create_job
        (job_name   => l_job_name
        ,job_type   => 'PLSQL_BLOCK'
        ,job_action => l_cmd
        ,start_date => SYSTIMESTAMP --run job immediately
        ,enabled    => TRUE --job is enabled
        ,auto_drop  => TRUE --request will be dropped when complete
        ,comments   => l_msg
        ); 
        EXIT;
      EXCEPTION 
        WHEN e_job_already_exists THEN 
          l_job_name := SUBSTR(l_job_name||'_',1,24)||TO_CHAR(SYSDATE,'HH24MISS');
      END;
    END LOOP;
    msg('Submitted Job '||l_job_name||': '||l_msg);
  END IF;
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END ind_stats_job;

-----------------------------------------------------------------------------------------------------------
--18.1.2013 read Oracle initialisation parameters into package global variables
-----------------------------------------------------------------------------------------------------------
PROCEDURE read_init_params IS
  k_action CONSTANT VARCHAR2(48) := 'READ_INIT_PARAMS';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_cpu_count                INTEGER;
  l_parallel_threads_per_cpu INTEGER;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action,action_name=>'');
  IF g_max_parallelism IS NULL THEN

    SELECT TO_NUMBER(value)
    INTO   l_cpu_count
    FROM   v$parameter
    WHERE  name = 'cpu_count';

    SELECT TO_NUMBER(value)
    INTO   l_parallel_threads_per_cpu
    FROM   v$parameter
    WHERE  name = 'parallel_threads_per_cpu';

    g_max_parallelism := l_cpu_count * l_parallel_threads_per_cpu;
    msg('cpu_count='||l_cpu_count||',parallel_threads_per_cpu='||l_parallel_threads_per_cpu||
        ',max_parallelism='||g_max_parallelism);

  END IF;
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END read_init_params;
-----------------------------------------------------------------------------------------------------------
--rebuilds table partitions which are either
--1:in the wrong tablespace
--2:have the wrong compression
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_tab_parts
(p_part_id     VARCHAR2 DEFAULT '%'
,p_recname     VARCHAR2 DEFAULT '%'
,p_partname    VARCHAR2 DEFAULT '%'
,p_tablespace  VARCHAR2 DEFAULT '%'
,p_updateind   BOOLEAN  DEFAULT TRUE --added 4.7.2012 to control whether automatically update indexes
,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
,p_parallel    BOOLEAN  DEFAULT FALSE --added 22.10.2012 to control parallel move/rebuild of partitions, 16.1.2013 default false
,p_max_parts   INTEGER  DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
  k_action      CONSTANT VARCHAR2(48) := 'LOCATE_TAB_PARTS';
  l_module      VARCHAR2(48);
  l_action      VARCHAR2(32);
  l_sql         VARCHAR2(1000);
  l_parallel    BOOLEAN; --added 22.10.2012 to control parallel move/rebuild of partitions
  l_parallelism NUMBER; --added 18.1.2013 maximum parallelism avoiding space wastage
  l_typical_compression NUMBER :=1; --estimated compression factor
BEGIN 
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                  ,action_name=>p_part_id||'/'||p_recname||'/'||p_partname||'/'||p_tablespace||'/'||p_max_parts);
  msg(k_module||'.'||k_action||'(part_id=>'||p_part_id||',recname=>'||p_recname||',partname=>'||p_partname
              ||',tablespace=>'||p_tablespace||',updateind=>'||display_bool(p_updateind)||',statsjob=>'||display_bool(p_statsjob)
              ||',max_parts=>'||p_max_parts||',testmode=>'||display_bool(p_testmode)||')',k_trace_alert);

  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);
  read_init_params;

  FOR i IN (
      --31.1.2013 USE_NL hint added, parallel hint corrected
      WITH x AS(
  	SELECT /*+LEADING(t r rp) USE_NL(p b) PARALLEL(rp) PARALLEL(p) PARALLEL(b)*/ 
             t.recname
      ,      t.part_id
      ,      p.table_owner
	,      p.table_name
	,      t.recname||'_'||rp.part_name partition_name
	,      COALESCE(rp.tab_tablespace,t.tab_tablespace) tab_tablespace
	,      p.tablespace_name
	,      UPPER(NVL(rp.tab_storage,t.tab_storage)) tab_storage
	,      p.compression
      ,      p.num_rows
	,      p.blocks
	,      p.last_analyzed
      ,      s.block_size
      ,      p.initial_extent
      ,      p.pct_free
      ,      CEIL(p.num_rows*p.avg_row_len/(1-p.pct_free/100)
                 /(s.block_size-(57+23*p.ini_trans+2*p.num_rows/LEAST(NULLIF(p.blocks,0), b.blocks)+4))) est_blocks
	,      100*(p.num_rows*p.avg_row_len/(1-p.pct_free/100))
             /(
              LEAST(NULLIF(p.blocks,0), b.blocks)
              *(s.block_size-(57+23*p.ini_trans+2*p.num_rows/LEAST(NULLIF(p.blocks,0), b.blocks)+4)) --blocksize less est header overhead
              ) block_pct
	,	 b.header_file, b.header_block, b.extents
      FROM   sysadm.gfc_part_tables t
	,      sysadm.psrecdefn r
	,      sysadm.gfc_part_ranges rp
	,      dba_tab_partitions p
      ,      dba_segments b
      ,      dba_tablespaces s
      WHERE  b.segment_type = 'TABLE PARTITION'
      AND    b.owner = p.table_owner
      AND    b.segment_name = p.table_name
      AND    b.partition_name = p.partition_name
      AND    b.tablespace_name = p.tablespace_name
      AND    s.tablespace_name = p.tablespace_name
      AND    s.tablespace_name = b.tablespace_name --added 31.1.2013
      AND    t.part_type = 'R'
      AND    t.subpart_type = 'N' --not subpartitions
      AND    t.part_id = rp.part_id
      AND    p.table_owner IN('SYSADM',t.arch_schema)
      AND    b.owner IN('SYSADM',t.arch_schema) --added 31.1.2013
      AND    r.recname = t.recname
      AND    p.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
      AND    b.segment_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
      AND    p.partition_name = t.recname||'_'||rp.part_name
      AND    b.partition_name = t.recname||'_'||rp.part_name
      AND    s.status = 'ONLINE'
      ) 
      SELECT x.* 
      FROM   x
      WHERE  (x.tablespace_name != x.tab_tablespace
      OR     (    (x.compression != 'ENABLED' OR x.pct_free > 0 OR x.block_pct < k_compress_threshold) 
              AND x.tab_storage LIKE '%COMPRESS%' AND NOT x.tab_storage LIKE '%NOCOMPRESS%' 
              AND x.extents > 1 --added 31.1.2013
              AND x.blocks >= k_min_blocks_threshold --added 31.1.2013
              AND x.est_blocks < x.blocks --added 31.1.2013 no point compressing if we do not think we will save space
              AND x.est_blocks > 5
             )
      OR     (x.compression  = 'ENABLED' AND (NOT x.tab_storage LIKE '%COMPRESS%' OR x.tab_storage IS NULL)))
--
      AND    x.part_id LIKE p_part_id
      AND    x.recname LIKE p_recname 
      AND    x.partition_name LIKE p_partname 
      AND 	 (rownum <= p_max_parts OR p_max_parts IS NULL)
      AND    x.tab_tablespace LIKE p_tablespace 
      ORDER BY x.tablespace_name, x.header_file, x.header_block
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>i.partition_name);
    msg('Partition '||i.partition_name||':'||i.compression||'('||LTRIM(TO_CHAR(i.block_pct,'9999'))||'%,'
                    ||'est. '||i.est_blocks||' comp. blocks, ' --added 31.1.2013
                    ||i.num_rows||' rows, '||i.blocks||' blocks, '||i.extents||' extents, ' --31.1.2013 added extents to message
                    ||i.pct_free||'% free, analyzed '||TO_CHAR(i.last_analyzed,'dd.mm.yyyy hh24:mi:ss')||') -> '||i.tab_storage);

    l_parallel := p_parallel; --22.10.2012 set local variable to parameter

    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name
	||' MOVE PARTITION '||i.partition_name
	||' TABLESPACE '||i.tab_tablespace;
	
    IF p_updateind THEN --added 4.7.2012 to control whether automatically update indexes
      l_sql := l_sql||' UPDATE INDEXES';
--16.1.2013 - we will no longer suppress parallelism if cascading to indexes, instead parallelism is not invoked by default
--    l_parallel := FALSE; --22.10.2012 suppress parallelism if cascading rebuild to indexes because PSFT_DDL_LOCK trigger cannot read package global variables in query slave sessions
    END IF;

    IF i.tab_storage LIKE '%COMPRESS%' THEN
      l_typical_compression := k_typical_compression;
    ELSE
      l_sql := l_sql||' NOCOMPRESS';
      l_typical_compression := 1;
    END IF;

    IF i.tab_storage IS NOT NULL THEN
      l_sql := l_sql||' '||i.tab_storage;
    END IF;

    IF l_parallel THEN --18.1.2013-determine how much parallelism
      --parallel rebuild can waste space on small objects so estimate number of extent and use that as parallelism
      l_parallelism := FLOOR((i.est_blocks/l_typical_compression)/(i.initial_extent/i.block_size));
      IF l_parallelism <= 0 THEN
        l_parallel := FALSE;
      END IF;
      msg('estblocks='||i.est_blocks||',typical_compression='||l_typical_compression||
         ',initial_extent='||i.initial_extent||',block_size='||i.block_size||
         ',max_parallelism='||g_max_parallelism||
         ',parallelism='||l_parallelism);
    END IF;

    IF l_parallel THEN
      l_sql := l_sql||' PARALLEL';
      IF l_parallelism > 0 AND l_parallelism <= g_max_parallelism THEN
        l_sql := l_sql||' '||l_parallelism;
      END IF;
    ELSIF NOT l_parallel THEN --22.10.2012 no keyword if null
      l_sql := l_sql||' NOPARALLEL';
    END IF;

    exec_sql(l_sql,p_testmode);

    IF p_statsjob THEN --added 9.7.2012
      tab_stats_job(p_ownname=>i.table_owner
                   ,p_tabname=>i.table_name
                   ,p_partname=>i.partition_name
                   ,p_granularity=>'PARTITION'
                   ,p_parallel=>p_parallel --added 22.10.2012
                   ,p_testmode=>p_testmode);
    END IF;

    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name||' NOPARALLEL';
    exec_sql(l_sql,p_testmode);

  END LOOP;
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
  msg(k_module||'.'||k_action||' completed',k_trace_alert);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
EXCEPTION 
  WHEN e_partition_index_rebuild_fail THEN
    msg('Parallel index partition rebuild failed - Check PSFT_DDL_LOCK trigger disabled');
    dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
    RAISE;
END locate_tab_parts;
 
-----------------------------------------------------------------------------------------------------------
--relocate/compress subpartitions--10g solution, revisit in 11g when can specify storage on subpartition
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_tab_subparts
(p_part_id     VARCHAR2 DEFAULT '%'
,p_recname     VARCHAR2 DEFAULT '%'
,p_partname    VARCHAR2 DEFAULT '%' --added 5.7.2012 specific partition
,p_subpartname VARCHAR2 DEFAULT '%' --added 5.7.2012 specific partition
,p_tablespace  VARCHAR2 DEFAULT '%'
,p_updateind   BOOLEAN  DEFAULT TRUE --added 4.7.2012 to control whether automatically update indexes
,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
,p_parallel    BOOLEAN  DEFAULT FALSE --added 22.10.2012 to control parallel move/rebuild of partitions, 16.1.2013 default false
,p_max_parts   INTEGER  DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
  k_action      CONSTANT VARCHAR2(48) := 'LOCATE_TAB_SUBPARTS';
  l_module      VARCHAR2(48);
  l_action      VARCHAR2(32);
  l_sql         VARCHAR2(1000);
  l_pre_tabpart_storage VARCHAR2(100);
  l_post_tabpart_storage VARCHAR2(100);
  l_parallel    BOOLEAN; --added 22.10.2012 to control parallel move/rebuild of partitions
  l_parallelism NUMBER; --added 18.1.2013 maximum parallelism avoiding space wastage
  l_typical_compression NUMBER :=1; --estimated compression factor
BEGIN 
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                  ,action_name=>p_part_id||'/'||p_recname||'/'||p_partname||'/'||p_subpartname||'/'||p_tablespace||'/'||p_max_parts);
  msg(k_module||'.'||k_action||'(part_id=>'||p_part_id||',recname=>'||p_recname||',partname=>'||p_partname||',subpartname=>'||p_subpartname
              ||',tablespace=>'||p_tablespace||',updateind=>'||display_bool(p_updateind)||',statsjob=>'||display_bool(p_statsjob)
              ||',max_parts=>'||p_max_parts||',testmode=>'||display_bool(p_testmode)||')',k_trace_alert);

  read_init_params;

  --5.7.2012 added blocksize calculation to determine whether block compressed
  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);

  FOR i IN ( --range/list partitioning only
      --31.1.2013 USE_NL hint added, parallel hint corrected
	WITH x AS ( 
	SELECT /*+LEADING(t d r l) USE_NL(P B) PARALLEL(r) PARALLEL(l) PARALLEL(p) PARALLEL(b) MATERIALIZE*/ 
             p.table_owner, p.table_name, p.partition_name, p.subpartition_name
	,      p.last_analyzed
      ,      s.block_size
      ,      p.initial_extent
      ,      p.pct_free
	,	 COALESCE(l.tab_tablespace, r.tab_tablespace, p.tablespace_name) tablespace_name
	, 	 LEAST(NULLIF(p.blocks,0), b.blocks) blocks
      ,      p.num_rows
      ,      CEIL(p.num_rows*p.avg_row_len/(1-p.pct_free/100)
                 /(s.block_size-(57+23*p.ini_trans+2*p.num_rows/LEAST(NULLIF(p.blocks,0), b.blocks)+4))) est_blocks
	,      100*(p.num_rows*p.avg_row_len/(1-p.pct_free/100))
		 /(
		  LEAST(NULLIF(p.blocks,0), b.blocks)
 		  *(s.block_size-(57+23*p.ini_trans+2*p.num_rows/LEAST(NULLIF(p.blocks,0), b.blocks)+4)) --blocksize less est header overhead
		 ) block_pct
	,	 COALESCE(r.tab_storage,t.tab_storage) tab_storage
	,	 COALESCE(l.tab_storage
                     ,CASE WHEN r.tab_storage LIKE '%NOCOMPRESS%' THEN 'NOCOMPRESS'
                           WHEN r.tab_storage LIKE   '%COMPRESS%' THEN 'COMPRESS' END 
                     ,CASE WHEN t.tab_storage LIKE '%NOCOMPRESS%' THEN 'NOCOMPRESS'
                           WHEN t.tab_storage LIKE   '%COMPRESS%' THEN 'COMPRESS' END 
                     ,'NOCOMPRESS') subpart_tab_storage
	,	 b.header_file, b.header_block, b.extents
      FROM	 dba_tab_subpartitions p
	, 	 dba_segments b
      ,	 dba_tablespaces s
	,	 sysadm.gfc_part_tables t
	,	 sysadm.gfc_part_ranges r
	,	 sysadm.gfc_part_lists l
	,	 sysadm.psrecdefn d
      WHERE	 b.segment_type = 'TABLE SUBPARTITION'
      AND    b.owner = p.table_owner
      AND    b.segment_name = p.table_name
      AND    b.partition_name = p.subpartition_name
      AND    b.tablespace_name = p.tablespace_name
      AND    s.tablespace_name = p.tablespace_name
      AND    s.tablespace_name = b.tablespace_name --31.1.2013
      AND    b.segment_name = DECODE(d.sqltablename,' ','PS_'||d.recname,d.sqltablename)
      AND    p.table_name = DECODE(d.sqltablename,' ','PS_'||d.recname,d.sqltablename)
      AND    p.num_rows > 0 --must have stats
      AND    r.part_id = t.part_id
      AND    t.part_type = 'R'
      AND    l.part_id = t.part_id
      AND    t.subpart_type = 'L'
      AND    d.recname = t.recname
      AND    p.partition_name = t.recname||'_'||r.part_name 
      AND    p.subpartition_name = t.recname||'_'||r.part_name||'_'||l.part_name
      AND    b.partition_name = t.recname||'_'||r.part_name||'_'||l.part_name
      AND    p.table_owner IN('SYSADM',t.arch_schema)
      AND    b.owner IN('SYSADM',t.arch_schema) --added 31.1.2013
      AND    p.partition_name LIKE p_partname 
      AND    p.subpartition_name LIKE p_subpartname 
      AND    t.part_id LIKE p_part_id
      AND    t.recname LIKE p_recname 
      AND    COALESCE(l.tab_tablespace, r.tab_tablespace, p.tablespace_name) LIKE p_tablespace
      AND    s.status = 'ONLINE'
	)
	SELECT x.*
	,	 CASE WHEN block_pct >100 THEN 'COMPRESS' ELSE 'NOCOMPRESS' END est_seg_comp_status
      FROM x
      WHERE	(  (        (x.block_pct  < k_compress_threshold OR x.pct_free > 0)
                AND     x.subpart_tab_storage LIKE '%COMPRESS%' 
                AND NOT x.subpart_tab_storage LIKE '%NOCOMPRESS%' 
                AND     x.extents > 1 --added 31.1.2013
                AND     x.blocks >= k_min_blocks_threshold --added 31.1.2013
                AND     x.est_blocks < x.blocks --added 31.1.2013 no point compressing if we do not think we will save space
                AND     x.est_blocks > 5)
            OR (    x.block_pct >= k_decompress_threshold 
                AND (x.subpart_tab_storage LIKE '%NOCOMPRESS%' OR x.subpart_tab_storage IS NULL)))
      AND 	 (rownum <= p_max_parts OR p_max_parts IS NULL)
      ORDER BY  x.tablespace_name, x.header_file, x.header_block
  ) --nb ordered by tablespace, file and then header block
  LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>i.subpartition_name);
    msg('Subpartition '||i.subpartition_name||' : '||i.est_seg_comp_status||' ('
                       ||LTRIM(TO_CHAR(i.block_pct,'9999'))||'%,'
                       ||'est. '||i.est_blocks||' comp. blocks, ' --added 31.1.2013
                       ||i.num_rows||' rows, '||i.blocks||' blocks, '||i.extents||' extents, ' --31.1.2013 added extents to message
                       ||i.pct_free||'% free, analyzed '||TO_CHAR(i.last_analyzed,'dd.mm.yyyy hh24:mi:ss')||') -> '||i.subpart_tab_storage);
    l_parallel := p_parallel; --22.10.2012 set local variable to parameter

    --determine pre move partition storage--10g technique
    IF i.subpart_tab_storage LIKE '%COMPRESS%' AND NOT i.subpart_tab_storage LIKE '%NOCOMPRESS%' THEN
      l_pre_tabpart_storage := 'COMPRESS PCTFREE 0 PCTUSED 99';
    ELSE--no compression
      l_pre_tabpart_storage := i.tab_storage;
      IF i.tab_storage LIKE '%COMPRESS%' THEN 
        NULL;
      ELSE 
        l_pre_tabpart_storage := l_pre_tabpart_storage || ' NOCOMPRESS';
      END IF;          
    END IF;

    --determine post move partition storage--10g technique
    l_post_tabpart_storage := i.tab_storage;
    IF i.tab_storage LIKE '%COMPRESS%' AND NOT i.tab_storage LIKE '%NOCOMPRESS%' THEN 
      l_typical_compression := k_typical_compression;
    ELSE 
      l_post_tabpart_storage := l_post_tabpart_storage ||' NOCOMPRESS';
      l_typical_compression := 1;
    END IF;

    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name
		||' MODIFY PARTITION '||i.partition_name||' '||l_pre_tabpart_storage;
    exec_sql(l_sql,p_testmode);

    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name
		||' MOVE SUBPARTITION '||i.subpartition_name||' TABLESPACE '||i.tablespace_name;
    IF p_updateind THEN --added 4.7.2012 to control whether automatically update indexes
      l_sql := l_sql||' UPDATE INDEXES';
--16.1.2013 - we will no longer suppress parallelism if cascading to indexes, instead parallelism is not invoked by default
--    l_parallel := FALSE; --22.10.2012 suppress parallelism if cascading rebuild to indexes because PSFT_DDL_LOCK trigger cannot read package global variables in query slave sessions
    END IF;

    IF l_parallel THEN --18.1.2013-determine how much parallelism
      --parallel rebuild can waste space on small objects so estimate number of extent and use that as parallelism
      l_parallelism := FLOOR((i.est_blocks/l_typical_compression)/(i.initial_extent/i.block_size));
      IF l_parallelism <= 0 THEN
        l_parallel := FALSE;
      END IF;
      msg('estblocks='||i.est_blocks||',typical_compression='||l_typical_compression||
         ',initial_extent='||i.initial_extent||',block_size='||i.block_size||
         ',max_parallelism='||g_max_parallelism||
         ',parallelism='||l_parallelism);
    END IF;

    IF l_parallel THEN
      l_sql := l_sql||' PARALLEL';
      IF l_parallelism > 0 AND l_parallelism <= g_max_parallelism THEN
        l_sql := l_sql||' '||l_parallelism;
      END IF;
    ELSIF NOT l_parallel THEN --22.10.2012 no keyword if null
      l_sql := l_sql||' NOPARALLEL';
    END IF;

    exec_sql(l_sql,p_testmode);
    IF p_statsjob THEN --added 9.7.2012
      tab_stats_job(p_ownname=>i.table_owner
                   ,p_tabname=>i.table_name
                   ,p_partname=>i.subpartition_name
                   ,p_granularity=>'SUBPARTITION'
                   ,p_parallel=>p_parallel --added 22.10.2012
                   ,p_testmode=>p_testmode);
    END IF;

    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name
           ||' MODIFY PARTITION '||i.partition_name||' '||l_post_tabpart_storage;
    exec_sql(l_sql,p_testmode);

    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name
           ||' NOPARALLEL';
    exec_sql(l_sql,p_testmode);

  END LOOP;
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
  msg(k_module||'.'||k_action||' completed',k_trace_alert);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
EXCEPTION 
  WHEN e_partition_index_rebuild_fail THEN
    msg('Parallel index partition rebuild failed - Check PSFT_DDL_LOCK trigger disabled');
    dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
    RAISE;
END locate_tab_subparts;
 
-----------------------------------------------------------------------------------------------------------
--added 6.7.2012: like locate_tab_subparts, but for hash subpartitions, cloned from locate_tab_subparts
--10g solution, revisit in 11g when can specify storage on subpartition
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_tab_hash_subparts
(p_part_id     VARCHAR2 DEFAULT '%'
,p_recname     VARCHAR2 DEFAULT '%'
,p_partname    VARCHAR2 DEFAULT '%'
--,p_subpartname VARCHAR2 DEFAULT '%' --we will apply compress to all subpartitions
,p_tablespace  VARCHAR2 DEFAULT '%'
,p_updateind   BOOLEAN  DEFAULT TRUE --added 4.7.2012 to control whether automatically update indexes
,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
,p_parallel    BOOLEAN  DEFAULT FALSE --added 22.10.2012 to control parallel move/rebuild of partitions, 16.1.2013 default false
,p_max_parts   INTEGER  DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
  k_action      CONSTANT VARCHAR2(48) := 'LOCATE_TAB_HASH_SUBPARTS';
  l_module      VARCHAR2(48);
  l_action      VARCHAR2(32);
  l_sql         VARCHAR2(1000);
  l_parallel    BOOLEAN; --added 22.10.2012 to control parallel move/rebuild of partitions
  l_parallelism NUMBER; --added 18.1.2013 maximum parallelism avoiding space wastage
  l_typical_compression NUMBER :=1; --estimated compression factor
BEGIN 
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                  ,action_name=>p_part_id||'/'||p_recname||'/'||p_partname||'/'||p_tablespace||'/'||p_max_parts);
  msg(k_module||'.'||k_action||'(part_id=>'||p_part_id||',recname=>'||p_recname||',partname=>'||p_partname
              ||',tablespace=>'||p_tablespace||',updateind=>'||display_bool(p_updateind)||',max_parts=>'
              ||',statsjob=>'||display_bool(p_statsjob)||p_max_parts||',testmode=>'||display_bool(p_testmode)||')',k_trace_alert);

  read_init_params;

  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);
  FOR i IN ( --range/list partitioning only
        --31.1.2013 USE_NL hint added, parallel hint corrected
        WITH x AS ( 
        SELECT /*+LEADING(t d r) USE_NL(P B) PARALLEL(r) PARALLEL(p) PARALLEL(b) MATERIALIZE*/ 
               p.table_owner, p.table_name, p.partition_name, p.subpartition_name
        ,      p.last_analyzed
        ,      s.block_size
        ,      p.initial_extent
        ,      p.pct_free 
        ,      COALESCE(r.tab_tablespace, p.tablespace_name) tablespace_name
        ,      p.num_rows
        ,      LEAST(NULLIF(p.blocks,0), b.blocks) blocks
        ,      CEIL(p.num_rows*p.avg_row_len/(1-p.pct_free/100)
                   /(s.block_size-(57+23*p.ini_trans+2*p.num_rows/LEAST(NULLIF(p.blocks,0), b.blocks)+4))) est_blocks
        ,      100*(p.num_rows*p.avg_row_len/(1-p.pct_free/100))
               /(
                LEAST(NULLIF(p.blocks,0), b.blocks)
                *(s.block_size-(57+23*p.ini_trans+2*p.num_rows/LEAST(NULLIF(p.blocks,0), b.blocks)+4)) --blocksize less est header overhead
                ) block_pct
        ,      COALESCE(r.tab_storage,t.tab_storage) tab_storage
        ,      b.header_file, b.header_block, b.extents
        FROM   dba_tab_subpartitions p
        ,      dba_segments b
        ,      dba_tablespaces s
        ,      sysadm.gfc_part_tables t
        ,      sysadm.gfc_part_ranges r
        ,      sysadm.psrecdefn d
        WHERE  b.segment_type = 'TABLE SUBPARTITION'
        AND    b.owner = p.table_owner
        AND    b.segment_name = p.table_name
        AND    b.partition_name = p.subpartition_name
        AND    b.tablespace_name = p.tablespace_name
        AND    s.tablespace_name = p.tablespace_name
        AND    s.tablespace_name = b.tablespace_name --added 31.1.2013
        AND    b.segment_name = DECODE(d.sqltablename,' ','PS_'||d.recname,d.sqltablename)
        AND    p.table_name = DECODE(d.sqltablename,' ','PS_'||d.recname,d.sqltablename)
        AND    p.num_rows > 0 --must have stats
        AND    r.part_id = t.part_id
        AND    t.part_type = 'R'
        AND    t.subpart_type = 'H'
        AND    d.recname = t.recname
        AND    p.partition_name = t.recname||'_'||r.part_name 
        AND    b.partition_name = p.subpartition_name
        AND    p.table_owner IN('SYSADM',t.arch_schema)
        AND    b.owner IN('SYSADM',t.arch_schema) --added 31.1.2013
        AND    p.partition_name LIKE p_partname 
        AND    t.part_id LIKE p_part_id
        AND    t.recname LIKE p_recname 
        AND    COALESCE(r.tab_tablespace, p.tablespace_name) LIKE p_tablespace
        AND    s.status = 'ONLINE'
        )
        SELECT x.*
        ,      CASE WHEN block_pct >100 THEN 'COMPRESS' ELSE 'NOCOMPRESS' END est_seg_comp_status
        FROM x
        WHERE  (  (        (x.block_pct  < k_compress_threshold OR x.pct_free > 0)
                   AND     x.tab_storage LIKE '%COMPRESS%' 
                   AND NOT x.tab_storage LIKE '%NOCOMPRESS%' 
                   AND     x.extents >1 --added 31.1.2013 no point compressing a single extent
                   AND     x.blocks >= k_min_blocks_threshold --added 31.1.2013
                   AND     x.est_blocks < x.blocks --added 31.1.2013 no point compressing if we do not think we will save space
                   AND     x.est_blocks > 5)
               OR (   x.block_pct >= k_decompress_threshold 
                  AND (x.tab_storage LIKE '%NOCOMPRESS%' OR x.tab_storage IS NULL)))
        AND    (rownum <= p_max_parts OR p_max_parts IS NULL)
        ORDER BY  x.tablespace_name, x.header_file, x.header_block
  ) --nb ordered by tablespace, file and then header block
  LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>i.subpartition_name);
    msg('Subpartition '||i.subpartition_name||' : '||i.est_seg_comp_status||' ('
                       ||LTRIM(TO_CHAR(i.block_pct,'9999'))||'%,'
                       ||'est. '||i.est_blocks||' comp. blocks, ' --added 31.1.2013
                       ||i.num_rows||' rows, '||i.blocks||' blocks, '||i.extents||' extents, ' --31.1.2013 added extents to message
                       ||i.pct_free||'% free, analyzed '||TO_CHAR(i.last_analyzed,'dd.mm.yyyy hh24:mi:ss')||') -> '||i.tab_storage);

    l_parallel := p_parallel; --22.10.2012 set local variable to parameter

    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name
                ||' MODIFY PARTITION '||i.partition_name||' '||i.tab_storage;
    exec_sql(l_sql,p_testmode);

    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name
                ||' MOVE SUBPARTITION '||i.subpartition_name||' TABLESPACE '||i.tablespace_name;
    IF p_updateind THEN --added 4.7.2012 to control whether automatically update indexes
      l_sql := l_sql||' UPDATE INDEXES';
--16.1.2013 - we will no longer suppress parallelism if cascading to indexes, instead parallelism is not invoked by default
--    l_parallel := FALSE; --22.10.2012 suppress parallelism if cascading rebuild to indexes because PSFT_DDL_LOCK trigger cannot read package global variables in query slave sessions
    END IF;

    IF i.tab_storage LIKE '%COMPRESS%' AND NOT i.tab_storage LIKE '%NOCOMPRESS%' THEN 
      l_typical_compression := k_typical_compression;
    ELSE 
      l_typical_compression := 1;
    END IF;

    IF l_parallel THEN --18.1.2013-determine how much parallelism
      --parallel rebuild can waste space on small objects so estimate number of extent and use that as parallelism
      l_parallelism := FLOOR((i.est_blocks/l_typical_compression)/(i.initial_extent/i.block_size));
      IF l_parallelism <= 0 THEN
        l_parallel := FALSE;
      END IF;
      msg('est_blocks='||i.est_blocks||',typical_compression='||l_typical_compression||
         ',initial_extent='||i.initial_extent||',block_size='||i.block_size||
         ',max_parallelism='||g_max_parallelism||
         ',parallelism='||l_parallelism);
    END IF;

    IF l_parallel THEN
      l_sql := l_sql||' PARALLEL';
      IF l_parallelism > 0 AND l_parallelism <= g_max_parallelism THEN
        l_sql := l_sql||' '||l_parallelism;
      END IF;
    ELSIF NOT l_parallel THEN
      l_sql := l_sql||' NOPARALLEL';
    END IF;

    exec_sql(l_sql,p_testmode);
    IF p_statsjob THEN --added 9.7.2012
      tab_stats_job(p_ownname=>i.table_owner
                   ,p_tabname=>i.table_name
                   ,p_partname=>i.subpartition_name
                   ,p_granularity=>'SUBPARTITION'
                   ,p_parallel=>p_parallel --added 22.10.2012
                   ,p_testmode=>p_testmode);
    END IF;

    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name
        ||' NOPARALLEL';
    exec_sql(l_sql,p_testmode);

  END LOOP;
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
  msg(k_module||'.'||k_action||' completed',k_trace_alert);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
EXCEPTION 
  WHEN e_partition_index_rebuild_fail THEN
    msg('Parallel index partition rebuild failed - Check PSFT_DDL_LOCK trigger disabled');
    dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
    RAISE;
END locate_tab_hash_subparts;
 
-----------------------------------------------------------------------------------------------------------
--relocate lob partitions
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_lob_parts
(p_part_id     VARCHAR2 DEFAULT '%'
,p_recname     VARCHAR2 DEFAULT '%'
,p_partname    VARCHAR2 DEFAULT '%'
,p_tablespace  VARCHAR2 DEFAULT '%'
,p_updateind   BOOLEAN  DEFAULT TRUE --added 4.7.2012 to control whether automatically update indexes
,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
,p_parallel    BOOLEAN  DEFAULT FALSE --added 22.10.2012 to control parallel move/rebuild of partitions, 16.1.2013 default false
,p_max_parts   INTEGER  DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
  k_action CONSTANT VARCHAR2(48) := 'LOCATE_LOB_PARTS';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_sql    VARCHAR2(1000);
  l_parallel BOOLEAN; --added 22.10.2012 to control parallel move/rebuild of partitions
BEGIN 
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                  ,action_name=>p_part_id||'/'||p_recname||'/'||p_partname||'/'||p_tablespace||'/'||p_max_parts);
  msg(k_module||'.'||k_action||'(part_id=>'||p_part_id||',recname=>'||p_recname||',partname=>'||p_partname
              ||',tablespace=>'||p_tablespace||',updateind=>'||display_bool(p_updateind)||',statsjob=>'||display_bool(p_statsjob)
              ||',max_parts=>'||p_max_parts||',testmode=>'||display_bool(p_testmode)||')',k_trace_alert);

  l_parallel := p_parallel; --22.10.2012 set local variable to parameter
  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);

  FOR i IN (
	SELECT /*+LEADING(t r rp) USE_NL(p) PARALLEL(rp) PARALLEL(p)*/
             p.table_owner
	,      p.table_name
	,	 p.partition_name
	,	 p.lob_name
	,	 p.column_name
	,	 p.lob_indpart_name
	,	 COALESCE(rp.tab_tablespace,t.tab_tablespace) tab_tablespace
	,	 p.tablespace_name
	,	 UPPER(rp.tab_storage) tab_storage
      FROM	 sysadm.gfc_part_tables t
	,	 sysadm.psrecdefn r
	,	 sysadm.gfc_part_ranges rp
	,	 dba_lob_partitions p
      WHERE	 t.part_id = rp.part_id
      AND    t.subpart_type = 'N' --not subpartitions
      AND    p.table_owner IN('SYSADM',t.arch_schema)
      AND    r.recname = t.recname
      AND    p.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
      AND    p.partition_name = t.recname||'_'||rp.part_name
      AND    p.tablespace_name != COALESCE(rp.tab_tablespace,t.tab_tablespace)
--
      AND    t.part_id LIKE p_part_id 
      AND    t.recname LIKE p_recname 
      AND    p.partition_name LIKE p_partname 
      AND    t.recname||'_'||rp.part_name LIKE p_partname
      AND 	 (rownum <= p_max_parts OR p_max_parts IS NULL)
      AND    rp.tab_tablespace LIKE p_tablespace 
      ORDER BY rp.tab_tablespace
--	, 	 p.blocks
	,	 rp.part_name
  ) LOOP

    dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>i.partition_name);
    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name
	||' MOVE PARTITION '||i.partition_name
	||' LOB ('||i.column_name||')'
	||' STORE AS (TABLESPACE '||i.tab_tablespace||')';

    IF p_updateind THEN --added 4.7.2012 to control whether automatically update indexes
      l_sql := l_sql||' UPDATE INDEXES';
--16.1.2013 - we will no longer suppress parallelism if cascading to indexes, instead parallelism is not invoked by default
--    l_parallel := FALSE; --22.10.2012 suppress parallelism if cascading rebuild to indexes because PSFT_DDL_LOCK trigger cannot read package global variables in query slave sessions
    END IF;

    IF i.tab_storage LIKE '%COMPRESS%' THEN
      NULL;
    ELSE
      l_sql := l_sql||' NOCOMPRESS';
    END IF;

    IF i.tab_storage IS NOT NULL THEN
      l_sql := l_sql||' '||i.tab_storage;
    END IF;

    IF l_parallel THEN 
      l_sql := l_sql||' PARALLEL';
    ELSIF NOT l_parallel THEN --22.10.2012 no keyword if null
      l_sql := l_sql||' NOPARALLEL';
    END IF;
    exec_sql(l_sql,p_testmode);

    IF p_statsjob THEN --added 9.7.2012
      tab_stats_job(p_ownname=>i.table_owner
                   ,p_tabname=>i.table_name
                   ,p_partname=>i.partition_name
                   ,p_granularity=>'PARTITION'
                   ,p_parallel=>p_parallel --added 22.10.2012
                   ,p_testmode=>p_testmode);
    END IF;

    l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name
	||' NOPARALLEL';
    exec_sql(l_sql,p_testmode);

  END LOOP;
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
  msg(k_module||'.'||k_action||' completed',k_trace_alert);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END locate_lob_parts;

-----------------------------------------------------------------------------------------------------------
--rebuilds index partitions which are either
--1:in the wrong tablespace
--currently restricted to AUDIT tables
-----------------------------------------------------------------------------------------------------------
PROCEDURE locate_ind_parts
(p_part_id     VARCHAR2 DEFAULT '%'
,p_recname     VARCHAR2 DEFAULT '%'
,p_partname    VARCHAR2 DEFAULT '%'
,p_tablespace  VARCHAR2 DEFAULT '%'
,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
,p_max_parts   INTEGER  DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
  k_action CONSTANT VARCHAR2(48) := 'LOCATE_IND_PARTS';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_sql    VARCHAR2(1000);
BEGIN 
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_part_id||'/'||p_recname||'/'||p_partname||'/'||p_tablespace||'/'||p_max_parts);
  msg(k_module||'.'||k_action||'(part_id=>'||p_part_id||',recname=>'||p_recname||',partname=>'||p_partname
              ||',tablespace=>'||p_tablespace||',statsjob=>'||display_bool(p_statsjob)
              ||',max_parts=>'||p_max_parts||',testmode=>'||display_bool(p_testmode)||')',k_trace_alert);

  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);
  FOR i IN (
	SELECT /*+LEADING(t r rp)*/ p.index_owner
	,	p.index_name
	,	p.partition_name
	,	COALESCE(rp.idx_tablespace,t.idx_tablespace) idx_tablespace
	,	p.tablespace_name
	,	rp.idx_storage
	,	p.leaf_blocks
      FROM	sysadm.gfc_part_tables t
	,	sysadm.psrecdefn r
	,	sysadm.gfc_part_ranges rp
	,	dba_indexes i
	,	dba_ind_partitions p
      WHERE	t.part_id = rp.part_id	
      AND	t.subpart_type = 'N' --not subpartitions
      AND	i.table_owner IN('SYSADM',t.arch_schema)
      AND	r.recname = t.recname
      AND	i.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
      AND   p.index_name = i.index_name
      AND   p.index_owner = i.owner
      AND	p.partition_name LIKE t.recname||'_'||rp.part_name
      AND	(p.tablespace_name != COALESCE(rp.idx_tablespace,t.idx_tablespace)
      OR     p.status = 'UNUSABLE') 
--
      AND   t.part_id LIKE p_part_id
      AND   t.recname LIKE p_recname 
      AND   p.partition_name LIKE p_partname 
      AND 	(rownum <= p_max_parts OR p_max_parts IS NULL)
      AND   rp.idx_tablespace LIKE p_tablespace 
      ORDER BY rp.idx_tablespace
--	, 	 p.leaf_blocks
	,	 rp.part_name
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>i.partition_name);

    l_sql := 'ALTER INDEX '||i.index_owner||'.'||i.index_name
	||' REBUILD PARTITION '||i.partition_name
        ||' TABLESPACE '||i.idx_tablespace||' '||i.idx_storage||' PARALLEL';
    exec_sql(l_sql,p_testmode);

    IF p_statsjob THEN --added 9.7.2012
      ind_stats_job(p_ownname=>i.index_owner
                   ,p_indname=>i.index_name
                   ,p_partname=>i.partition_name
                   ,p_granularity=>'PARTITION'
                   ,p_parallel=>TRUE --added 22.10.2012
                   ,p_testmode=>p_testmode);
    END IF;

    l_sql := 'ALTER INDEX '||i.index_owner||'.'||i.index_name
	||' NOPARALLEL';
    exec_sql(l_sql,p_testmode);
  END LOOP;
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
  msg(k_module||'.'||k_action||' completed',k_trace_alert);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END locate_ind_parts;

---------------------------------------------------------------------------------------------------
--rebuild index subpartitions in wrong tablespace or which are not usable
----------------------------------------------------------------------------------------------------
PROCEDURE locate_ind_subparts
(p_part_id     VARCHAR2 DEFAULT '%'
,p_recname     VARCHAR2 DEFAULT '%'
,p_partname    VARCHAR2 DEFAULT '%' --added 10.7.2012 specific partition
,p_subpartname VARCHAR2 DEFAULT '%' --added 10.7.2012 specific subpartition
,p_tablespace  VARCHAR2 DEFAULT '%'
,p_statsjob    BOOLEAN  DEFAULT TRUE --added 10.7.2012 to update stats on physical partitions
,p_max_parts   INTEGER  DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
  k_action CONSTANT VARCHAR2(48) := 'LOCATE_IND_SUBPARTS';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_sql    VARCHAR2(1000);
BEGIN 
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_part_id||'/'||p_recname||'/'||p_partname||'/'||p_subpartname||'/'||p_tablespace||'/'||p_max_parts);
  msg(k_module||'.'||k_action||'(part_id=>'||p_part_id||',recname=>'||p_recname||',partname=>'||p_partname||',subpartname=>'||p_subpartname
              ||',tablespace=>'||p_tablespace||',statsjob=>'||display_bool(p_statsjob)
              ||',max_parts=>'||p_max_parts||',testmode=>'||display_bool(p_testmode)||')',k_trace_alert);

  sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);
  FOR i IN ( --range/list partitioning only
	SELECT /*+LEADING(t r)*/ s.index_owner
	,      s.index_name
	,      s.partition_name
	,      s.subpartition_name
	,      COALESCE(lp.idx_tablespace,rp.idx_tablespace,t.idx_tablespace) idx_tablespace
	,      s.tablespace_name
	,      COALESCE(lp.idx_storage,rp.idx_storage,t.idx_storage) idx_storage
	,      COALESCE(rp.idx_storage,t.idx_storage) idx_part_storage
	,      s.leaf_blocks
      FROM   sysadm.gfc_part_tables t
	,      sysadm.psrecdefn r
	,      sysadm.gfc_part_ranges rp
	,      sysadm.gfc_part_lists lp
	,      sysadm.gfc_part_range_lists rlp
	,      dba_indexes i
	,      dba_ind_subpartitions s
      WHERE  t.part_type = 'R'
      AND    rp.part_id = t.part_id
      AND    t.subpart_type = 'L' 
      AND    lp.part_id = t.part_id
      AND    i.table_owner IN('SYSADM',t.arch_schema)
      AND    r.recname = t.recname
      AND    i.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
      AND    s.index_name = i.index_name
      AND    s.index_owner = i.owner
      AND    s.partition_name = t.recname||'_'||rp.part_name
      AND    s.subpartition_name = t.recname||'_'||rp.part_name||'_'||lp.part_name
      AND    t.recname||'_'||rp.part_name LIKE p_partname 
      AND    t.recname||'_'||rp.part_name||'_'||lp.part_name LIKE p_subpartname 
      AND    rlp.part_id = t.part_id
      AND    rlp.range_name = rp.part_name
      AND    rlp.list_name = lp.part_name
      AND    rlp.build = 'Y'
      AND    (s.tablespace_name != COALESCE(lp.idx_tablespace,rp.idx_tablespace,t.idx_tablespace)
      OR     s.status = 'UNUSABLE') 
--
      AND    t.part_id LIKE p_part_id 
      AND    t.recname LIKE p_recname 
      AND    s.partition_name LIKE p_partname 
      AND    s.subpartition_name LIKE p_subpartname 
      AND 	 (rownum <= p_max_parts OR p_max_parts IS NULL)
      AND    COALESCE(lp.idx_tablespace,rp.idx_tablespace,t.idx_tablespace) LIKE p_tablespace 
      ORDER BY idx_tablespace
--	, 	 p.leaf_blocks
	,	 rp.part_name
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>i.subpartition_name);

--    l_sql := 'ALTER INDEX '||i.index_owner||'.'||i.index_name
--          ||' MODIFY PARTITION '||i.partition_name
--          ||' '||i.idx_storage;
--    exec_sql(l_sql,p_testmode);

    l_sql := 'ALTER INDEX '||i.index_owner||'.'||i.index_name
          ||' REBUILD SUBPARTITION '||i.subpartition_name
          ||' TABLESPACE '||i.idx_tablespace||' PARALLEL';
    exec_sql(l_sql,p_testmode);

    IF p_statsjob THEN --added 9.7.2012
      ind_stats_job(p_ownname=>i.index_owner
                   ,p_indname=>i.index_name
                   ,p_partname=>i.subpartition_name
                   ,p_granularity=>'SUBPARTITION'
                   ,p_parallel=>TRUE --added 22.10.2012
                   ,p_testmode=>p_testmode);
    END IF;
--    l_sql := 'ALTER INDEX '||i.index_owner||'.'||i.index_name
--          ||' MODIFY PARTITION '||i.partition_name
--          ||' '||i.idx_part_storage;
--    exec_sql(l_sql,p_testmode);

    l_sql := 'ALTER INDEX '||i.index_owner||'.'||i.index_name
	||' NOPARALLEL';
    exec_sql(l_sql,p_testmode);
  END LOOP;
  sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);
  msg(k_module||'.'||k_action||' completed',k_trace_alert);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END locate_ind_subparts;
----------------------------------------------------------------------------------------------------
--added 10.7.2012 to call all partition management programs in a single call
----------------------------------------------------------------------------------------------------
PROCEDURE locate_all_parts
(p_part_id     VARCHAR2 DEFAULT '%'
,p_recname     VARCHAR2 DEFAULT '%'
,p_partname    VARCHAR2 DEFAULT '%'
,p_subpartname VARCHAR2 DEFAULT '%' --added 5.7.2012 specific partition
,p_tablespace  VARCHAR2 DEFAULT '%'
,p_updateind   BOOLEAN  DEFAULT TRUE --added 4.7.2012 to control whether automatically update indexes
,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
,p_parallel    BOOLEAN  DEFAULT FALSE --added 22.10.2012 to control parallel move/rebuild of partitions, 16.1.2013 default false
,p_max_parts   INTEGER  DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
  k_action CONSTANT VARCHAR2(48) := 'LOCATE_ALL_PARTS';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_sql    VARCHAR2(1000);
BEGIN 
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action
                                  ,action_name=>p_part_id||'/'||p_recname||'/'||p_partname||'/'||p_tablespace||'/'||p_max_parts);
  msg(k_module||'.'||k_action||'(part_id=>'||p_part_id||',recname=>'||p_recname||',partname=>'||p_partname||',subpartname=>'||p_subpartname
              ||',tablespace=>'||p_tablespace||',updateind=>'||display_bool(p_updateind)||',statsjob=>'||display_bool(p_statsjob)
              ||',max_parts=>'||p_max_parts||',testmode=>'||display_bool(p_testmode)||')',k_trace_alert);

  locate_tab_parts
      (p_part_id
      ,p_recname
      ,p_partname
      ,p_tablespace
      ,p_updateind
      ,p_statsjob
      ,p_parallel --added 22.10.2012
      ,p_max_parts
      ,p_testmode);

  locate_tab_subparts
      (p_part_id
      ,p_recname
      ,p_partname
      ,p_subpartname
      ,p_tablespace
      ,p_updateind
      ,p_statsjob
      ,p_parallel --added 22.10.2012
      ,p_max_parts
      ,p_testmode);
  
  locate_tab_hash_subparts
      (p_part_id
      ,p_recname
      ,p_partname
      ,p_tablespace
      ,p_updateind
      ,p_statsjob
      ,p_parallel --added 22.10.2012
      ,p_max_parts
      ,p_testmode);

  locate_lob_parts
      (p_part_id
      ,p_recname
      ,p_partname
      ,p_tablespace
      ,p_updateind
      ,p_statsjob
      ,p_parallel --added 22.10.2012
      ,p_max_parts
      ,p_testmode);
  
  locate_ind_parts
      (p_part_id
      ,p_recname
      ,p_partname
      ,p_tablespace
      ,p_statsjob
      ,p_max_parts
      ,p_testmode);
  
  locate_ind_subparts
      (p_part_id
      ,p_recname
      ,p_partname
      ,p_subpartname
      ,p_tablespace
      ,p_statsjob
      ,p_max_parts
      ,p_testmode);

  msg(k_module||'.'||k_action||' completed',k_trace_alert);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END locate_all_parts;
----------------------------------------------------------------------------------------------------
--11.7.2012 rebuild unusable non-partitioned indexes made unusable by move
----------------------------------------------------------------------------------------------------
PROCEDURE rebuild_indexes
(p_ownname  VARCHAR2 DEFAULT '%'
,p_tabname  VARCHAR2 DEFAULT '%'
,p_indname  VARCHAR2 DEFAULT '%'
,p_testmode BOOLEAN  DEFAULT FALSE
) IS
  k_action CONSTANT VARCHAR2(48) := 'REBUILD_INDEXES';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_cmd    VARCHAR2(200);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_ownname||'/'||p_tabname||'/'||p_indname);
  msg(k_module||'.'||k_action||'(ownname=>'||p_ownname||',tabname=>'||p_tabname||',indname=>'||p_indname||')');

  FOR i IN (
    SELECT *
    FROM   dba_indexes
    WHERE  table_owner LIKE p_ownname
    AND    table_name LIKE p_tabname
    AND    index_name LIKE p_indname
    AND    status = 'UNUSABLE'
    AND    partitioned = 'NO'
    AND    index_type like '%NORMAL' --exclude LOB indexes
  ) LOOP
    l_cmd := 'ALTER INDEX '||i.owner||'.'||i.index_name||' REBUILD TABLESPACE '||i.tablespace_name||' PARALLEL';
    exec_sql(l_cmd,p_testmode);
    l_cmd := 'ALTER INDEX '||i.owner||'.'||i.index_name||' NOPARALLEL';
    exec_sql(l_cmd,p_testmode);
  END LOOP;

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END rebuild_indexes;
----------------------------------------------------------------------------------------------------
END gfc_arch_mgmt;
/

show errors
spool off

