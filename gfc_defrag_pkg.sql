--------------------------------------------------------------------------------------
--
-- script        : gfc_defrag_pkg.sql
--
-- created by    : DMK
-- creation date : 19.10.2012
--
-- description   : Free space defragmentation utility package
--
-- dependencies:  none
--
set echo on serveroutput on
spool gfc_defrag_pkg
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE sys.gfc_defrag AS
-----------------------------------------------------------------------------------------------------------
-- development and maintenance history
--
-- date           author            version   reference    description
-- ------------   ----------------- -------   ------------ --------------------
-- 19.10.2012     DMK               1.00      MOR          New package - created for the high_block_segments and trim_free_space procedures
--                                                         which were removed from the GFC_ARCH_MGMT package. This was done because these
--                                                         procedures need to be run as SYS, whereas the WMS_ARCH_MGMT package is now going to
--                                                         be owned by SYSADM.
-- 24.10.2012     DMK               1.01      MOR          Additional logging to alert log and trace file. Move the development and maintenance history section 
--                                                         to within the package itself.
--                                                         N.B. The associated package GFC_ARCH_MGMT has also been amended back so that it is owned by SYS.
-- 15.11.2012  	  DMK	            1.02      MOR          As the package is owned by SYS, the section on Audit Comments added 
-- 18.12.2012     DMK               1.03      MOR          Rebuild degree 1 as NOPARALLEL in rebuild_indexes() 
-- 20.12.2012     DMK               1.04      MOR          Bugfix trim_free_space trim to last space
-- 19.01.2013     DMK               1.05      MOR          Limit parallelism to number of extents in segment
-----------------------------------------------------------------------------------------------------------
--General note on parameters used in subprograms in this package
-----------------------------------------------------------------------------------------------------------
--p_partname   - Oracle partition name (or subpartition where appropriate and there is no separate subpartition parameter) - uses LIKE pattern match
--p_tablespace - Oracle tablespace name - uses LIKE pattern match
--p_updateind  - If true include UPDATE INDEXES clause in ALTER TABLE command, if false then do not
--p_statsjob   - If true the submit job to collect stats on partition that has just been moved/rebuilt
--p_max_parts  - Maximum number of partitions to be acted upon.  If null then no limit
--p_testmode   - If true do not submit SQL to database only echo to screen
-----------------------------------------------------------------------------------------------------------
--rebuild partition with highest block in tablespace
-----------------------------------------------------------------------------------------------------------
PROCEDURE high_block_segments
(p_tablespace  VARCHAR2 DEFAULT '%'
,p_file_id     NUMBER   DEFAULT NULL
,p_relfileno   NUMBER   DEFAULT NULL --added 14.7.2012 
,p_updateind   BOOLEAN  DEFAULT TRUE --added 11.7.2012 to control whether automatically update indexes
,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
,p_resize      BOOLEAN  DEFAULT TRUE --added 17.7.2012 to control whether to resize datafile
,p_max_parts   INTEGER  DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
--this procedure trims free space at the top of a data file. To be used in conjuction with high_block_segments 
-----------------------------------------------------------------------------------------------------------
PROCEDURE trim_free_space
(p_tablespace  VARCHAR2 DEFAULT '%'
,p_file_id     NUMBER   DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE);
-----------------------------------------------------------------------------------------------------------
END gfc_defrag;
/


----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY sys.gfc_defrag AS
-----------------------------------------------------------------------------------------------------------
k_module  CONSTANT VARCHAR2(48) := $$PLSQL_UNIT;
-----------------------------------------------------------------------------------------------------------
--24.10.2012 parameters to dbms_system.kswrdt 
-----------------------------------------------------------------------------------------------------------
k_trace       CONSTANT INTEGER := 1; --write to session trace file only
k_alert       CONSTANT INTEGER := 2; --write to database alert log only
k_trace_alert CONSTANT INTEGER := 3; --write to both alert log and trace file
-----------------------------------------------------------------------------------------------------------
--18.1.2013 Package global variable to limit specified parallelism and mimimise space wastage during compression
g_max_parallelism INTEGER; --calculate as 
-----------------------------------------------------------------------------------------------------------
e_data_beyond_size EXCEPTION; --added 20.12.2012
PRAGMA EXCEPTION_INIT(e_data_beyond_size,-3297); --added 20.12.2012
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
(p_msg VARCHAR2
, p_dest NUMBER DEFAULT 1 --added 23.10.2012 by default write message to trace file
) IS
BEGIN
  dbms_output.put_line(TO_CHAR(SYSDATE,'hh24:mi:ss dd.mm.yyyy')||':'||p_msg);
  sys.dbms_system.ksdwrt(p_dest, p_msg); --24.10.2012 write to session trace and/or alert log - no need to add timestamp
END msg;

-----------------------------------------------------------------------------------------------------------
--executes a dynamic sql statement in a variable
--20.12.2012 added error message logging code
-----------------------------------------------------------------------------------------------------------
PROCEDURE exec_sql
(p_sql VARCHAR2
,p_testmode BOOLEAN DEFAULT FALSE
) IS
  l_sqlcode NUMBER;
  l_sqlerrm VARCHAR2(64);
BEGIN
  IF p_testmode THEN NULL;
    msg('Test SQL:'||p_sql||';');
  ELSE
    msg(p_sql);
    EXECUTE IMMEDIATE p_sql;
  END IF;
EXCEPTION WHEN OTHERS THEN
  l_sqlcode := SQLCODE;
  l_sqlerrm := SUBSTR(SQLERRM,1,64);
  msg(l_sqlerrm);
  RAISE; --reraise the exception
END exec_sql;

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
--added 9.7.2012 to submit asynchronous scheduled job to collect table partition stats
-----------------------------------------------------------------------------------------------------------
PROCEDURE tab_stats_job 
(p_ownname     VARCHAR2
,p_tabname     VARCHAR2
,p_partname    VARCHAR2 DEFAULT NULL
,p_granularity VARCHAR2 DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
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
  msg(k_action||'(ownname=>'||p_ownname||',tabname=>'||p_tabname||',partname=>'||p_partname
              ||',granularity=>'||p_granularity||',testmode=>'||display_bool(p_testmode)||')');
  l_msg := 'Collect statistics on table '||LTRIM(lower(p_granularity)||' ')||p_ownname||'.'||p_tabname||'.'||p_partname;

  l_cmd := 'BEGIN sys.dbms_stats.gather_table_stats(ownname=>'''||p_ownname||''',tabname=>'''||p_tabname||'''';
  IF p_partname IS NOT NULL THEN
    l_cmd := l_cmd||',partname=>'''||p_partname||'''';
  END IF;
  IF p_granularity IS NOT NULL THEN
    l_cmd := l_cmd||',granularity=>'''||p_granularity||'''';
  END IF;
  l_cmd := l_cmd||',estimate_percent=>DBMS_STATS.AUTO_SAMPLE_SIZE,method_opt=>''FOR ALL COLUMNS SIZE REPEAT'',cascade=>TRUE); END;';
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
    msg('Submitted Job '||l_job_name||':'||l_msg);
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
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
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
  msg(k_action||'(ownname=>'||p_ownname||',indname=>'||p_indname||',partname=>'||p_partname
              ||',granularity=>'||p_granularity||',testmode=>'||display_bool(p_testmode)||')');
  l_msg := 'Collect statistics on index '||ltrim(lower(p_granularity)||' ')||p_ownname||'.'||p_indname||'.'||p_partname;

  l_cmd := 'BEGIN sys.dbms_stats.gather_index_stats(ownname=>'''||p_ownname||''',indname=>'''||p_indname||'''';
  IF p_partname IS NOT NULL THEN
    l_cmd := l_cmd||',partname=>'''||p_partname||'''';
  END IF;
  IF p_granularity IS NOT NULL THEN
    l_cmd := l_cmd||',granularity=>'''||p_granularity||'''';
  END IF;
  l_cmd := l_cmd||',estimate_percent=>DBMS_STATS.AUTO_SAMPLE_SIZE); END;';
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
    msg('Submitted Job '||l_job_name||':'||l_msg);
  END IF;
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END ind_stats_job;

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
  msg(k_action||'(ownname=>'||p_ownname||',tabname=>'||p_tabname||',indname=>'||p_indname||')');

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

    l_cmd := 'ALTER INDEX '||i.owner||'.'||i.index_name;
    IF i.degree = 'DEFAULT' THEN 
      l_cmd := l_cmd||' PARALLEL';
    ELSIF i.degree IN('0','1') THEN --18.12.2012 rebuild both degree 0 and 1 as noparallel
      l_cmd := l_cmd||' NOPARALLEL';
    ELSE
      l_cmd := l_cmd||' DEGREE '||LTRIM(i.degree);
    END IF;
    exec_sql(l_cmd,p_testmode);

  END LOOP;

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END rebuild_indexes;

----------------------------------------------------------------------------------------------------
--rebuild object with highest block in tablespace
--10.7.2012 rewritten to handle 10g restriction on directly specifying subpartition storage options
----------------------------------------------------------------------------------------------------
PROCEDURE high_block_segments
(p_tablespace  VARCHAR2 DEFAULT '%'
,p_file_id     NUMBER   DEFAULT NULL
,p_relfileno   NUMBER   DEFAULT NULL --added 14.7.2012 
,p_updateind   BOOLEAN  DEFAULT TRUE --added 11.7.2012 to control whether automatically update indexes
,p_statsjob    BOOLEAN  DEFAULT TRUE --added 9.7.2012 to update stats on physical partitions
,p_resize      BOOLEAN  DEFAULT TRUE --added 17.7.2012 to control whether to resize datafile
,p_max_parts   INTEGER  DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
  k_action         CONSTANT VARCHAR2(48) := 'HIGH_BLOCK_SEGMENTS';
  k_pct_free       NUMBER := 10; --default PCTFREE
  k_pct_used       NUMBER := 40; --default PCTUSED
  l_module         VARCHAR2(48);
  l_action         VARCHAR2(32);
  l_cmd1           VARCHAR2(200); --first ddl command -this will set partition storage
  l_cmd2           VARCHAR2(200); --second ddl command -this will do the move
  l_cmd3           VARCHAR2(200); --third ddl command -this will reset partition storage
  l_cmd4           VARCHAR2(200); --fourth ddl command -this will reset partition storage
  l_cmd_resize     VARCHAR2(200); --ddl to resize tablespace
  l_file_id        NUMBER;
  l_relfileno      NUMBER;
  l_resize         BOOLEAN; --control file resize - renamed from doddl 17.7.2012
  l_table_name     VARCHAR2(30);
  l_column_name    VARCHAR2(30);
  l_partition_name VARCHAR2(30); --parent partition of subpartition
  l_pct_free       NUMBER;
  l_pct_used       NUMBER;
  l_degree         VARCHAR2(10); --degree of parallelism
  l_granularity    VARCHAR2(20); --granularity parameter for stats job
  l_rebuildind     BOOLEAN;      --rebuild non-partitioned indexes because can only UPDATE INDEXES for partitions
  l_parallelism    INTEGER; --parallelism of rebuild option
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_tablespace||'/'||p_file_id||'/'||p_max_parts);
  msg(k_action||'(tablespace=>'||p_tablespace
	||',file_id=>'||p_file_id||',relfileno=>'||p_relfileno
	||',max_parts=>'||p_max_parts||',testmode=>'||display_bool(p_testmode)||')',k_trace_alert);
  l_file_id := p_file_id;

  --if not specified assume relative file number is same as file ID - which it usually is.
  IF p_relfileno IS NULL and p_file_id IS NOT NULL THEN
    l_relfileno := p_file_id;
  ELSE
    l_relfileno := p_relfileno;
  END IF;

  read_init_params; --read oracle init parameter to determine make parallelism

  FOR i IN (
	with x as (
	SELECT /*+LEADING(ds) use_NL(e) parallel(e)*/
      	ds.owner, ds.segment_name, ds.partition_name, ds.segment_type,
      	ds.tablespace_name, 
--      	e.ktfbueextno extent_id,
      	f.file# file_id,
		e.ktfbuebno block_id,
--		e.ktfbueblks * ds.blocksize bytes,
		e.ktfbueblks blocks,
--		e.ktfbuefno relative_fno,
		DECODE(BITAND(ds.segment_flags, 2048), 2048, 'ENABLED', 'DISABLED') compression
      FROM  sys.sys_dba_segs ds, sys.x$ktfbue e, sys.file$ f
      WHERE e.ktfbuesegfno = ds.relative_fno
      AND   e.ktfbuesegbno = ds.header_block
      AND   e.ktfbuesegtsn = ds.tablespace_id
      AND   ds.tablespace_id = f.ts#
      AND   e.ktfbuefno = f.relfile#
      AND   BITAND(NVL(ds.segment_flags, 0), 1) = 1
      AND   BITAND(NVL(ds.segment_flags,0), 65536) = 0
      AND   ds.tablespace_name LIKE p_tablespace
      AND   (f.file# = l_file_id OR l_file_id IS NULL)
      AND   (f.relfile# = l_relfileno OR l_relfileno IS NULL) --relative file no
	), y as (
	SELECT owner, segment_type, segment_name, partition_name, tablespace_name, file_id, compression
	,      MAX(block_id+blocks-1) max_block_id
	,      MAX(block_id) max_start_block_id --added 20.12.2012
	,      SUM(blocks) blocks
      ,      COUNT(*) extents
      FROM   x
	GROUP BY owner, segment_type, segment_name, partition_name, tablespace_name, file_id, compression
	), z as (
	SELECT owner, segment_type, segment_name, partition_name, tablespace_name, file_id, compression
	,      max_block_id, max_start_block_id --added 20.12.2012
	,      blocks, extents
	,      (
             SELECT SUM(blocks) blocks_free
             FROM   dba_free_space f
             WHERE  f.tablespace_name = y.tablespace_name
             AND    (f.block_id  < y.max_block_id OR y.file_id != f.file_id)
             ) free_blocks_below
	,      ROW_NUMBER() OVER (partition by tablespace_name ORDER BY max_block_id desc) as ranking1
	,      ROW_NUMBER() OVER (partition by tablespace_name, owner, segment_type, segment_name, partition_name ORDER BY max_block_id desc) as ranking2
      FROM y
	)
	SELECT z.*
	, t.block_size, t.min_extlen
	, f.file_name, f.blocks file_blocks
      FROM z, dba_tablespaces t, dba_data_files f
      WHERE z.ranking2 = 1
      AND z.free_blocks_below >= z.blocks --/DECODE(compression,'ENABLED',1,8) --est space avail assume factor 8 compression
	--AND free_blocks_below > 100 --prevent CTD
      AND t.tablespace_name = z.tablespace_name
      AND f.tablespace_name = z.tablespace_name
      AND f.tablespace_name = t.tablespace_name
      AND f.file_id = z.file_id	
      AND (z.ranking1 <= p_max_parts OR p_max_parts IS NULL)
      ORDER BY ranking1 
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>NVL(i.partition_name,i.segment_name));
    msg(i.segment_type||':'||i.owner||'.'||i.segment_name||'.'||i.partition_name);
    msg('Tablespace:'||i.tablespace_name
      ||', file:'||i.file_id
      ||', max block:'||i.max_block_id
      ||', max start block:'||i.max_start_block_id
      ||', blocks:'||i.blocks --added 20.12.2012
      ||', extents:'||i.extents);

    l_cmd1 := '';
    l_cmd2 := '';
    l_cmd3 := '';
    l_cmd4 := '';
    l_granularity := 'AUTO';
    l_rebuildind := FALSE; --by default do not rebuild indexes

    IF i.extents >= g_max_parallelism THEN 
      l_parallelism := TO_NUMBER(NULL);
    ELSE
      l_parallelism := i.extents;
    END IF;
    
    IF i.segment_type LIKE 'TABLE%' THEN
      l_cmd1 := 'ALTER TABLE '||i.owner||'.'||i.segment_name;
      l_cmd4 := l_cmd1;

      IF i.segment_type IN('TABLE SUBPARTITION') THEN --qwert--need to think about compress option for subpartitions
        l_granularity := 'SUBPARTITION';
	  SELECT s.partition_name --get name of parent partition of subpartition
        ,      COALESCE(s.pct_free,p.pct_free,t.pct_free,k_pct_free)
        ,      COALESCE(s.pct_used,p.pct_used,t.pct_used,k_pct_used)
        ,      LTRIM(TO_CHAR(t.degree))
        INTO   l_partition_name, l_pct_free, l_pct_used, l_degree
        FROM   dba_tab_subpartitions s
         INNER JOIN dba_tab_partitions p
         ON     p.table_owner       = s.table_owner
         AND    p.table_name        = s.table_name
         AND    p.partition_name    = s.partition_name
         INNER JOIN dba_tables t
         ON     t.owner             = p.table_owner
         AND    t.table_name        = p.table_name
	  WHERE  s.table_owner       = i.owner
        AND    s.table_name        = i.segment_name
        AND    s.subpartition_name = i.partition_name;
      ELSIF i.segment_type IN('TABLE PARTITION') THEN
        l_granularity := 'PARTITION';
        l_partition_name := i.partition_name;
	  SELECT COALESCE(p.pct_free,t.pct_free,k_pct_free)
        ,      COALESCE(p.pct_used,t.pct_used,k_pct_used)
        ,      LTRIM(TO_CHAR(t.degree))
        INTO   l_pct_free, l_pct_used, l_degree
        FROM   dba_tab_partitions p
         INNER JOIN dba_tables t
         ON     t.owner       = p.table_owner
         AND    t.table_name  = p.table_name
	  WHERE  p.table_owner = i.owner
        AND    p.table_name  = i.segment_name
        AND    p.partition_name = i.partition_name;
      ELSIF i.segment_type IN('TABLE') THEN 
	  SELECT COALESCE(t.pct_free,k_pct_free)
        ,      COALESCE(t.pct_used,k_pct_used)
        ,      LTRIM(TO_CHAR(t.degree))
        INTO   l_pct_free, l_pct_used, l_degree
        FROM   dba_tables t
	  WHERE  t.owner = i.owner
        AND    t.table_name = i.segment_name;
      END IF;

      l_cmd2 := l_cmd1||' MOVE';
      IF i.segment_type LIKE 'TABLE%PARTITION' THEN
        l_cmd1 := l_cmd1||' MODIFY PARTITION '||l_partition_name; --cannot modify subpartition in 10g
        l_cmd2 := l_cmd2||' '||l_granularity||' '||i.partition_name;
      END IF;
      l_cmd2 := l_cmd2||' TABLESPACE '||i.tablespace_name;
      IF p_updateind THEN
        IF i.segment_type LIKE '%PARTITION' THEN
          l_cmd2 := l_cmd2||' UPDATE INDEXES';
        ELSE
           l_rebuildind := TRUE; --need to rebuild unusable indexes
        END IF;
      END IF;

	IF i.extents > 1 THEN --only move in parallel if segment has multiple extents
        l_cmd2 := l_cmd2||' PARALLEL';
        IF l_parallelism IS NOT NULL THEN --18.1.2013 but specify parallelism
          l_cmd2 := l_cmd2||' '||LTRIM(TO_CHAR(l_parallelism));
        END IF;
      ELSE
        l_cmd2 := l_cmd2||' NOPARALLEL';
      END IF;
      l_cmd3 := l_cmd1;

      IF l_pct_free IS NOT NULL THEN
        IF i.compression = 'ENABLED' THEN
          l_cmd1 := l_cmd1||' PCTFREE 0';
        ELSE
          l_cmd1 := l_cmd1||' PCTFREE '||l_pct_free;
        END IF;
        l_cmd3 := l_cmd3||' PCTFREE '||l_pct_free;
      END IF;

      IF l_pct_used IS NOT NULL THEN
        IF i.compression = 'ENABLED' THEN
          l_cmd1 := l_cmd1||' PCTUSED 99';
        ELSE
          l_cmd1 := l_cmd1||' PCTUSED '||l_pct_used;
        END IF;
        l_cmd3 := l_cmd3||' PCTUSED '||l_pct_used;
      END IF;

      IF i.compression = 'ENABLED' THEN
        l_cmd1 := l_cmd1||' COMPRESS';
      END IF;
      l_resize := TRUE;

      IF l_degree = 'DEFAULT' THEN
        l_cmd4 := l_cmd4||' PARALLEL';
      ELSIF l_degree IN('0','1') THEN
        l_cmd4 := l_cmd4||' NOPARALLEL';
      ELSE 
        l_cmd4 := l_cmd4||' PARALLEL '||l_degree;
      END IF;

    ELSIF i.segment_type LIKE 'INDEX%' THEN 
      --note there is no support for index organised tables
      l_cmd2 := 'ALTER INDEX '||i.owner||'.'||i.segment_name;
      l_cmd4 := l_cmd2;

      IF i.segment_type IN('INDEX SUBPARTITION') THEN --qwert--need to think about compress option for subpartitions
        l_granularity := 'SUBPARTITION';
	  SELECT s.partition_name --get name of parent partition of subpartition
        ,      COALESCE(s.pct_free,p.pct_free,t.pct_free,k_pct_free)
        ,      LTRIM(TO_CHAR(t.degree))
        INTO   l_partition_name, l_pct_free, l_degree
        FROM   dba_ind_subpartitions s
         INNER JOIN dba_ind_partitions p
         ON     p.index_owner       = s.index_owner
         AND    p.index_name        = s.index_name
         AND    p.partition_name    = s.partition_name
         INNER JOIN dba_indexes t
         ON     t.owner             = p.index_owner
         AND    t.index_name        = p.index_name
        WHERE  s.index_owner       = i.owner
        AND    s.index_name        = i.segment_name
        AND    s.subpartition_name = i.partition_name;
      ELSIF i.segment_type IN('INDEX PARTITION') THEN
        l_granularity := 'PARTITION';
        l_partition_name := i.partition_name;
	  SELECT COALESCE(p.pct_free,t.pct_free,k_pct_free)
        ,      LTRIM(TO_CHAR(t.degree))
        INTO   l_pct_free, l_degree
        FROM   dba_ind_partitions p
         INNER JOIN dba_indexes t
         ON     t.owner = p.index_owner
         AND    t.index_name = p.index_name
	  WHERE  p.index_owner = i.owner
        AND    p.index_name = i.segment_name
        AND    p.partition_name = i.partition_name;
      ELSIF i.segment_type IN('INDEX') THEN 
	  SELECT COALESCE(t.pct_free,k_pct_free)
        ,      LTRIM(TO_CHAR(t.degree))
        INTO   l_pct_free, l_degree
        FROM   dba_indexes t
	  WHERE  t.owner = i.owner
        AND    t.index_name = i.segment_name;
      END IF;

      IF i.segment_type LIKE 'INDEX SUBPARTITION' THEN
        l_cmd1 := l_cmd2||' MODIFY DEFAULT ATTRIBUTES FOR PARTITION '||l_partition_name; --cannot modify subpartition in 10g
        l_cmd2 := l_cmd2||' REBUILD '||l_granularity||' '||i.partition_name;
        l_cmd2 := l_cmd2||' TABLESPACE '||i.tablespace_name;
        l_cmd3 := l_cmd1;
        IF l_pct_free IS NOT NULL THEN
          l_cmd1 := l_cmd1||' PCTFREE 0';
          l_cmd3 := l_cmd3||' PCTFREE '||l_pct_free;
        END IF;
      ELSIF i.segment_type LIKE 'INDEX PARTITION' THEN
        l_cmd1 := l_cmd2||' MODIFY PARTITION '||l_partition_name; --cannot modify subpartition in 10g
        l_cmd2 := l_cmd2||' REBUILD '||l_granularity||' '||i.partition_name;
        l_cmd2 := l_cmd2||' TABLESPACE '||i.tablespace_name;
        IF l_pct_free IS NOT NULL THEN
          l_cmd2 := l_cmd2||' PCTFREE '||l_pct_free;
        END IF;
        l_cmd3 := l_cmd1;
      ELSE
        l_cmd1 := '';
        l_cmd3 := '';
        l_cmd2 := l_cmd2||' REBUILD';
        l_cmd2 := l_cmd2||' TABLESPACE '||i.tablespace_name;
        IF l_pct_free IS NOT NULL THEN
          l_cmd2 := l_cmd2||' PCTFREE '||l_pct_free;
        END IF;
      END IF;

	IF i.extents > 1 THEN --only move in parallel if segment has multiple extents
        l_cmd2 := l_cmd2||' PARALLEL'; --rebuild index parallel
        IF l_parallelism IS NOT NULL THEN --18.1.2013 but specify parallelism
          l_cmd2 := l_cmd2||' '||LTRIM(TO_CHAR(l_parallelism));
        END IF;
      ELSE 
        l_cmd2 := l_cmd2||' NOPARALLEL'; 
      END IF;

      IF l_degree = 'DEFAULT' THEN
        l_cmd4 := l_cmd4||' PARALLEL';
      ELSIF l_degree IN('0','1') THEN
        l_cmd4 := l_cmd4||' NOPARALLEL';
      ELSE 
        l_cmd4 := l_cmd4||' PARALLEL '||l_degree;
      END IF;

    ELSIF i.segment_type IN('LOBSEGMENT','LOBINDEX') THEN
      IF i.segment_type LIKE 'LOBSEGMENT' THEN
	  SELECT   table_name,   column_name
        INTO   l_table_name, l_column_name
    	  FROM   dba_lobs l
        WHERE  l.owner = i.owner
        AND    l.segment_name = i.segment_name;
      ELSIF i.segment_type LIKE 'LOBINDEX' THEN
  	  SELECT   table_name,   column_name
        INTO   l_table_name, l_column_name
	  FROM   dba_lobs l
        WHERE  l.owner = i.owner
        AND    l.index_name = i.segment_name;
      END IF;
      --set action to table_name.column_name rather than lob segment name
      dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>NVL(i.partition_name,l_table_name)||'.'||i.segment_name);
      l_cmd2 := 'ALTER TABLE '||i.owner||'.'||l_table_name; 
      l_cmd2 := l_cmd2||' MOVE LOB ('||l_column_name||')';
      l_cmd2 := l_cmd2||' STORE AS (TABLESPACE '||i.tablespace_name||')';
      IF p_updateind THEN
        IF i.segment_type LIKE '%PARTITION' THEN
          l_cmd2 := l_cmd2||' UPDATE INDEXES';
        ELSE
          l_rebuildind := TRUE; --need to rebuild unusable indexes
        END IF;
      END IF;
    ELSE --some other kind of segment
      l_resize := FALSE; 
      msg('Cannot process '||i.segment_type||' '||i.segment_name||'.');
    END IF;

    IF l_cmd1 IS NOT NULL THEN
        exec_sql(l_cmd1,p_testmode);
    END IF;

    IF l_cmd2 IS NOT NULL THEN
        l_cmd_resize := 'ALTER DATABASE DATAFILE '''||i.file_name
                        ||''' RESIZE '||CEIL(i.block_size*(i.max_block_id+i.blocks-1)/i.min_extlen+1)*i.min_extlen/1024||'K';
--      exec_sql(l_cmd_resize,p_testmode);

        exec_sql(l_cmd2,p_testmode);
        l_resize := TRUE;

        IF l_rebuildind THEN
          IF i.segment_type LIKE 'TABLE%' THEN
            rebuild_indexes(p_ownname=>i.owner
                           ,p_tabname=>i.segment_name
                           ,p_testmode=>p_testmode);
          ELSIF i.segment_type LIKE 'LOB%' THEN
            rebuild_indexes(p_ownname=>i.owner
                           ,p_tabname=>l_table_name
                           ,p_testmode=>p_testmode);
          END IF;
        END IF;

        IF p_statsjob THEN --added 9.7.2012
          IF i.segment_type LIKE 'TABLE%' THEN
            tab_stats_job(p_ownname=>i.owner
                         ,p_tabname=>i.segment_name
                         ,p_partname=>i.partition_name
                         ,p_granularity=>l_granularity
                         ,p_testmode=>p_testmode);
          ELSIF i.segment_type LIKE 'INDEX%' THEN
            ind_stats_job(p_ownname=>i.owner
                         ,p_indname=>i.segment_name
                         ,p_partname=>i.partition_name
                         ,p_granularity=>l_granularity
                         ,p_testmode=>p_testmode);
          END IF;
        END IF;
    END IF;

    IF l_cmd3 IS NOT NULL THEN
        exec_sql(l_cmd3,p_testmode);
    END IF;

    IF l_cmd4 IS NOT NULL THEN --reset object parallelism
        exec_sql(l_cmd4,p_testmode);
    END IF;

    IF l_resize AND p_resize THEN --added resize parameter 17.7.2012
      l_cmd_resize := 'ALTER DATABASE DATAFILE '''||i.file_name --20.12.2012 altered to use max_start_block_id
                        ||''' RESIZE '||CEIL(i.block_size*i.max_start_block_id/i.min_extlen)*i.min_extlen/1024||'K';
      IF l_cmd_resize IS NOT NULL AND i.max_block_id < i.file_blocks THEN
        exec_sql(l_cmd_resize,p_testmode);
      END IF;
    END IF;

  END LOOP;
  msg(k_module||'.'||k_action||' completed',k_trace_alert);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END high_block_segments;
-----------------------------------------------------------------------------------------------------------
--this procedure trims free space at the top of a data file. To be used in conjuction with high_block_segments 
-----------------------------------------------------------------------------------------------------------
PROCEDURE trim_free_space
(p_tablespace  VARCHAR2 DEFAULT '%'
,p_file_id     NUMBER   DEFAULT NULL
,p_testmode    BOOLEAN  DEFAULT FALSE
) IS
  k_action         CONSTANT VARCHAR2(48) := 'TRIM_FREE_SPACE';
  l_module         VARCHAR2(48);
  l_action         VARCHAR2(32);
  l_cmd            VARCHAR2(200); --ddl command 
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>p_tablespace||'/'||p_file_id);
  msg(k_action||'(tablespace=>'||p_tablespace||',file_id=>'||p_file_id||',testmode=>'||display_bool(p_testmode)||')',k_trace_alert);
  <<datafile_loop>>
  FOR f IN (
    SELECT f.* 
    ,	     t.block_size, t.min_extlen
    FROM   dba_data_files f
    ,      dba_tablespaces t
    WHERE  t.tablespace_name = f.tablespace_name
    AND    t.contents = 'PERMANENT'
--  AND    f.bytes > 2*f.min_extlen --exclude very small tablespaces
    AND    f.tablespace_name LIKE p_tablespace
    AND    t.tablespace_name LIKE p_tablespace
    AND    (f.file_id = p_file_id OR p_file_id IS NULL)
    ORDER BY f.tablespace_name, f.file_id
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module||'.'||k_action, action_name=>f.tablespace_name||'/'||f.file_id);
    msg('Tablespace:'||f.tablespace_name||', file:'||f.file_id||', '||f.blocks||' blocks, '||f.user_blocks||' user blocks, '||f.block_size*f.blocks/1024||'KB');
    <<freespace_loop>>
    FOR s IN (
      SELECT row_number() over (partition by f.tablespace_name, f.file_id order by s.block_id desc) as ranking
      ,      s.block_id 
      ,      s.blocks 
      ,      LEAD(s.block_id,1) over (order by s.block_id) next_free_block
      FROM   dba_free_space s
      WHERE  s.tablespace_name = f.tablespace_name
      AND    s.file_id = f.file_id
      ORDER BY ranking
    ) LOOP
      msg('Free space '||s.ranking||':'||s.blocks||' @ block id:'||s.block_id||'-'||(s.block_id+s.blocks-1)||', next free block id:'||s.next_free_block);
      IF (s.block_id+s.blocks) >= COALESCE(s.next_free_block,f.blocks) AND (f.bytes>2*f.min_extlen) THEN --note not comparing to user blocks
        l_cmd := 'ALTER DATABASE DATAFILE '''||f.file_name||''' RESIZE '||GREATEST(f.block_size*(s.block_id-1),2*f.min_extlen)/1024||'K';
        BEGIN
          exec_sql(l_cmd,p_testmode);
        EXCEPTION WHEN e_data_beyond_size THEN EXIT;
        END;
      ELSIF s.next_free_block IS NULL AND (s.block_id+s.blocks)>=f.user_blocks AND (f.bytes>2*f.min_extlen) THEN --added 19.12.2012, trim to user blocks if no next space
        l_cmd := 'ALTER DATABASE DATAFILE '''||f.file_name||''' RESIZE '||GREATEST(f.block_size*(s.block_id-1),2*f.min_extlen)/1024||'K';
        BEGIN
          exec_sql(l_cmd,p_testmode);
        EXCEPTION WHEN e_data_beyond_size THEN EXIT;
        END;
      ELSE
        EXIT; --break out of inner freespace loop
      END IF;
    END LOOP freespace_loop;
  END LOOP datafile_loop;
  msg(k_module||'.'||k_action||' completed',k_trace_alert);
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END trim_free_space;
----------------------------------------------------------------------------------------------------
END gfc_defrag;
/

show errors
pause
spool off

set wrap on long 5000 lines 100 serveroutput on 
/*--------------------------------------------------------------------------------------------------
--this section contains a number of sample commands
----------------------------------------------------------------------------------------------------
--these commands demonstrate attempts to defragment free space in data files by rebuilding objects 
--in high numbered blocks so that Oracle builds them in free space in lower numbered blocks, the 
--space freed at the end of the data file can be trimmed
--the query shows the data files with the most free space - it makes most sense to start with those
--------------------------------------------------------------------------------------------------
set lines 70
ttitle 'Top 20 Data Files by Free Space'
column ranking1        heading '#'               format 999
column Mb              heading 'Total|Free|MB'   format 99999.9
column max_free_Mb     heading 'Max|Free|MB'     format 99999.9
column max_free_block  heading 'Max|Free|Block'  format 9999999
column largest_free_MB heading 'Largest|Free|MB' format 99999.9
column file_ID         heading 'File|ID'         format 9999
column num_free        heading 'Num|Free|Spaces' format 9999
column tablespace_name heading 'Tablespace|Name' format a17
with x as (select tablespace_name, file_id
, count(*) num_free
, sum(bytes)/1024/1024 Mb
, max(bytes)/1024/1024 largest_free_MB
, max(block_id)*8/1024 max_free_Mb
, max(block_id) max_free_block
from dba_Free_space
group by tablespace_name, file_id
), y as (
select 
  row_number() over (ORDER BY Mb desc) as ranking1
, x.* from x)
select * from y
where ranking1 <= 20 
ORDER BY ranking1
/
ttitle off

--there is a DDL trigger in SYSADM that prevents DDL on partitioned objects
--the following command sets a package global variable that switches the tests off for the current session
--thus permitting DDL
execute sysadm.psft_ddl_lock.set_ddl_permitted(TRUE);


--for each tablespace identified 
--execute gfc_defrag.high_block_segments(p_tablespace=>'GPAPP',p_file_id=>61);

/*
@gfc_defrag_pkg
!pg gfc_defrag_pkg.lst
set serveroutput on timi on	
execute gfc_defrag.high_block_segments(p_tablespace=>'PSIMAGE',p_file_id=>505);
execute gfc_defrag.high_block_segments(p_tablespace=>'PSIMAGE',p_file_id=>495);
execute gfc_defrag.high_block_segments(p_tablespace=>'PSIMAGE',p_file_id=>461);
execute gfc_defrag.high_block_segments(p_tablespace=>'PSIMAGE',p_file_id=>90);

execute gfc_defrag.high_block_segments(p_tablespace=>'PSIMAGE',p_file_id=>450,p_max_parts=>1);
execute gfc_defrag.trim_free_space(p_tablespace=>'PSIMAGE');

execute gfc_defrag.high_block_segments(p_tablespace=>'PSINDEX',p_file_id=>586);
execute gfc_defrag.high_block_segments(p_tablespace=>'PSINDEX',p_file_id=>506);
execute gfc_defrag.high_block_segments(p_tablespace=>'PSINDEX',p_file_id=>499);
execute gfc_defrag.high_block_segments(p_tablespace=>'PSINDEX',p_file_id=>491);
execute gfc_defrag.high_block_segments(p_tablespace=>'PSINDEX',p_file_id=>459);
execute gfc_defrag.high_block_segments(p_tablespace=>'PSINDEX',p_file_id=>122);
execute gfc_defrag.high_block_segments(p_tablespace=>'PSINDEX',p_file_id=>121);
execute gfc_defrag.trim_free_space(p_tablespace=>'PSINDEX');


set serveroutput on timi on	
execute gfc_defrag.high_block_segments(p_tablespace=>'PSDEFAULT',p_resize=>FALSE);

set serveroutput off
set serveroutput on timi on lines 200
execute gfc_defrag.trim_free_space(p_tablespace=>'TL20%');
execute gfc_defrag.high_block_segments(p_tablespace=>'TL2011M12IDX',p_testmode=>FALSE);
execute gfc_defrag.high_block_segments(p_tablespace=>'TL2011M05IDX',p_testmode=>FALSE);
execute gfc_defrag.trim_free_space(p_tablespace=>'TL20%');


--------------------------------------------------------------------------------------------------
--after using high_block_segments to move freespace to end of datafile, trim free space off
--------------------------------------------------------------------------------------------------
set serveroutput on timi on	
execute gfc_defrag.trim_free_space(p_tablespace=>'PSIMAGE');
execute gfc_defrag.trim_free_space(p_tablespace=>'PSIMAGE');
execute gfc_defrag.trim_free_space;
execute sysadm.psft_ddl_lock.set_ddl_permitted(FALSE);

--------------------------------------------------------------------------------------------------*/


