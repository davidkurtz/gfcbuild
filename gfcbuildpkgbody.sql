rem gfcbuildpkgbody.sql
rem (c) Go-Faster Consultancy - www.go-faster.co.uk
rem ***Version History moved to procedure history

set echo on termout on
spool gfcbuildpkgbody

@@psownerid
-------------------------------------------------------------------------------------------------------
-- source Code
-------------------------------------------------------------------------------------------------------
-- debug Levels
-- 5:default debug level
-- 6:print output
-- 7:print end of procedure reset action
-- 9:debug for INS_LINE
-------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY &&ownerid..gfc_pspart AS
-------------------------------------------------------------------------------------------------------
-- spool Files
k_build  CONSTANT INTEGER := 0; -- build peoplesoft tables
k_index  CONSTANT INTEGER := 1; -- rebuild all indexes
k_stats  CONSTANT INTEGER := 2; -- generate CBO stats
k_alter  CONSTANT INTEGER := 3; -- alter main tables, add/split partitions, insert from source table
k_arch1  CONSTANT INTEGER := 4; -- archive build script
k_arch2  CONSTANT INTEGER := 5; -- archive privileges
-------------------------------------------------------------------------------------------------------
k_max_line_length CONSTANT INTEGER := 160; --max lines length
k_max_line_length_margin CONSTANT INTEGER := 128; --max lines length
-------------------------------------------------------------------------------------------------------
k_module      CONSTANT VARCHAR2(64) := $$PLSQL_UNIT;
k_sys_context CONSTANT VARCHAR2(10 CHAR) := 'GFC_PSPART'; -- name of system context
-------------------------------------------------------------------------------------------------------
l_lineno INTEGER := 0;
l_dbname VARCHAR2(8 CHAR);              -- PeopleSoft database name
l_oraver NUMBER;                        -- oracle rdbms version
-- l_parallel_max_servers INTEGER;      -- value of oracle initialisation parameter
l_ptver  VARCHAR2(20 CHAR);             -- peopletools version
l_schema1 VARCHAR2(30 CHAR);            -- schema name
l_schema2 VARCHAR2(31 CHAR);            -- schema name with separator
l_debug BOOLEAN := FALSE;               -- enable debug messages
l_unicode_enabled INTEGER := 0;         -- unicode database
l_database_options INTEGER := 0;        -- database options-- 6.9.2007
l_lf VARCHAR2(1 CHAR) := CHR(10);       -- line feed character
l_drop_purge_suffix VARCHAR2(10 CHAR);  -- use explicit purge clause on drop table - 14.2.2008
l_use_timestamp BOOLEAN := FALSE;       -- use oracle timestamp columns for time and datetime columns

-- the following variables are set via the context
l_chardef VARCHAR2(1 CHAR);             -- permit VARCHAR2 character definition
l_logging VARCHAR2(1 CHAR);             -- set to Y to generate build script that logs
l_parallel_index VARCHAR2(1 CHAR);      -- set to true to enable parallel index build
l_parallel_table VARCHAR2(1 CHAR);      -- set to true to enable parallel index build
l_force_para_dop VARCHAR2(2 CHAR);      -- forced degree of parallelism
l_roles VARCHAR2(1 CHAR);               -- should roles be granted
l_scriptid VARCHAR2(8 CHAR);            -- id string in script and project names
l_update_all VARCHAR2(30 CHAR);         -- name of role than can update PS tables
l_read_all VARCHAR2(30 CHAR);           -- name of role than can read PS tables
l_drop_index VARCHAR2(1 CHAR);          -- if true drops index on exiting table, else alters name of index to old
l_pause VARCHAR2(1 CHAR);               -- if true add pause commands to build script
l_explicit_schema VARCHAR2(1 CHAR);     -- all objects schema explicitly named
l_block_sample VARCHAR2(1 CHAR);        -- use block sampling for statistics
l_build_stats VARCHAR2(1 CHAR);         -- if true analyzes tables as it builds them
l_deletetempstats VARCHAR2(1 CHAR);     -- if true delete and in ORacle 10 also lock stats on temp tables
l_longtoclob VARCHAR2(1 CHAR);          -- if Y then partition and convert longs to clobs if override schema
-- l_ddltrigger VARCHAR2(30 CHAR);      -- name of trigger (T_LOCK) that locks DDL on partitioned objects - removed 5.2.2013
l_ddlenable VARCHAR2(200);              -- command to enable DDL - added 5.2.2013
l_ddldisable VARCHAR2(200);             -- command to disable DDL - added 5.2.2013
l_drop_purge VARCHAR2(1 CHAR);          -- use explicit purge clause on drop table - 14.2.2008
l_forcebuild VARCHAR2(1 CHAR);          -- if true then force build even if structure matches DB
l_desc_index VARCHAR2(1 CHAR);          -- Y to force desc index, N to disable, null to follow PS default
l_repopdfltsub VARCHAR2(1 CHAR);        -- Y to force move data from old to new default list subpartition
l_repopnewmax VARCHAR2(1 CHAR);         -- Y to force add partition by exchange of max value and reinsert
l_rename_parts VARCHAR2(1 CHAR);        -- Y to force rename partition before rebuild - only required in Oracle 8i--20.05.2014
l_split_index_update VARCHAR2(10 CHAR); -- Index update on Split partition: None=No update index clause, ALL=Update Indexes, GLOBAL=UPDATE GLOBAL INDEXES --14.3.2022
l_debug_level INTEGER := 0;             -- variable to hold debug level of package
l_debug_indent INTEGER := 0;

l_noalterprefix VARCHAR2(8 CHAR) := ''; -- if true then do not  generate alters, just for debug-- 6.9.2007

-------------------------------------------------------------------------------------------------------
-- added 26.1.2011 to optionally print debug text during package run time
-------------------------------------------------------------------------------------------------------
PROCEDURE debug_msg(p_text VARCHAR2 DEFAULT ''
                   ,p_debug_level INTEGER DEFAULT 5) IS
BEGIN
  IF p_debug_level <= l_debug_level AND p_text IS NOT NULL THEN
    sys.dbms_output.put_line(LPAD('.',l_debug_indent,'.')||'('||p_debug_level||')'||p_text);
  END IF;
END debug_msg;

-------------------------------------------------------------------------------------------------------
-- added 26.1.2011 to permit debug when setting action
-------------------------------------------------------------------------------------------------------
PROCEDURE set_action(p_action_name VARCHAR2 DEFAULT ''
                    ,p_debug_level INTEGER DEFAULT 5) IS
BEGIN
  l_debug_indent := l_debug_indent + 1;
  dbms_application_info.set_action(action_name=>p_action_name);
  debug_msg(p_text=>'Setting action to: '||p_action_name,p_debug_level=>p_debug_level);
END set_action;
	        
-------------------------------------------------------------------------------------------------------
-- added 26.1.2011 to permit debug code when unseting action
-------------------------------------------------------------------------------------------------------
PROCEDURE unset_action(p_action_name VARCHAR2 DEFAULT ''
                      ,p_debug_level INTEGER DEFAULT 7) IS
BEGIN
  IF l_debug_indent > 0 THEN
    l_debug_indent := l_debug_indent - 1;
  END IF;
  dbms_application_info.set_action(action_name=>p_action_name);
  debug_msg(p_text=>'Resetting action to: '||p_action_name,p_debug_level=>p_debug_level);                
END unset_action;

-------------------------------------------------------------------------------------------------------
-- display debug code
-------------------------------------------------------------------------------------------------------
PROCEDURE debug(p_message VARCHAR2) IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'DEBUG');

  IF l_debug THEN
    sys.dbms_output.put_line(p_message);
  END IF;

  unset_action(p_action_name=>l_action);
END debug;

-------------------------------------------------------------------------------------------------------
FUNCTION show_bool(p_bool BOOLEAN) RETURN VARCHAR IS
BEGIN
  IF p_bool THEN
    RETURN 'TRUE';
  ELSE
    RETURN 'FALSE';
  END IF;
END show_bool;

-------------------------------------------------------------------------------------------------------
-- version history -- need an array
-------------------------------------------------------------------------------------------------------
PROCEDURE history IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>'HISTORY');

  banner;
  sys.dbms_output.put_line('03.12.2002 - improved subrecord handling');
  sys.dbms_output.put_line('11.02.2003 - correction to column sequencing on user'); -- some table names change to bring script in line with colaudit
-- 19.06.2003 - correction to MOD() that processes useedit flags
  sys.dbms_output.put_line('09.07.2003 - nologging facility added');
  sys.dbms_output.put_line('10.07.2003 - oracle version detection added to control fix for 8.1.7.2 bug');
  sys.dbms_output.put_line('05.09.2003 - corrected handling of peoplesoft long character columns');
-- 8.09.2003 - changed statistics script to only build histograms on indexes columns, and default number of buckets
  sys.dbms_output.put_line('18.09.2003 - added trigger to prevent updates on tables whilst being rebuilt');
  sys.dbms_output.put_line('09.10.2003 - tables and indexes set to logging enabled and parallel disabled, parallel control variable added');
-- 9.10.2003 - grants to PS_READ_ALL and PS_UPDATE_ALL roles added to vanilla version
  sys.dbms_output.put_line('28.10.2003 - supress partitioning for tables with long columns'); -- additional partitioned table
  sys.dbms_output.put_line('29.10.2003 - pt version detection to enable new PT8.4 features');
  sys.dbms_output.put_line('04.11.2003 - oracle 9i features, role name control');
  sys.dbms_output.put_line('17.11.2003 - rename/drop index');
  sys.dbms_output.put_line('07.01.2004 - explicit schema name, script control');
  sys.dbms_output.put_line('22.03.2004 - oracle 9 varchars in characters');
  sys.dbms_output.put_line('27.09.2004 - suppress disabled index build, but force disabled index to drop');
  sys.dbms_output.put_line('18.04.2005 - support for PeopleSoft temporary tables'); -- backported from gtbuild.sql
-- 22.06.2005 - cloned from GPBUILD.sql
-- 25.07.2006 - applied GPBUILD corrections, and remove chartfield fudge
  sys.dbms_output.put_line('05.12.2006 - added handling for hash-partitioned only tables');
  sys.dbms_output.put_line('13.12.2006 - remove partitioning column check');
  sys.dbms_output.put_line('06.09.2007 - enhancement for PeopleTools 8.48, support partitioned function based indexes');
  sys.dbms_output.put_line('08.11.2007 - support for range-list composite partitioning');
  sys.dbms_output.put_line('14.02.2008 - selective list partition -v- range build, drop purge, all keys on subrecords');
  sys.dbms_output.put_line('28.08.2008 - conversion to package procedure');
  sys.dbms_output.put_line('16.09.2008 - DDL moved to gfcbuildtab');
  sys.dbms_output.put_line('16.12.2008 - Single table build options can be combined');
  sys.dbms_output.put_line('23.01.2009 - Partitioning columns in unique indexes must not be descending');
  sys.dbms_output.put_line('01.04.2009 - Corrections to add partition scripts');
  sys.dbms_output.put_line('18.04.2009 - Override Default Application Designer Project Name');
  sys.dbms_output.put_line('03.06.2009 - Extended check descending partitioning columns in unique indexes to subrecords');
  sys.dbms_output.put_line('23.04.2010 - Add ability to add range partitions by splitting');
  sys.dbms_output.put_line('01.05.2010 - Correct setting new table LOGGING NOPARALLEL');
  sys.dbms_output.put_line('05.05.2010 - Create table only if doesi not exist, add Colums when altering table');
  sys.dbms_output.put_line('17.05.2010 - Temporary Record that are not declared to any AE can be GTT');
  sys.dbms_output.put_line('08.06.2010 - Specified Source Table');
  sys.dbms_output.put_line('16.06.2010 - Function Based Index support enhanced');
  sys.dbms_output.put_line('01.10.2010 - Insert into copy table within PL/SQL block');
  sys.dbms_output.put_line('10.12.2010 - PT8.50 uses bit 5 of database options, so just look at bit 1');
  sys.dbms_output.put_line('16.01.2011 - Optionally use database parallelism to copy tables by adding hints');
  sys.dbms_output.put_line('29.03.2011 - Add parameter to move data from default to new list partition');
  sys.dbms_output.put_line('26.05.2011 - Add support for partitioned index organised tables');
  sys.dbms_output.put_line('14.07.2011 - Add support for partition archive management');
  sys.dbms_output.put_line('08.03.2012 - Support for partitioning PeopleSoft history tables');
  sys.dbms_output.put_line('29.06.2012 - Add support non-partition source tables');
  sys.dbms_output.put_line('01.11.2012 - Enable parallel DML during copy operation');
-- sys.dbms_output.put_line('01.11.2012 - Minor fix for parallel DML');
  sys.dbms_output.put_line('30.01.2013 - Bugfix Temp Table Column List');
  sys.dbms_output.put_line('05.02.2013 - Change to DDL trigger control');
  sys.dbms_output.put_line('04.03.2013 - Remove insert/select on table with source table from alter script');
-- sys.dbms_output.put_line('11.03.2013 - Fix storage clause of 2nd partition in partition split');
  sys.dbms_output.put_line('12.03.2013 - Option to exchange maxvalue range partition out and reinsert data');
  sys.dbms_output.put_line('15.03.2013 - Add archive flag processing to add subpartition processing');
  sys.dbms_output.put_line('22.03.2013 - Add archive insert script to incrementally populate archive tables');
  sys.dbms_output.put_line('27.03.2013 - Global Partition Index Archive/Purge fix');
  sys.dbms_output.put_line('15.04.2013 - Support for Oracle Timestamp column type added');
  sys.dbms_output.put_line('14.08.2013 - Global Temporary Tables not supressed by restartable AE with 0 instances');
  sys.dbms_output.put_line('28.08.2013 - Fix to drop all not just partitioned indexes on table rebuild');
  sys.dbms_output.put_line('11.12.2013 - No default roles.  They must now be specified');
  sys.dbms_output.put_line('23.01.2014 - Fix to only add subpartitons separately in parent partitions already exist');
  sys.dbms_output.put_line('20.05.2014 - Suppress partition rename, reinstate with option');
  sys.dbms_output.put_line('20.10.2014 - Support for range sub-partitioning added');
  sys.dbms_output.put_line('07.01.2015 - Fix indexes on subrecords in temporary tables');
  sys.dbms_output.put_line('02.03.2015 - Support for Interval partitioning');
  sys.dbms_output.put_line('01.08.2017 - Support for add partition to composite range partitioned table with maxvalue');
  sys.dbms_output.put_line('17.09.2019 - Added sys_context( to read current_schema if not running as SYSADM');
  sys.dbms_output.put_line('10.08.2020 - From 19c, v$version only has a single version row');
  sys.dbms_output.put_line('26.11.2020 - Support for partial indexing');
  sys.dbms_output.put_line('13.05.2021 - Permit same sub-partitioning type with different PART_ID');
  sys.dbms_output.put_line('08.07.2021 - Add Session parallel force DDL for parallel index creation with automatic parallelism');
  sys.dbms_output.put_line('16.01.2022 - Correct table name in index commands to use custom table names');
  sys.dbms_output.put_line('10.03.2022 - remove physical attribute from index subpartition');
  sys.dbms_output.put_line('14.03.2022 - control index rebuild on split partition');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END history;

-------------------------------------------------------------------------------------------------------
-- read defaults from contexts
-------------------------------------------------------------------------------------------------------
PROCEDURE reset_variables IS
l_module v$session.module%type;
l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'RESET_VARIABLES');

  l_chardef := 'N';                -- permit VARCHAR2 character definition
  l_logging := 'N';                -- set to Y to generate build script that logs
  l_parallel_table := 'Y';         -- set to true to enable parallelism on table copy
  l_parallel_index := 'Y';         -- set to true to enable parallel index build
  l_force_para_dop := '';          -- value to append to parallel keyword
  l_roles := 'N';                  -- should roles be granted
  l_scriptid := 'GFCBUILD';        -- id string in script and project names
  l_update_all := '';              -- 11.12.2013 no default update all role any more
  l_read_all := '';                -- 11.12.2013 no default real all role any more
  l_drop_index := 'Y';             -- if true drops index on exiting table, else alters name of index to old
  l_pause := 'N';                  -- if true add pause commands to build script
  l_explicit_schema := 'Y';        -- all objects schema explicitly named
  l_block_sample := 'N';           -- use block sampling for statistics --changed to N 13.5.2021
  l_build_stats := 'N';            -- if true analyzes tables as it builds them
  l_deletetempstats := 'Y';        -- if true delete and in ORacle 10 also lock stats on temp tables
  l_longtoclob := 'N';             -- if Y then partition and convert longs to clobs if override schema
-- l_ddltrigger := '';             -- name of trigger (T_LOCK) that locks DDL on partitioned objects - removed 5.2.2013
  l_ddlenable := '';               -- command to enable DDL - added 5.2.2013
  l_ddldisable := '';              -- command to disable DDL - added 5.2.2013
  l_drop_purge := 'Y';             -- use explicit purge clause on drop table - 14.2.2008
  l_noalterprefix := '';           -- if true then do not  generate alters, just for debug-- 6.9.2007
  l_forcebuild := 'Y';             -- if true then force build even if structure matches DB
  l_desc_index := 'Y';             -- y to force desc index, N to disable, null to follow PS default
  l_repopdfltsub := 'N';           -- by default will just exchange default subpartition back
  l_repopnewmax := 'N';            -- by default split maxvalue partition rather than exchange and reinsert - added 11.3.2013
  l_rename_parts := 'N';           -- by default do not rename partitions before rebuild - left over from Oracle 8i - added 20.5.2014
  l_split_index_update := 'ALL';   -- by default update all indexes on split partition --14.03.2022
  l_debug_level := 0;		   -- default debug level is 0

  unset_action(p_action_name=>l_action);

END reset_variables;

-------------------------------------------------------------------------------------------------------
-- read defaults from contexts
-------------------------------------------------------------------------------------------------------
PROCEDURE read_context IS
  l_module v$session.module%type;
  l_action v$session.action%type; 
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'READ_CONTEXT');

  reset_variables;
  l_chardef            := NVL(SYS_CONTEXT(k_sys_context,'chardef'           ),l_chardef);
  l_logging            := NVL(SYS_CONTEXT(k_sys_context,'logging'           ),l_logging);
  l_parallel_table     := NVL(SYS_CONTEXT(k_sys_context,'parallel_table'    ),l_parallel_table);
  l_parallel_index     := NVL(SYS_CONTEXT(k_sys_context,'parallel_index'    ),l_parallel_index);
  l_force_para_dop     := NVL(SYS_CONTEXT(k_sys_context,'force_para_dop'    ),l_force_para_dop);
  l_roles              := NVL(SYS_CONTEXT(k_sys_context,'roles'             ),l_roles);
  l_scriptid           := NVL(SYS_CONTEXT(k_sys_context,'scriptid'          ),l_scriptid);
  l_update_all         := NVL(SYS_CONTEXT(k_sys_context,'update_all'        ),l_update_all);
  l_read_all           := NVL(SYS_CONTEXT(k_sys_context,'read_all'          ),l_read_all);
  l_drop_index         := NVL(SYS_CONTEXT(k_sys_context,'drop_index'        ),l_drop_index);
  l_pause              := NVL(SYS_CONTEXT(k_sys_context,'pause'             ),l_pause);
  l_explicit_schema    := NVL(SYS_CONTEXT(k_sys_context,'explicit_schema'   ),l_explicit_schema);
  l_block_sample       := NVL(SYS_CONTEXT(k_sys_context,'block_sample'      ),l_block_sample);
  l_build_stats        := NVL(SYS_CONTEXT(k_sys_context,'build_stats'       ),l_build_stats);
  l_deletetempstats    := NVL(SYS_CONTEXT(k_sys_context,'deletetempstats'   ),l_deletetempstats);
  l_longtoclob         := NVL(SYS_CONTEXT(k_sys_context,'longtoclob'        ),l_longtoclob);
--l_ddltrigger         := NVL(SYS_CONTEXT(k_sys_context,'ddltrigger'        ),l_ddltrigger);
  l_ddlenable          := NVL(SYS_CONTEXT(k_sys_context,'ddlenable'         ),l_ddlenable);
  l_ddldisable         := NVL(SYS_CONTEXT(k_sys_context,'ddldisable'        ),l_ddldisable);
  l_drop_purge         := NVL(SYS_CONTEXT(k_sys_context,'drop_purge'        ),l_drop_purge);
--l_noalterprefix      := NVL(SYS_CONTEXT(k_sys_context,'noalterprefix'     ),l_noalterprefix);
  l_forcebuild         := NVL(SYS_CONTEXT(k_sys_context,'forcebuild'        ),l_forcebuild);
  l_desc_index         := NVL(SYS_CONTEXT(k_sys_context,'desc_index'        ),l_desc_index);
  l_repopdfltsub       := NVL(SYS_CONTEXT(k_sys_context,'repopdfltsub'      ),l_repopdfltsub);
  l_repopnewmax        := NVL(SYS_CONTEXT(k_sys_context,'repopnewmax'       ),l_repopnewmax);
  l_rename_parts       := NVL(SYS_CONTEXT(k_sys_context,'rename_parts'      ),l_rename_parts); --20.05.2014
  l_split_index_update := NVL(SYS_CONTEXT(k_sys_context,'split_index_update'),l_split_index_update); --14.03.2022

  l_debug_level        := NVL(TO_NUMBER(SYS_CONTEXT(k_sys_context,'debug_level')),l_debug_level);

  unset_action(p_action_name=>l_action);
END read_context;

-------------------------------------------------------------------------------------------------------
-- write defaults to context
-------------------------------------------------------------------------------------------------------
PROCEDURE write_context IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'WRITE_CONTEXT');

  dbms_session.set_context(k_sys_context,'chardef'           ,l_chardef);
  dbms_session.set_context(k_sys_context,'logging'           ,l_logging);
  dbms_session.set_context(k_sys_context,'parallel_table'    ,l_parallel_table);
  dbms_session.set_context(k_sys_context,'parallel_index'    ,l_parallel_index);
  dbms_session.set_context(k_sys_context,'force_para_dop'    ,l_force_para_dop);
  dbms_session.set_context(k_sys_context,'roles'             ,l_roles);
  dbms_session.set_context(k_sys_context,'scriptid'          ,l_scriptid);
  dbms_session.set_context(k_sys_context,'update_all'        ,l_update_all);
  dbms_session.set_context(k_sys_context,'read_all'          ,l_read_all);
  dbms_session.set_context(k_sys_context,'drop_index'        ,l_drop_index);
  dbms_session.set_context(k_sys_context,'pause'             ,l_pause);
  dbms_session.set_context(k_sys_context,'explicit_schema'   ,l_explicit_schema);
  dbms_session.set_context(k_sys_context,'block_sample'      ,l_block_sample);
  dbms_session.set_context(k_sys_context,'build_stats'       ,l_build_stats);
  dbms_session.set_context(k_sys_context,'deletetempstats'   ,l_deletetempstats);
  dbms_session.set_context(k_sys_context,'longtoclob'        ,l_longtoclob);
--dbms_session.set_context(k_sys_context,'ddltrigger'        ,l_ddltrigger);
  dbms_session.set_context(k_sys_context,'ddlenable'         ,l_ddlenable);
  dbms_session.set_context(k_sys_context,'ddldisable'        ,l_ddldisable);
  dbms_session.set_context(k_sys_context,'drop_purge'        ,l_drop_purge);
--dbms_session.set_context(k_sys_context,'noalterprefix'     ,l_noalterprefix);
  dbms_session.set_context(k_sys_context,'forcebuild'        ,l_forcebuild);
  dbms_session.set_context(k_sys_context,'desc_index'        ,l_desc_index);
  dbms_session.set_context(k_sys_context,'repopdfltsub'      ,l_repopdfltsub);
  dbms_session.set_context(k_sys_context,'repopnewmax'       ,l_repopnewmax);
  dbms_session.set_context(k_sys_context,'rename_parts'      ,l_rename_parts); --20.05.2014
  dbms_session.set_context(k_sys_context,'split_index_update',l_split_index_update); --14.03.2022
  dbms_session.set_context(k_sys_context,'debug_level'       ,TO_CHAR(l_debug_level));

  unset_action(p_action_name=>l_action);
END write_context;

-------------------------------------------------------------------------------------------------------
-- get name of PeopleSoft database
-- added 17.9.2019 added sys_context to read current_schema if not running as SYSADM
-------------------------------------------------------------------------------------------------------
PROCEDURE dbname IS
  l_module   v$session.module%type;
  l_action   v$session.action%type;
  l_username v$session.username%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'DBNAME');

  l_username := NVL(sys_context('userenv','current_schema'),user);

  SELECT  UPPER(ownerid), MAX(dbname)
  INTO    l_schema1,l_dbname
  FROM    ps.psdbowner
  WHERE   UPPER(ownerid) = l_username
  GROUP BY UPPER(ownerid);
  
  IF l_explicit_schema = 'Y' THEN
    l_schema2 := LOWER(l_schema1)||'.';
  ELSE
    l_schema2 := '';
  END IF;

  unset_action(p_action_name=>l_action);
END dbname;
-------------------------------------------------------------------------------------------------------
-- get version of Oracle
-------------------------------------------------------------------------------------------------------
PROCEDURE oraver IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'ORAVER');

  SELECT  REGEXP_SUBSTR(banner, '[0-9]+\.[0-9]+') version
  INTO    l_oraver
  FROM    v$version
  WHERE   rownum <= 1
  ;

  IF l_oraver < 10 THEN
    l_drop_purge := 'N';
    l_drop_purge_suffix := '';
  ELSE	
    l_drop_purge_suffix := ' PURGE';
  END IF;

-- sELECT  TO_NUMBER(value)
-- iNTO    l_parallel_max_servers
--FROM    v$parameter
-- wHERE   name = 'parallel_max_servers'
--;

  unset_action(p_action_name=>l_action);
END oraver;

-------------------------------------------------------------------------------------------------------
-- get version of PeopleTools
-------------------------------------------------------------------------------------------------------
PROCEDURE ptver IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'PTVER');

  -- 6.9.2007 added PT8.48 unicode/long logic
  SELECT  toolsrel, unicode_enabled
  INTO    l_ptver, l_unicode_enabled
  FROM    psstatus
  ;

  IF l_ptver < '8.15' AND l_desc_index IS NULL THEN
    l_desc_index := 'N';
  END IF;

  IF l_ptver >= '8.48' THEN
    l_logging := 'N';
    -- 10.12.2010 in PT8.50 bit 5 of database options is used for something else, so just look at bit 1
    -- 15.4.2013 - bit 1 indicates CLOB/BLOB usage and character semantics for unicode, bit 5 indicate timestamp for time and datatime fields.
    EXECUTE IMMEDIATE 'SELECT database_options FROM psstatus' INTO l_database_options;

    IF BITAND(l_database_options,2) = 2 THEN
      l_unicode_enabled := 0; -- 6.9.2007-character semantics
      l_longtoclob := 'Y'; -- use clobs              
    END IF;

    IF BITAND(l_database_options,32) = 32 THEN
      l_use_timestamp := TRUE;
    ELSE
      l_use_timestamp := FALSE;
    END IF;

    IF l_desc_index IS NULL THEN
      l_desc_index := 'Y';
    END IF;
  END IF;

  unset_action(p_action_name=>l_action);
END ptver;

-------------------------------------------------------------------------------------------------------
-- make a project of the interested tables
-------------------------------------------------------------------------------------------------------
PROCEDURE gfc_project(p_projectname VARCHAR2 DEFAULT '') IS
  l_version INTEGER;
  l_version2 INTEGER;
  l_projectname VARCHAR2(20 CHAR) := UPPER(l_scriptid);
  l_sql VARCHAR2(32767);
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'GFC_PROJECT');

  IF p_projectname IS NOT NULL THEN
      l_projectname := p_projectname;
  END IF;

  UPDATE  PSLOCK
  SET     version = version + 1
  WHERE   objecttypename IN ('PJM','SYS');

  UPDATE  PSVERSION
  SET     version = version + 1
  WHERE   objecttypename IN ('PJM','SYS');

  SELECT  version
  INTO    l_version
  FROM    PSLOCK
  WHERE   objecttypename IN ('PJM') FOR UPDATE OF version;

  SELECT  version
  INTO    l_version2
  FROM    psversion
  WHERE   objecttypename IN ('PJM') FOR UPDATE OF version;

  l_version := GREATEST(l_version,l_version2);
  l_version2 := l_version;

  DELETE FROM PSPROJECTDEL        WHERE PROJECTNAME = l_projectname;
  DELETE FROM psprojectitem       WHERE PROJECTNAME = l_projectname;
  DELETE FROM PSPROJDEFNLANG      WHERE PROJECTNAME = l_projectname;
  DELETE FROM PSPROJECTSEC        WHERE PROJECTNAME = l_projectname;
  DELETE FROM PSPROJECTINC        WHERE PROJECTNAME = l_projectname;
  DELETE FROM PSPROJECTDEP        WHERE PROJECTNAME = l_projectname;
  DELETE FROM PSPROJECTDEFN       WHERE PROJECTNAME = l_projectname;

  INSERT INTO psprojectitem
  (      PROJECTNAME, OBJECTTYPE, OBJECTID1, OBJECTVALUE1,
         OBJECTID2, OBJECTVALUE2, OBJECTID3, OBJECTVALUE3,
         OBJECTID4, OBJECTVALUE4, NODETYPE, SOURCESTATUS,
         TARGETSTATUS, UPGRADEACTION, TAKEACTION, COPYDONE)
  SELECT l_projectname,0,1,r.recname,
         0,' ',0,' ',
         0,' ',0,0,
         0,2,1,0
  FROM   psrecdefn r,
         gfc_ps_tables e
  WHERE  e.recname = r.recname
  ;

  INSERT INTO psprojectitem
  (      PROJECTNAME, OBJECTTYPE, OBJECTID1, OBJECTVALUE1, 
         OBJECTID2, OBJECTVALUE2, OBJECTID3, OBJECTVALUE3, 
         OBJECTID4, OBJECTVALUE4, NODETYPE, SOURCESTATUS, 
         TARGETSTATUS, UPGRADEACTION, TAKEACTION, COPYDONE)
  SELECT DISTINCT l_projectname,1,1,k.recname,
         24,k.indexid,0,' ',
         0,' ',0,0,
         0,0,1,0
  FROM   gfc_ps_tables e,
         psrecfielddb f,
         pskeydefn k
  WHERE  f.recname = e.recname
  AND    k.recname = f.recname_parent
  AND    k.fieldname = f.fieldname
  ;

  l_sql := 'INSERT INTO '||l_schema2||'PSPROJECTDEFN ('
            ||'VERSION, PROJECTNAME, TGTSERVERNAME, TGTDBNAME, TGTOPRID, '
            ||'TGTOPRACCT, REPORTFILTER, TGTORIENTATION, COMPARETYPE, KEEPTGT, '
            ||'COMMITLIMIT, MAINTPROJ, COMPRELEASE, COMPRELDTTM,';
  IF l_ptver >= '8.4' THEN -- new column in pt8.4
          l_sql := l_sql || 'OBJECTOWNERID, ';
  END IF;
  l_sql := l_sql  ||'LASTUPDDTTM, LASTUPDOPRID, PROJECTDESCR, '
        ||'RELEASELABEL, RELEASEDTTM, DESCRLONG) '
        ||'VALUES ('||l_version||','''||l_projectname||''','' '','' '','' '','
        ||''' '',16232832,0,1,3,'
        ||'50,0,'' '', null,' ;
  IF l_ptver >= '8.4' THEN -- new column in pt8.4
      l_sql := l_sql || ''' '',' ;
  END IF;
      l_sql := l_sql  ||'sysdate,''PS'',''Partitioned + Global Temp Tabs'', '' '', NULL, '
            ||'''Partitioned and Global Temporary Tables generated by gfcbuild script on '
            ||TO_CHAR(SYSDATE,'dd.mm.yyyy')||''' )';

  EXECUTE IMMEDIATE l_sql;

  unset_action(p_action_name=>l_action);
END gfc_project;

-------------------------------------------------------------------------------------------------------
-- populate list of tables
-- 19.6.2003 list of tables reorganised
-------------------------------------------------------------------------------------------------------
PROCEDURE gfc_ps_tables
(p_part_id VARCHAR2 DEFAULT ''
,p_recname VARCHAR2 DEFAULT ''
,p_rectype VARCHAR2 DEFAULT 'A'
) IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'GFC_PS_TABLES');

  MERGE INTO gfc_ps_tables u -- 19.4.2013 converted to merge statement
  USING (
  SELECT  r.recname, r.rectype
  ,       DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
  ,       'P' table_type
  ,       0 temptblinstances
  ,       p.override_schema
  FROM    psrecdefn r
  ,       gfc_part_tables p
  WHERE   r.rectype = 0
  AND     r.recname = p.recname -- reverted 29.6.2012 
  AND     (p.part_id LIKE p_part_id OR p_part_id IS NULL)
  AND     (p.recname LIKE p_recname OR p_recname IS NULL)
  AND     (p_rectype IN('A','P') OR p_rectype IS NULL)
  UNION -- add archive record - split out 29.6.2012 to support non-partition source table name
  SELECT  r.recname, r.rectype
  ,       DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
  ,      'P' table_type
  ,       0 temptblinstances
  ,       p.override_schema 
  FROM    psrecdefn r
  ,       gfc_part_tables p
  WHERE   r.rectype = 0
  AND     r.recname = p.arch_recname 
  AND     (p.part_id LIKE p_part_id OR p_part_id IS NULL)
  AND     (  p.arch_recname LIKE p_recname -- removed 29.6.2012
          OR p_recname IS NULL)
  AND  (p_rectype IN('A','P') OR p_rectype IS NULL)
  UNION -- this query adds source tables
  SELECT  r.recname, r.rectype,
          DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name,
          'P' table_type
  ,       0 temptblinstances
  ,       '' src_table_schema -- 29.6.2012 might add this field in the future
  FROM    psrecdefn r
  ,       gfc_part_tables p
  WHERE   r.rectype = 0
  AND     p.src_table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
  AND     (p.part_id LIKE p_part_id OR p_part_id IS NULL)
  AND     (p.recname LIKE p_recname OR p_recname IS NULL)
  AND     (p_rectype IN('A','P') OR p_rectype IS NULL)
  UNION -- add any temp records
  SELECT  r.recname, r.rectype
  ,       DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
  ,       'T' table_type
  ,        0 temptblinstances
  ,       '' override_schema
  FROM    psrecdefn r
  ,       gfc_temp_tables t
  WHERE   r.recname = t.recname
  AND     (  r.rectype = 0
          OR  (   r.rectype = 7
      AND NOT r.recname IN(
          SELECT   a.recname
          FROM  psaeappldefn b
          ,   psaeappltemptbl a
          WHERE  a.ae_applid = b.ae_applid
          AND  b.ae_disable_restart = 'N'
          AND  b.ae_appllibrary = 'N' -- added 14.8.2013-do not suppress GTT if restartable AE appl library refs temp tables
          AND  b.temptblinstances > 0 -- added 14.8.2013-do not suppress GTT if restartable AE subprogram refs temp tables with 0 instances
          )
    )
          )
  AND     (t.recname LIKE p_recname OR p_recname IS NULL)
  AND     (p_rectype IN('A','T') OR p_rectype IS NULL)
  ) s
  ON (u.recname = s.recname)
  WHEN MATCHED THEN UPDATE 
  SET  u.rectype          = s.rectype
  ,    u.table_name       = s.table_name
  ,    u.temptblinstances = s.temptblinstances
  ,    u.override_schema  = s.override_schema
  WHEN NOT MATCHED THEN INSERT 
         (u.recname, u.rectype, u.table_name, u.table_type, u.temptblinstances, u.override_schema)
  VALUES (s.recname, s.rectype, s.table_name, s.table_type, s.temptblinstances, s.override_schema)
  ;

-------------------------------------------------------------------------------------------------------
-- 17.5.2010 this test was previously in the above statement - but PeopleSoft delivers temp tables that  
-- are not declared to a process, and the first part looks for tables associated with a non-restartable
-- process
-------------------------------------------------------------------------------------------------------
--        AND     r.recname IN(
--  SELECT   a.recname
--  FROM  psaeappldefn b
--  ,   psaeappltemptbl a
--  WHERE   a.ae_applid = b.ae_applid
--  AND  b.ae_disable_restart = 'Y'
--  MINUS
--  SELECT   a.recname
--  FROM  psaeappldefn b
--  ,   psaeappltemptbl a
--  WHERE   a.ae_applid = b.ae_applid
--  AND  b.ae_disable_restart = 'N'
--          )
-------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------
-- 17.5.2010 correction to update to handle tables not declared to AEs
-------------------------------------------------------------------------------------------------------
  UPDATE gfc_ps_tables g
  SET    temptblinstances = (
         SELECT NVL(o.temptblinstances+t.temptblinstances,0) temptblinstances
         FROM   psoptions o
         ,      gfc_ps_tables g1
          LEFT OUTER JOIN pstemptblcntvw t
          ON   t.recname = g1.recname
         WHERE g1.recname = g.recname)
  WHERE  g.rectype = 7
  ;

-- 30.01.2013 -- two inserts first puts in temp and permanent tables.
  INSERT INTO gfc_ora_tab_columns
           (table_name, column_name, column_id)
  SELECT  c.table_name, c.column_name, c.column_id
  FROM    user_tab_columns c
  ,       gfc_ps_tables g
  ,       gfc_temp_tables t -- added 30.1.2013
  WHERE   g.table_name = c.table_name
  AND     t.recname = g.recname
  AND     (g.recname LIKE p_recname OR p_recname IS NULL) 
  AND NOT EXISTS( -- 18.12.2008 -- added criteria on p_recname to prevent duplicate inserts
          SELECT 'x'
          FROM  gfc_ora_tab_columns o
          WHERE  o.table_name = c.table_name
          AND   o.column_name = c.column_name)
  ;

  INSERT INTO gfc_ora_tab_columns
  (table_name, column_name, column_id)
  SELECT DISTINCT c.table_name, c.column_name, c.column_id
  FROM   user_tab_columns c
  ,      gfc_ps_tables g
  ,      gfc_part_tables p
  WHERE  g.table_name = c.table_name
  AND    (  g.recname = p.recname
         OR g.table_name = p.src_table_name -- 29.6.2012 added to support nonpartitioned source tablename
         ) 
  AND    (p.part_id LIKE p_part_id OR p_part_id IS NULL)
  AND    (  p.recname LIKE p_recname 
         OR p.arch_recname LIKE p_recname 
         OR p_recname IS NULL
         )
  AND NOT EXISTS( -- 18.12.2008 -- added criteria on p_recname to prevent duplicate inserts
         SELECT 'x'
         FROM  gfc_ora_tab_columns o
         WHERE  o.table_name = c.table_name
         AND   o.column_name = c.column_name)
  ;
  unset_action(p_action_name=>l_action);
END gfc_ps_tables;

-------------------------------------------------------------------------------------------------------
-- populate table of columns
-------------------------------------------------------------------------------------------------------
PROCEDURE gfc_ps_tab_columns
(p_part_id VARCHAR2 DEFAULT ''
,p_recname VARCHAR2 DEFAULT ''
) IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'GFC_PS_TAB_COLUMNS');

  INSERT INTO gfc_ps_tab_columns -- 30.1.2013 added to make sure any temp tables also go in
  (      recname, fieldname, useedit, fieldnum, subrecname)
  SELECT DISTINCT r.recname, f.fieldname, f.useedit, f.fieldnum
  , f.recname_parent --7.1.2015 changed from r.recname 
  FROM   gfc_ps_tables r
  ,	 gfc_temp_tables t -- 30.1.2013 this query only does temp record
  ,      psrecfielddb f
  WHERE  r.recname = f.recname
  AND    t.recname = r.recname
  AND NOT EXISTS(-- 18.12.2008 -- added criteria on p_recname to prevent duplicate inserts
         SELECT 'x' 
         FROM   gfc_ps_tab_columns c
         WHERE  c.recname = f.recname
         AND    c.fieldname = f.fieldname)
  AND    (r.recname LIKE p_recname OR p_recname IS NULL) 
  ;

  INSERT INTO gfc_ps_tab_columns
  (       recname, fieldname, useedit, fieldnum, subrecname)
  SELECT  DISTINCT f.recname, f.fieldname, f.useedit, f.fieldnum, f.recname_parent
  FROM    gfc_ps_tables r
  ,       gfc_part_tables p
  ,       psrecfielddb f
  WHERE   r.recname = f.recname
  AND     (  r.recname = p.recname 
          OR r.table_name = p.src_table_name
          ) -- added 29.6.2012 to support non-partition source table name
  AND     (p.part_id LIKE p_part_id OR p_part_id IS NULL) --changed to LIKE 3.3.2015
  AND     (  p.recname LIKE p_recname 
          OR p.arch_recname LIKE p_recname 
          OR p_recname IS NULL
          )
  AND NOT EXISTS( -- 18.12.2008 -- added criteria on p_recname to prevent duplicate inserts
          SELECT 'x'
          FROM   gfc_ps_tab_columns c
          WHERE  c.recname = f.recname
          AND	 c.fieldname = f.fieldname)
  ;

  unset_action(p_action_name=>l_action);
END gfc_ps_tab_columns;

-------------------------------------------------------------------------------------------------------
-- populate table of indexes - dmk
-------------------------------------------------------------------------------------------------------
PROCEDURE gfc_ps_indexdefn 
(p_part_id VARCHAR2 DEFAULT ''
,p_recname VARCHAR2 DEFAULT ''
) IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'GFC_PS_INDEXDEFN');

  MERGE INTO gfc_ps_indexdefn u USING ( -- 16.6.2010 rewritten as merge
  SELECT DISTINCT x.recname, x.indexid
  ,      x.subrecname
  ,      x.subindexid
  ,      x.platform_ora
  ,      x.custkeyorder
  ,      NVL(j.uniqueflag,x.uniqueflag) uniqueflag -- 6.9.2007 added psindexdefn psindexdefn
  FROM   (
    SELECT c.recname, i.indexid
    ,      c.recname subrecname
    ,      i.indexid subindexid
    ,      i.platform_ora
    ,      i.custkeyorder
    ,      i.uniqueflag
    FROM   gfc_ps_tab_columns c
    ,      psindexdefn i
    WHERE  i.recname = c.subrecname
    AND    (c.recname LIKE p_recname OR p_recname IS NULL)
  ) x
  LEFT OUTER JOIN psindexdefn j
    ON  j.recname = x.recname
    AND j.indexid = x.indexid
  UNION
  SELECT c.recname, i.indexid -- indexes defined on record -- added in case all keys on subrecords
  ,      c.recname subrecname
  ,      i.indexid subindexid
  ,      i.platform_ora
  ,      i.custkeyorder
  ,      i.uniqueflag
  FROM   gfc_ps_tab_columns c
  ,      psindexdefn i
  WHERE  i.recname = c.recname
  AND    (c.recname LIKE p_recname OR p_recname IS NULL) 
  UNION
  SELECT  DISTINCT x.recname, x.indexid
  ,       x.subrecname, x.subindexid, i.platform_ora
  ,       i.custkeyorder, i.uniqueflag -- 6.9.2007 added psindexdefn fields
  FROM    gfc_ps_alt_ind_cols x
  ,       psindexdefn i
  WHERE   x.indexid BETWEEN '0' AND '9'
  AND     x.subindexid = i.indexid
  AND     x.subrecname = i.recname
  AND     (x.recname LIKE p_recname OR p_recname IS NULL) 
--    ORDER BY 1,2,3,4,5
  ) S
  ON (s.recname = u.recname AND s.indexid = u.indexid)
  WHEN MATCHED THEN UPDATE 
  SET u.subrecname = s.subrecname
  ,   u.subindexid = s.subindexid
  ,   u.platform_ora = s.platform_ora
  ,   u.custkeyorder = s.custkeyorder
  ,   u.uniqueflag = s.uniqueflag
  WHEN NOT MATCHED THEN INSERT
  (u.recname, u.indexid, u.subrecname, u.subindexid, u.platform_ora, u.custkeyorder, u.uniqueflag)
  VALUES
  (s.recname, s.indexid, s.subrecname, s.subindexid, s.platform_ora, s.custkeyorder, s.uniqueflag)
  ;

-- 25.5.2011 -- return organisation to Table if not possible to build IOT
  UPDATE  gfc_part_tables t
  SET     t.organization = 'T'
  ,       t.subpart_type = 'N'
  ,       t.subpart_column = ''
  WHERE   t.organization = 'I'
    AND NOT EXISTS(
      SELECT   'x'
      FROM  gfc_ps_indexdefn i
      WHERE  i.recname = t.recname
      AND  i.indexid = '_'
      AND  i.uniqueflag = 1)
  AND  (t.recname LIKE p_recname OR p_recname IS NULL) 
  AND  (t.part_id LIKE p_part_id OR p_part_id IS NULL)
  ;

  unset_action(p_action_name=>l_action);
END gfc_ps_indexdefn;

-------------------------------------------------------------------------------------------------------
-- populate table of indexes - dmk
-------------------------------------------------------------------------------------------------------
PROCEDURE gfc_ps_keydefn
(p_recname VARCHAR2 DEFAULT ''
) IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'GFC_PS_KEYDEFN');
  debug_msg('(recname='||p_recname||')',8);

  MERGE INTO gfc_ps_keydefn u USING ( -- 16.6.2010 rewritten as merge
  -- insert unique and user indexes
  SELECT recname, indexid, keyposn, fieldname, ascdesc
  FROM   gfc_ps_keydefn_vw
  WHERE  (recname LIKE p_recname OR p_recname IS NULL) -- 26.1.2011
  UNION
  -- insert first columns of alternate search key indexes
  SELECT c.recname, c.indexid, 1, c.fieldname, ascdesc
  FROM   gfc_ps_alt_ind_cols c
  WHERE  (c.recname LIKE p_recname OR p_recname IS NULL) 
  UNION
  -- insert other columns of alternate search key indexes
  SELECT c.recname, c.indexid, k.fieldposn+1, k.fieldname, k.ascdesc
  FROM   gfc_ps_alt_ind_cols c
  ,      gfc_ps_keydefn_vw k
  where  k.recname = c.recname
  AND    k.indexid = '_'
  AND    (c.recname LIKE p_recname OR p_recname IS NULL)
  ) S
  ON (s.recname = u.recname
  AND s.indexid = u.indexid
  AND s.keyposn = u.keyposn)
  WHEN MATCHED THEN UPDATE 
  SET u.fieldname = s.fieldname 
  , u.ascdesc = s.ascdesc
  WHEN NOT MATCHED THEN INSERT
  (u.recname, u.indexid, u.keyposn, u.fieldname, u.ascdesc)
  VALUES
  (s.recname, s.indexid, s.keyposn, s.fieldname, s.ascdesc)
  ;
  debug_msg('merged '||SQL%ROWCOUNT||' rows',8);

-- 30.6.2005-remove special processing columns
-- dELETE FROM gfc_ps_keydefn
-- wHERE fieldname IN('AFFILIATE_INTRA2','CHARTFIELD1','CHARTFIELD2','CHARTFIELD3')
--;

-- correct column sequencing
  UPDATE  gfc_ps_keydefn k
  SET     k.keyposn = (SELECT k1.keyposn
                       FROM   pskeydefn k1
                       WHERE  k1.recname = k.recname
                       AND    k1.indexid = k.indexid
                       AND    k1.fieldname = k.fieldname)
  WHERE (k.recname,k.indexid) IN (
                       SELECT j.recname, j.indexid
                       FROM   gfc_ps_indexdefn j  -- 6.9.2007 removed psindexdefn
                       WHERE  j.custkeyorder = 1
                       AND   (j.recname LIKE p_recname OR p_recname IS NULL) 
                       )
  ;
  debug_msg('update position on '||SQL%ROWCOUNT||' rows',8);


-- remove descending key option on partition and subpartition columns 23.1.2009
-- and corrected again for subrecords 3.6.2009
  UPDATE  gfc_ps_keydefn k
  SET   k.ascdesc = 1 -- is now an ascending column
  WHERE   k.ascdesc = 0 -- is a descending column
  AND EXISTS(
      SELECT 'x'
      FROM   psindexdefn i
      ,      psrecfielddb f
      ,      gfc_part_tables p
      WHERE  k.recname = f.recname
      AND    f.recname_parent = i.recname
      AND    i.indexid = k.indexid
      AND    i.uniqueflag = 1 -- it's a unique index
      AND    p.recname = k.recname
      AND    f.fieldname = k.fieldname
      AND    k.fieldname IN (p.part_column,p.subpart_column)
      AND    ROWNUM <= 1
      )
  ;
  debug_msg('update key direction '||SQL%ROWCOUNT||' rows',8);

  unset_action(p_action_name=>l_action);
END gfc_ps_keydefn;

-------------------------------------------------------------------------------------------------------
-- recursively expand and subrecords in the list of columns
-------------------------------------------------------------------------------------------------------
PROCEDURE expand_sbr IS
            CURSOR c_sbr_col IS
            SELECT *
            FROM   gfc_ps_tab_columns
            WHERE  fieldname IN
                  (SELECT recname    
                   FROM    psrecdefn
                   WHERE   rectype = 3)
            ORDER BY recname, fieldnum
            ;

            p_sbr_col c_sbr_col%ROWTYPE;

            l_found_sbr INTEGER :=0; -- number of subrecords found in loop
            l_sbr_cols  INTEGER;     -- number of columns in the subrecord
            l_last_recname VARCHAR2(18 CHAR) := ''; -- name oflast record processed
            l_fieldnum_adj INTEGER := 0; --field number offset when expanding subrecords
            l_module v$session.module%type;
            l_action v$session.action%type;
	BEGIN
            dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
            set_action(p_action_name=>'EXPAND_SBR');

            LOOP
                        l_found_sbr := 0;
                        OPEN c_sbr_col;
                        LOOP
                                FETCH c_sbr_col INTO p_sbr_col;
                                EXIT WHEN c_sbr_col%NOTFOUND;

                                debug_msg(l_last_recname||'/'||p_sbr_col.recname||'/'||l_fieldnum_adj,8);
                                IF (l_last_recname != p_sbr_col.recname OR l_last_recname IS NULL) THEN
                                        l_fieldnum_adj := 0;
                                        l_last_recname := p_sbr_col.recname;
                                END IF;

                                debug_msg(p_sbr_col.recname||'/'|| p_sbr_col.fieldnum||'/'|| 
                                      p_sbr_col.fieldname||'/'|| l_fieldnum_adj,8);

                                l_found_sbr := l_found_sbr +1;

                                SELECT COUNT(*)
                                INTO   l_sbr_cols
                                FROM   psrecfield f
                                WHERE  f.recname = p_sbr_col.fieldname;

                                DELETE FROM gfc_ps_tab_columns
                                WHERE  recname = p_sbr_col.recname
                                AND    fieldname = p_sbr_col.fieldname;

                                UPDATE gfc_ps_tab_columns
                                SET    fieldnum = fieldnum + l_sbr_cols - 1
                                WHERE  recname = p_sbr_col.recname
                                AND    fieldnum > p_sbr_col.fieldnum + l_fieldnum_adj;

                                INSERT INTO gfc_ps_tab_columns
                                (recname, fieldname, useedit, fieldnum, subrecname)
                                SELECT  p_sbr_col.recname, f.fieldname, f.useedit
				, 	f.fieldnum + p_sbr_col.fieldnum + l_fieldnum_adj - 1, f.recname
                                FROM    psrecfield f
                                WHERE   f.recname = p_sbr_col.fieldname;

                                l_fieldnum_adj := l_fieldnum_adj + l_sbr_cols -1;

                        END LOOP;
                        CLOSE c_sbr_col;
                        debug('Found: '||l_found_sbr||' sub-records');
                        EXIT WHEN l_found_sbr = 0;
                END LOOP;

		unset_action(p_action_name=>l_action);
        END expand_sbr;

-------------------------------------------------------------------------------------------------------
-- shuffle long columns to bottom of record
-------------------------------------------------------------------------------------------------------
PROCEDURE shuffle_long IS
                CURSOR c_cols IS
                SELECT  c.recname, c.fieldname, c.fieldnum
                FROM    gfc_ps_tab_columns c
                ,       psdbfield d
                ,       psrecdefn r
                WHERE   c.fieldname = d.fieldname
                AND     (       (       d.fieldtype = 1
                                AND     NOT d.length BETWEEN 1 AND 2000)
                        OR      d.fieldtype = 8)
                AND     r.recname = c.recname
                AND     r.rectype IN(k_build,7)
                AND EXISTS(
                        SELECT 'x'
                        FROM    gfc_ps_tab_columns c1
                        ,       psdbfield d1
                        WHERE   c1.recname = c.recname
                        AND     c1.fieldname = d1.fieldname
                        AND     c1.fieldnum > c.fieldnum
                        AND NOT (       (       d1.fieldtype = 1
                                        AND     NOT d1.length BETWEEN 1 AND 2000)
                                OR      d1.fieldtype = 8)
                        )
                ;
                p_cols c_cols%ROWTYPE;
                l_fieldcount INTEGER;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'SHUFFLE_LONG');

                OPEN c_cols;
                LOOP
                        FETCH c_cols INTO p_cols;
                        EXIT WHEN c_cols%NOTFOUND;

                        SELECT  MAX(fieldnum)
                        INTO    l_fieldcount
                        FROM    gfc_ps_tab_columns
                        WHERE   recname = p_cols.recname;

                        UPDATE  gfc_ps_tab_columns
                        SET     fieldnum = DECODE(fieldnum,p_cols.fieldnum,l_fieldcount,fieldnum-1)
                        WHERE   recname = p_cols.recname
                        AND     fieldnum >= p_cols.fieldnum
                        ;

                END LOOP;
                CLOSE c_cols;
		unset_action(p_action_name=>l_action);
        END;

-- generate the column definition clause for create table DDL
        FUNCTION col_def(p_recname VARCHAR2, p_fieldname VARCHAR2, p_longtoclob VARCHAR) RETURN VARCHAR2 IS
                l_col_def VARCHAR2(100 CHAR);

                CURSOR c_dbfield (p_fieldname VARCHAR2) IS
                SELECT  *
                FROM    psdbfield
                WHERE   fieldname = p_fieldname;
                p_dbfield c_dbfield%ROWTYPE;

                CURSOR c_gp_columns (p_recname VARCHAR2, p_fieldname VARCHAR2) IS
                SELECT  *
                FROM    gfc_ps_tab_columns
                WHERE   fieldname = p_fieldname
                AND     recname = p_recname;
                p_gp_columns c_gp_columns%ROWTYPE;

                l_nullable VARCHAR2(10 CHAR);
                l_datatype VARCHAR2(12 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'COL_DEF');

                OPEN c_dbfield(p_fieldname);
                OPEN c_gp_columns(p_recname, p_fieldname);
                FETCH c_dbfield INTO p_dbfield;
                FETCH c_gp_columns INTO p_gp_columns;

                IF p_dbfield.fieldtype = 0 THEN
                        l_datatype := 'VARCHAR2';
                ELSIF p_dbfield.fieldtype IN(1,8,9) THEN
                        IF p_dbfield.length BETWEEN 1 AND 2000 THEN
                                l_datatype := 'VARCHAR2';
                        ELSE
                                IF p_longtoclob = 'Y' THEN 
                                        l_datatype := 'CLOB';
                                ELSIF l_ptver >= '8.4' THEN
                                        l_datatype := 'LONG VARCHAR';
                                ELSE
                                        l_datatype := 'LONG';
                                END IF;
                        END IF;
                ELSIF p_dbfield.fieldtype IN(4,5,6) THEN
 			IF p_dbfield.fieldtype IN(5,6) AND l_use_timestamp AND l_ptver >= 8.50 AND l_oraver >= 10.0 THEN
                        	l_datatype := 'TIMESTAMP'; -- 15.4.2013 time and datetime columns created as timestamp from Apps 9.2 from PT8.50
			ELSE
                        	l_datatype := 'DATE';
			END IF;
                ELSIF p_dbfield.fieldtype = 2 THEN
                        IF p_dbfield.decimalpos > 0 THEN
                                l_datatype := 'DECIMAL';
                        ELSE
                                IF p_dbfield.length <= 4 THEN
                                        l_datatype := 'SMALLINT';
				ELSIF p_dbfield.length > 8 THEN -- 20.2.2008 pt8.47??
					l_datatype := 'DECIMAL';
					p_dbfield.length := p_dbfield.length + 1;
                                ELSE
                                        l_datatype := 'INTEGER';
                                END IF;
                        END IF;
                ELSIF p_dbfield.fieldtype = 3 THEN
                        l_datatype := 'DECIMAL';
                        IF p_dbfield.decimalpos = 0 THEN
                                p_dbfield.length := p_dbfield.length + 1;
                        END IF;
                END IF;

                l_col_def := l_datatype;
                IF l_datatype = 'VARCHAR2' THEN 
                        IF l_oraver >= 9.0 AND l_chardef = 'Y' THEN -- in Oracle 9 to provide for unicode VARCHAR2s defined in CHRs
                                l_col_def := l_col_def||'('||p_dbfield.length||' CHAR)';
                        ELSIF l_unicode_enabled = 1 THEN
                                l_col_def := l_col_def||'('||LEAST(4000,p_dbfield.length*3)||') CHECK(LENGTH('||p_fieldname||')<='||p_dbfield.length||')';
                        ELSE -- normal
                                l_col_def := l_col_def||'('||p_dbfield.length||')';
                        END IF;
                ELSIF l_datatype = 'DECIMAL' THEN
                        IF p_dbfield.fieldtype = 2 THEN
                                l_col_def := l_col_def||'('||(p_dbfield.length-1);
                        ELSE -- type 3
                                l_col_def := l_col_def||'('||(p_dbfield.length-2);
                        END IF;
                        IF p_dbfield.decimalpos > 0 THEN
                                l_col_def := l_col_def||','||p_dbfield.decimalpos;
                        END IF;
                        l_col_def := l_col_def||')';
                END IF;

                IF p_dbfield.fieldtype IN(k_build,2,3) OR mod(FLOOR(p_gp_columns.useedit/256),2) = 1 THEN
                        l_col_def := l_col_def||' NOT NULL';
                END IF;

                CLOSE c_dbfield;
                CLOSE c_gp_columns;

		unset_action(p_action_name=>l_action);
                RETURN l_col_def;
END col_def;

-------------------------------------------------------------------------------------------------------
-- insert line of script into table
-------------------------------------------------------------------------------------------------------
PROCEDURE ins_line(p_type NUMBER, p_line VARCHAR2) IS
--              PRAGMA AUTONOMOUS_TRANSACTION;
		l_module v$session.module%type;
		l_action v$session.action%type;

		l_line VARCHAR2(4000);
		l_char VARCHAR2(1);
		l_last_char VARCHAR2(1);
		l_length INTEGER;
		l_pos INTEGER;
		l_last_break INTEGER;
		l_inquote BOOLEAN := FALSE;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'INS_LINE',p_debug_level=>9);

		-- if string longer than threshold (k_max_line_length chars) calculate position of last not in quote space, and cut string there.  
 		--Keep cutting until string less than k_max_line_length chars and position > 0
		l_pos := 0;
		l_last_break := 0;
		l_line := RTRIM(p_line);
		l_length := LENGTH(l_line);
                debug_msg('type='||p_type||',l_length='||l_length,9);
		WHILE l_length >= k_max_line_length LOOP
			l_pos := l_pos + 1;
			l_last_char := l_char;
			l_char := SUBSTR(l_line,l_pos,1);
			IF NOT l_inquote AND INSTR(', ',l_last_char)>0 THEN
				l_last_break := l_pos-1;
				debug_msg('Break Point at position '||l_last_break,9);
			END IF;
			IF l_char = '''' THEN
				IF l_inquote THEN
					l_inquote := FALSE;
				ELSE
					l_inquote := TRUE;
				END IF;
			END IF;
                        debug_msg(l_char||'@'||l_pos||':l_last_break='||l_last_break||':l_inquote='||show_bool(l_inquote),9);
			IF l_pos >= l_length THEN
				EXIT;
			ELSIF l_pos >= k_max_line_length AND l_last_break > 0 THEN
		                l_lineno := l_lineno + 1;
		                EXECUTE IMMEDIATE 'INSERT INTO gfc_ddl_script (type, lineno, line) VALUES(:p_type, :l_lineno, :l_line)'
				USING p_type, l_lineno, SUBSTR(l_line,1,l_last_break);
				debug_msg('Break Line @ '||l_last_break||':'||TO_CHAR(p_type)||':'||TO_CHAR(l_lineno)||':'||l_length||' chars:'||SUBSTR(l_line,1,l_last_break)||'...',8);

				l_line := SUBSTR(l_line,l_last_break+1);
				l_length := LENGTH(l_line);
				l_pos := l_pos - l_last_break;
				l_last_break := 0;
			END IF;
		END LOOP;

                l_lineno := l_lineno + 1;
                EXECUTE IMMEDIATE 'INSERT INTO gfc_ddl_script VALUES(:p_type, :l_lineno, :l_line)'
		USING p_type, l_lineno, l_line;
		debug_msg(TO_CHAR(p_type)||':'||TO_CHAR(l_lineno)||':'||l_length||' chars:'||l_line,8);

--              COMMIT;
		unset_action(p_action_name=>l_action,p_debug_level=>9);
        END ins_line;

-------------------------------------------------------------------------------------------------------
-- insert line of script into table
-------------------------------------------------------------------------------------------------------
PROCEDURE debug_line(p_type NUMBER, p_line VARCHAR2) IS
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DEBUG_LINE');

                IF l_debug THEN
                        ins_line(p_type, p_line);
                END IF;

		unset_action(p_action_name=>l_action);
        END debug_line;

-------------------------------------------------------------------------------------------------------
-- insert pause into script
-------------------------------------------------------------------------------------------------------
PROCEDURE pause_sql(p_type NUMBER) IS
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'PAUSE_SQL');

                IF l_pause = 'Y' THEN
                        ins_line(p_type,'pause');
                        ins_line(p_type,'');
                END IF;

		unset_action(p_action_name=>l_action);
        END pause_sql;

-------------------------------------------------------------------------------------------------------
-- sql whenever error control
-------------------------------------------------------------------------------------------------------
PROCEDURE whenever_sqlerror(p_type NUMBER, p_error BOOLEAN) IS
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'WHENEVER_SQLERROR');

                IF p_error THEN
                        ins_line(p_type,'WHENEVER SQLERROR EXIT FAILURE');
                ELSE
                        ins_line(p_type,'WHENEVER SQLERROR CONTINUE');
                END IF;
                ins_line(p_type,'');

		unset_action(p_action_name=>l_action);
        END whenever_sqlerror;

-------------------------------------------------------------------------------------------------------
-- print generation date into build script
-------------------------------------------------------------------------------------------------------
PROCEDURE signature(p_type    NUMBER
                           ,p_error BOOLEAN
                           ,p_spool   VARCHAR2 DEFAULT NULL
                           ,p_recname VARCHAR2 DEFAULT NULL) IS
       	l_module v$session.module%type;
            l_action v$session.action%type;
        BEGIN
            dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
            set_action(p_action_name=>'SIGNATURE');

            ins_line(p_type,'set echo on pause off verify on feedback on timi on autotrace off pause off lines '
                            ||k_max_line_length||' sqlblanklines on serveroutput on');
            IF p_recname IS NULL THEN
                ins_line(p_type,LOWER('spool '||p_spool||'_'||l_dbname||'.lst'));
            ELSE
                ins_line(p_type,LOWER('spool '||p_spool||'_'||l_dbname||'_'||p_recname||'.lst'));
            END IF;

            ins_line(p_type,'REM Generated by GFC_PSPART - (c)Go-Faster Consultancy - www.go-faster.co.uk 2001-2022');
            ins_line(p_type,'REM '||l_dbname||' @ '||TO_CHAR(sysdate,'HH24:MI:SS DD.MM.YYYY'));
            whenever_sqlerror(p_type,FALSE);
            ins_line(p_type,'EXECUTE dbms_application_info.set_module(module_name=>'''||UPPER(p_spool)||''',action_name=>'''||UPPER(p_recname)||''');');
            IF p_error THEN
                whenever_sqlerror(p_type,p_error);
            END IF;
            ins_line(p_type,'');

            unset_action(p_action_name=>l_action);
        END signature;

-------------------------------------------------------------------------------------------------------
-- print ALTER SESSION PARALLEL DDL into script
-------------------------------------------------------------------------------------------------------
PROCEDURE forceddldop(p_type NUMBER) IS
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
    set_action(p_action_name=>'FORCEDDLDOP');

    IF l_force_para_dop IS NOT NULL THEN
      ins_line(p_type,'ALTER SESSION FORCE PARALLEL DDL PARALLEL '||l_force_para_dop||';');
      ins_line(p_type,'ALTER SESSION FORCE PARALLEL DML PARALLEL '||l_force_para_dop||';');
    END IF;
    ins_line(p_type,'');

  unset_action(p_action_name=>l_action);
END forceddldop;
-------------------------------------------------------------------------------------------------------
-- print generation date into build script
-------------------------------------------------------------------------------------------------------
PROCEDURE signoff(p_type NUMBER) IS
        BEGIN
            whenever_sqlerror(p_type,FALSE);
            ins_line(p_type,'EXECUTE dbms_application_info.set_module(module_name=>''SQL*Plus'',action_name=>'''');');
            ins_line(p_type,'spool off');
        END signoff;

-------------------------------------------------------------------------------------------------------
-- create database roles
-------------------------------------------------------------------------------------------------------
PROCEDURE create_roles IS
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'CREATE_ROLES');

                IF l_roles = 'Y' THEN
                        -- ins_line(k_build,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
                        -- ins_line(k_build,LOWER('spool '||LOWER(l_scriptid)||'_'||l_dbname||'.lst'));
                        signature(k_build,FALSE,l_scriptid);
                        ins_line(k_build,'');
                        IF l_update_all IS NOT NULL THEN
                          ins_line(k_build,'CREATE ROLE '||LOWER(l_update_all));
                          ins_line(k_build,'/');
                        END IF;
                        IF l_read_all IS NOT NULL THEN
                          ins_line(k_build,'CREATE ROLE '||LOWER(l_read_all));
                          ins_line(k_build,'/');
                        END IF;
                        ins_line(k_build,'');
                        ins_line(k_build,'spool off');
                END IF;

		unset_action(p_action_name=>l_action);
        END create_roles;
-------------------------------------------------------------------------------------------------------
-- generate commands to rename table partititons
-------------------------------------------------------------------------------------------------------
PROCEDURE rename_parts
(p_type INTEGER DEFAULT 0
,p_table_name  VARCHAR2
,p_drop_index   VARCHAR2
) IS
                CURSOR c_tab_parts(p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'ALTER TABLE '||LOWER(l_schema2||utp.table_name)||' RENAME PARTITION '
                                 ||LOWER(utp.partition_name)||' TO old_'||LOWER(utp.partition_name)||';' rename_cmd
                FROM    user_tab_partitions utp
                WHERE   utp.table_name = p_table_name
		AND     SUBSTR(utp.partition_name,1,4) != 'OLD_'
		ORDER BY 1
                ;

                CURSOR c_idx_parts (p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'ALTER INDEX '||LOWER(l_schema2||uip.index_name)||' RENAME PARTITION '
                                 ||LOWER(uip.partition_name)||' TO old_'||LOWER(uip.partition_name)||';' rename_cmd
                FROM    user_ind_partitions uip
                ,       user_part_indexes upi
                WHERE   uip.index_name = upi.index_name
                AND     upi.table_name = p_table_name
		AND     SUBSTR(uip.partition_name,1,4) != 'OLD_'
		ORDER BY 1
                ;

-- drop index rather than rebuilding and renaming it somewhere else
-----------------------------------------------------------------------------------------------------
-- 28.8.2013 - previously only dropped partitioned indexes, but we have global non-partitioned indexes
--              FROM    user_ind_partitions uip, user_part_indexes upi
--              WHERE   uip.index_name = upi.index_name
--              AND     upi.table_name = p_table_name
-----------------------------------------------------------------------------------------------------
                CURSOR c_drop_idx (p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'DROP INDEX '||LOWER(ui.index_name)||';'  drop_cmd
                FROM    user_indexes ui
                WHERE   ui.table_name = p_table_name
		ORDER BY 1
                ;

                p_tab_parts c_tab_parts%ROWTYPE;
                p_idx_parts c_idx_parts%ROWTYPE;
                p_drop_idx  c_drop_idx%ROWTYPE;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'RENAME_PARTS');

		whenever_sqlerror(p_type,FALSE);
                IF l_rename_parts = 'Y' THEN --20.05.2014
                        OPEN c_tab_parts(p_table_name);
                        LOOP
                                FETCH c_tab_parts INTO p_tab_parts;
                                EXIT WHEN c_tab_parts%NOTFOUND;

                                ins_line(p_type,p_tab_parts.rename_cmd);
                        END LOOP;
                        CLOSE c_tab_parts;
                        ins_line(p_type,'');
                END IF;

                IF p_drop_index = 'Y' THEN
                        OPEN c_drop_idx(p_table_name);
                        LOOP
                                FETCH c_drop_idx INTO p_drop_idx;
                                EXIT WHEN c_drop_idx%NOTFOUND;

                                ins_line(p_type,p_drop_idx.drop_cmd);
                        END LOOP;
                        CLOSE c_drop_idx;
                ELSIF l_rename_parts = 'Y' THEN --20.05.2014
                        OPEN c_idx_parts(p_table_name);
                        LOOP
                                FETCH c_idx_parts INTO p_idx_parts;
                                EXIT WHEN c_idx_parts%NOTFOUND;

                                ins_line(p_type,p_idx_parts.rename_cmd);
                        END LOOP;
                        CLOSE c_idx_parts;
                END IF;
		whenever_sqlerror(k_build,TRUE);
                ins_line(p_type,'');

		unset_action(p_action_name=>l_action);
        END rename_parts;

-------------------------------------------------------------------------------------------------------
-- generate commands to rename table partititons
-------------------------------------------------------------------------------------------------------
PROCEDURE rename_subparts
(p_type INTEGER DEFAULT 0
,p_table_name   VARCHAR2
,p_part_name    VARCHAR2 DEFAULT ''
,p_drop_index   VARCHAR2 DEFAULT 'N'
) IS
                CURSOR c_tab_subparts (p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'ALTER TABLE '||LOWER(l_schema2||utp.table_name)||' RENAME SUBPARTITION '
                                 ||LOWER(utp.subpartition_name)||' TO old_'||LOWER(utp.subpartition_name)||';' rename_cmd
                FROM    user_tab_subpartitions utp
                WHERE   utp.table_name = p_table_name
                AND    (utp.partition_name LIKE p_part_name OR p_part_name IS NULL)
		AND     SUBSTR(utp.subpartition_name,1,4) != 'OLD_'
                ORDER BY 1
                ;

                CURSOR c_idx_subparts (p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'ALTER INDEX '||LOWER(l_schema2||uip.index_name)||' RENAME SUBPARTITION '
                        ||LOWER(uip.subpartition_name)||' TO old_'||LOWER(uip.subpartition_name)||';' rename_cmd
                FROM    user_ind_subpartitions uip
                ,       user_part_indexes upi
                WHERE   upi.table_name = p_table_name
                AND     uip.index_name = upi.index_name
                AND    (uip.partition_name LIKE p_part_name OR p_part_name IS NULL)
		AND     SUBSTR(uip.subpartition_name,1,4) != 'OLD_'
                ORDER BY 1
                ;

-- drop index rather than rebuilding and renaming it somewhere else
-----------------------------------------------------------------------------------------------------
-- 28.8.2013 - previously only dropped partitioned indexes, but we have global non-partitioned indexes
--              FROM    user_ind_subpartitions uip, user_part_indexes upi
--              WHERE   uip.index_name = upi.index_name
--              AND     upi.table_name = p_table_name
--              ORDER BY 1
-----------------------------------------------------------------------------------------------------
                CURSOR c_drop_idx (p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'DROP INDEX '||LOWER(ui.index_name)||';'  drop_cmd
                FROM    user_indexes ui
                WHERE   ui.table_name = p_table_name
                ORDER BY 1
                ;

                p_tab_subparts c_tab_subparts%ROWTYPE;
                p_idx_subparts c_idx_subparts%ROWTYPE;
                p_drop_idx     c_drop_idx%ROWTYPE;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'RENAME_SUBPARTS');
		debug_msg('RENAME_SUBPARTS:type='||p_type
					||'/table='||p_table_name
					||'/part_name='||p_part_name
					||'/drop_index='||p_drop_index,6);

		whenever_sqlerror(p_type,FALSE);
                IF l_rename_parts = 'Y' THEN --20.05.2014
                        OPEN c_tab_subparts(p_table_name);
                        LOOP
                                FETCH c_tab_subparts INTO p_tab_subparts;
                                EXIT WHEN c_tab_subparts%NOTFOUND;

                                ins_line(p_type,p_tab_subparts.rename_cmd);
                        END LOOP;
                        CLOSE c_tab_subparts;
                END IF;

                IF p_drop_index = 'Y' THEN
                        ins_line(p_type,'');
                        OPEN c_drop_idx(p_table_name);
                        LOOP
                                FETCH c_drop_idx INTO p_drop_idx;
                                EXIT WHEN c_drop_idx%NOTFOUND;

                                ins_line(p_type,p_drop_idx.drop_cmd);
                        END LOOP;
                        CLOSE c_drop_idx;
                        ins_line(p_type,'');
                ELSIF l_rename_parts = 'Y' THEN --20.05.2014
                        ins_line(p_type,'');
                        OPEN c_idx_subparts(p_table_name);
                        LOOP
                                FETCH c_idx_subparts INTO p_idx_subparts;
                                EXIT WHEN c_idx_subparts%NOTFOUND;

                                ins_line(p_type,p_idx_subparts.rename_cmd);
                        END LOOP;
                        CLOSE c_idx_subparts;
                        ins_line(p_type,'');
                END IF;
		whenever_sqlerror(p_type,TRUE);

		unset_action(p_action_name=>l_action);
        END rename_subparts;

-------------------------------------------------------------------------------------------------------
-- list indexed columns
-------------------------------------------------------------------------------------------------------
PROCEDURE ind_cols(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2, p_desc_index VARCHAR2) IS
                CURSOR c_ind_cols(p_recname VARCHAR2, p_indexid VARCHAR2) IS
                SELECT  NVL(LOWER(c.fieldname),k.fieldname) fieldname -- 6.9.2007 only lower case of columns
-- indexes not built descending in PT8.15
--              ||      DECODE(k.ascdesc,0,' DESC')
                ,       k.ascdesc
		,	t.part_column, t.subpart_column
                FROM    gfc_ps_keydefn k
			LEFT OUTER JOIN gfc_ps_tab_columns c -- 6.9.2007 to determine fields not expressions
			ON c.recname = k.recname
			AND c.fieldname = k.fieldname
			LEFT OUTER JOIN gfc_part_tables t
			ON t.recname = k.recname
                WHERE   k.recname = p_recname
                AND     k.indexid = p_indexid
                ORDER BY k.keyposn
                ;

                p_ind_cols c_ind_cols%ROWTYPE;
                l_col_def VARCHAR2(100 CHAR);
                l_counter INTEGER := 0;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'IND_COLS');
		debug_msg('(type='||p_type||', recname='||p_recname||', indexid='||p_indexid||')');

                OPEN c_ind_cols(p_recname, p_indexid);
                LOOP
                        FETCH c_ind_cols INTO p_ind_cols;
                        EXIT WHEN c_ind_cols%NOTFOUND;
			debug_msg('index column='||p_ind_cols.fieldname);
                        l_counter := l_counter + 1;
                        IF l_counter > 1 THEN
                                l_col_def := ',';
                        ELSE
                                l_col_def := '(';
                        END IF;
                        l_col_def := l_col_def||p_ind_cols.fieldname;

                        IF (p_desc_index = 'Y' OR p_desc_index IS NULL) AND p_ind_cols.ascdesc = 0 THEN
                                l_col_def := l_col_def || ' DESC';
                        END IF;
                        ins_line(p_type,l_col_def);
                END LOOP;
                ins_line(p_type,')');
                CLOSE c_ind_cols;

		unset_action(p_action_name=>l_action);
        END ind_cols;

-------------------------------------------------------------------------------------------------------
-- list columns in table for column list in create table DDL
-------------------------------------------------------------------------------------------------------
PROCEDURE tab_cols
(p_type         INTEGER
,p_recname      VARCHAR2
,p_longtoclob   VARCHAR2
,p_organisation VARCHAR2 DEFAULT 'T'
,p_prefix       VARCHAR2 DEFAULT ''
,p_table_name   VARCHAR2 DEFAULT ''
) IS
  CURSOR   c_tab_cols(p_recname VARCHAR2) IS
  SELECT   c.*
  FROM     gfc_ps_tab_columns c
    LEFT OUTER JOIN gfc_ora_tab_Columns o
    ON   o.table_name = p_table_name
    AND  o.column_name = c.fieldname
  WHERE    c.recname = p_recname
  ORDER BY c.recname, o.column_id, c.fieldnum
  ;

  p_tab_cols c_tab_cols%ROWTYPE;
  l_col_def VARCHAR2(100 CHAR);
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'TAB_COLS');

  OPEN c_tab_cols(p_recname);
  LOOP
    FETCH c_tab_cols INTO p_tab_cols;
    EXIT WHEN c_tab_cols%NOTFOUND;

    IF p_tab_cols.fieldnum = 1 THEN
      l_col_def := '(';
    ELSE
      l_col_def := ',';
    END IF;
    l_col_def := l_col_def||LOWER(p_tab_cols.fieldname)||' '
                          ||col_def(p_recname,p_tab_cols.fieldname,p_longtoclob);
    ins_line(p_type,l_col_def);
  END LOOP;

  IF p_organisation = 'I' THEN -- 25.5.2011 add pk to col list if iot
   ins_line(p_type,',CONSTRAINT '||LOWER(p_prefix||p_recname)||' PRIMARY KEY ');
   ind_cols(p_type,p_recname,'_','N');
  END IF;

  ins_line(p_type,')');
  CLOSE c_tab_cols;

  unset_action(p_action_name=>l_action);
END tab_cols;

-------------------------------------------------------------------------------------------------------
-- list columns in table for column list in create table DDL
-------------------------------------------------------------------------------------------------------
PROCEDURE tab_col_list(p_type        INTEGER
                              ,p_recname     VARCHAR2
	                      ,p_table_name  VARCHAR2 DEFAULT NULL
	                      ,p_column_name BOOLEAN DEFAULT TRUE) IS
                -- cursor lists columns in record, returns column name on specified table (or matching record if not specified) 
                CURSOR c_tab_cols(p_recname VARCHAR2) IS
                SELECT  g.*, c.column_name
                FROM    (
                        SELECT g.*, r.sqltablename, d.fieldtype
                        FROM   gfc_ps_tab_columns g
                        ,      psrecdefn r
                        ,      psdbfield d
			WHERE  r.recname = g.recname
			AND    d.fieldname = g.fieldname
			) g
			LEFT OUTER JOIN gfc_ora_tab_columns c
				ON c.column_name = g.fieldname
				AND c.table_name = NVL(UPPER(p_table_name),DECODE(g.sqltablename,' ','PS_'||g.recname,g.sqltablename))
                        WHERE   g.recname = p_recname
                        ORDER BY g.recname, g.fieldnum
                        ;
                p_tab_cols c_tab_cols%ROWTYPE;
                l_col_def VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'TAB_COL_LIST');

		debug_msg('TAB_COL_LIST:type='||p_type
                                     ||'/recname='||p_recname
                                     ||'/table_name='||p_table_name
                                     ||'/column_name='||show_bool(p_column_name)
                                     ,6);

                OPEN c_tab_cols(p_recname);
                LOOP
                        FETCH c_tab_cols INTO p_tab_cols;
                        EXIT WHEN c_tab_cols%NOTFOUND;
	
			debug_msg('fieldnum='||p_tab_cols.fieldnum
                                     ||'/fieldname='||p_tab_cols.fieldname
                                     ||'/column_name='||p_tab_cols.column_name
                                     ,7);

                        IF p_tab_cols.fieldnum=1 THEN
                                l_col_def := '';
 			ELSE
                                l_col_def := l_col_def||',';
                        END IF;
			
			IF p_column_name OR p_tab_cols.column_name IS NOT NULL THEN -- 22.1.2007 - to handle new columns not in table
	                        l_col_def := l_col_def||LOWER(p_tab_cols.fieldname);
			ELSIF p_tab_cols.subrecname = 'PSARCHIVE_SBR' THEN -- special processing for PS archive tables
				IF p_tab_cols.fieldtype IN(0) THEN -- character
					l_col_def := l_col_def||'''&'||'&'||p_tab_cols.fieldname||'''';
				ELSE
					l_col_def := l_col_def||'&'||'&'||p_tab_cols.fieldname;
				END IF;
			ELSIF p_tab_cols.fieldtype = 0 THEN -- character
				l_col_def := l_col_def||''' ''';
			ELSIF p_tab_cols.fieldtype IN(2,3) THEN -- numeric
				l_col_def := l_col_def||'0';
			ELSIF p_tab_cols.fieldtype IN(4,5,6) THEN -- date
				IF mod(FLOOR(p_tab_cols.useedit/256),2) = 1 THEN -- required
					l_col_def := l_col_def||'SYSDATE';
				ELSE
					l_col_def := l_col_def||'NULL';
				END IF;
			ELSIF p_tab_cols.fieldtype IN(1,8,9) THEN -- long
				IF mod(FLOOR(p_tab_cols.useedit/256),2) = 1 THEN -- required
					l_col_def := l_col_def||''' ''';
				ELSE
					l_col_def := l_col_def||'NULL';
				END IF;
			END IF; -- 22.1.2007 - end of new column handling 

                        IF LENGTH(l_col_def) >= 950 THEN
                                ins_line(p_type,l_col_def);
				debug_msg('col_def='||l_col_def,7);
                                l_col_def := '';
                        END IF;
                END LOOP;
                IF LENGTH(l_col_def) > 0 THEN
                        ins_line(p_type,l_col_def);
			debug_msg('col_def='||l_col_def,7);
                END IF;
                CLOSE c_tab_cols;
		unset_action(p_action_name=>l_action);
        END;

-- 6.9.2007 substituate table storage variables in same way as Peoplesoft
	FUNCTION tab_storage(p_recname VARCHAR2, p_storage VARCHAR2) RETURN VARCHAR2 IS
		l_storage VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'TAB_STORAGE');
 
                debug_msg('TAB_STORAGE:recname='||p_recname
                                   ||'/storage='||p_storage
                                   ,6);

		l_storage := p_storage;

		FOR c_tab_storage IN
		(SELECT	'**'||d.parmname||'**' parmname
		, 	DECODE(NVL(r.parmvalue,' '),' ',d.parmvalue,r.parmvalue) parmvalue
		FROM	psddldefparms d
		,	psrecddlparm r
		WHERE	d.statement_type = 1 -- create table
		AND	d.platformid = 2 --oracle
		AND	d.sizing_set = 0 --just because
		AND	r.recname(+) = p_recname
		AND	r.platformid(+) = d.platformid
		AND	r.sizingset(+) = d.sizing_set -- yes, sizingset without an underscore - psft got it wrong
		AND	r.parmname(+) = d.parmname
                AND     l_storage LIKE '%**'||d.parmname||'**%'
                ) LOOP
			l_storage := replace(l_storage,c_tab_storage.parmname,c_tab_storage.parmvalue);
		END LOOP;

		unset_action(p_action_name=>l_action);
		RETURN l_storage;
	END tab_storage;

-- 6.9.2007 new:substituate index storage variables in same way as Peoplesoft
	FUNCTION idx_storage(p_recname VARCHAR2, p_indexid VARCHAR2, p_storage VARCHAR2, 
                             p_subpartitions NUMBER DEFAULT 0) RETURN VARCHAR2 IS
		l_storage VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'IDX_STORAGE');

                debug_msg('IDX_STORAGE:recname='||p_recname
                                   ||'/indexid='||p_indexid
                                   ||'/storage='||p_storage
                                   ,6);
		l_storage := p_storage;

		FOR c_idx_storage IN
		(SELECT	'**'||d.parmname||'**' parmname
		,	CASE WHEN i.parmvalue IS NOT NULL AND i.parmvalue != ' ' THEN i.parmvalue
			     WHEN g.parmvalue IS NOT NULL THEN g.parmvalue
			     ELSE d.parmvalue
                        END as parmvalue
		FROM	psddldefparms d
			LEFT OUTER JOIN	psidxddlparm i
				ON	i.recname = p_recname
				AND	i.indexid = p_indexid
				AND	i.platformid = d.platformid
				AND	i.sizingset = d.sizing_set -- yes, sizingset without an underscore - psft got it wrong
				AND	i.parmname = d.parmname
			LEFT OUTER JOIN	gfc_ps_idxddlparm g
				ON	g.recname = p_recname
				AND	g.indexid = p_indexid
				AND	g.parmname = d.parmname
		WHERE	d.statement_type = 2 -- create index
		AND	d.platformid = 2 --oracle
		AND	d.sizing_set = 0 --just because
                AND     l_storage LIKE '%**'||d.parmname||'**%'
                ) LOOP
			l_storage := replace(l_storage,c_idx_storage.parmname,c_idx_storage.parmvalue);
			if p_subpartitions > 1 AND l_oraver < 10 THEN -- cannot compress composite partitions
				l_storage := replace(l_storage,' COMPRESS',' -- cOMPRESS');
			END IF;
		END LOOP;

		unset_action(p_action_name=>l_action);
		RETURN l_storage;
	END idx_storage;

-------------------------------------------------------------------------------------------------------
-- generate range subpartition clause on the basis of part ranges table
-------------------------------------------------------------------------------------------------------
PROCEDURE tab_rangesubparts(p_type NUMBER, p_recname VARCHAR2
                               ,p_part_id VARCHAR2, p_part_name VARCHAR2
                               ,p_part_basename VARCHAR2
                               ,p_arch_flag VARCHAR2 DEFAULT 'N') IS -- 19.3.2010 added subpart
            l_part_def CLOB;
            l_counter INTEGER := 0;
            l_module v$session.module%type;
            l_action v$session.action%type;
        BEGIN
            dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
            set_action(p_action_name=>'TAB_RANGESUBPARTS');

            debug_msg('TAB_RANGESUBPARTS:recname='||p_recname
                                 ||'/part_id='||p_part_id
                                 ||'/part_name='||p_part_name
                                 ||'/part_basename='||p_part_basename
                                 ||'/arch_flag='||p_arch_flag
                                 ,6);

            -- 12.2.2008 restrict combinations of partitions
            FOR t IN(
                SELECT a.*
                FROM   gfc_part_ranges a, gfc_part_subparts b
                WHERE  a.part_id = p_part_id
                AND    b.part_id = p_part_id
                AND    b.part_name = p_part_name
                AND    b.subpart_name = a.part_name
                AND    b.build = 'Y' -- if subpartition to be built in range partition
                AND    (a.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
                ORDER BY a.part_no
            ) LOOP
                IF l_counter = 0 THEN
                    l_part_def := '(';
                ELSE
                    l_part_def := ',';
                END IF;
                l_part_def := l_part_def||'SUBPARTITION '||LOWER(p_part_basename
                                        ||'_'||p_part_name||'_'||t.part_name);
                ins_line(p_type,l_part_def);

		l_part_def := ' VALUES LESS THAN ('||t.part_value||')';
		IF LENGTH(l_part_def) > k_max_line_length_margin THEN --26.11.2020 added partial index support
					ins_line(p_type,l_part_def);
					l_part_def := '';
				END IF;
				IF t.partial_index = 'Y' THEN
					l_part_def := l_part_def||' INDEXING ON';
				ELSIF t.partial_index = 'N' THEN
					l_part_def := l_part_def||' INDEXING OFF';
				END IF;
                ins_line(p_type,l_part_def);
                l_part_def := '';

                IF t.tab_tablespace IS NOT NULL THEN
                    l_part_def := ' TABLESPACE '||t.tab_tablespace;
                END IF;

-- 3.7.2012 remove physical storage parameters from subpartition definition including Oracle 11.2
--              IF t.tab_storage IS NOT NULL AND l_oraver >= 11.2 THEN
--                  l_part_def := l_part_def||' '||tab_storage(p_recname, t.tab_storage); -- 6.9.2007
--              END IF;

                IF l_part_def IS NOT NULL THEN
                    debug(l_part_def);
                    ins_line(p_type,l_part_def);
                END IF;
                l_counter := l_counter + 1;
            END LOOP;
            IF l_counter > 0 THEN
                    ins_line(p_type,')');
            END IF;

            unset_action(p_action_name=>l_action);
         END tab_rangesubparts;

-------------------------------------------------------------------------------------------------------
-- generate list subpartition clause on the basis of part ranges table
-------------------------------------------------------------------------------------------------------
PROCEDURE tab_listsubparts(p_type NUMBER, p_recname VARCHAR2
                               ,p_part_id VARCHAR2, p_part_name VARCHAR2
                               ,p_part_basename VARCHAR2
                               ,p_arch_flag VARCHAR2 DEFAULT 'N') IS -- 19.3.2010 added subpart
            l_part_def CLOB;
            l_counter INTEGER := 0;
            l_module v$session.module%type;
            l_action v$session.action%type;
        BEGIN
            dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
            set_action(p_action_name=>'TAB_LISTSUBPARTS');

            debug_msg('TAB_LISTSUBPARTS:recname='||p_recname
                                 ||'/part_id='||p_part_id
                                 ||'/part_name='||p_part_name
                                 ||'/part_basename='||p_part_basename
                                 ||'/arch_flag='||p_arch_flag
                                 ,6);

            -- 12.2.2008 restrict combinations of partitions
            FOR t IN(
                SELECT a.*
                FROM   gfc_part_lists a, gfc_part_subparts b
                WHERE  a.part_id = p_part_id
                AND    b.part_id = p_part_id
                AND    b.part_name = p_part_name
                AND    b.subpart_name = a.part_name
                AND    b.build = 'Y' -- if subpartition to be built in range partition
                AND    (a.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
                ORDER BY a.part_no --20.10.2014 corrected order by
            ) LOOP
                IF l_counter = 0 THEN
                    l_part_def := '(';
                ELSE
                    l_part_def := ',';
                END IF;
                l_part_def := l_part_def||'SUBPARTITION '||LOWER(p_part_basename||'_'||p_part_name||'_'||t.part_name);
                ins_line(p_type,l_part_def);

                l_part_def := ' VALUES ('||t.list_value||')';
		IF LENGTH(l_part_def) > k_max_line_length_margin THEN --26.11.2020 added partial index support
			ins_line(p_type,l_part_def);
					l_part_def := '';
				END IF;
				IF t.partial_index = 'Y' THEN
					l_part_def := l_part_def||' INDEXING ON';
				ELSIF t.partial_index = 'N' THEN
					l_part_def := l_part_def||' INDEXING OFF';
				END IF;

                IF l_part_def IS NOT NULL THEN
                        ins_line(p_type,l_part_def);
                        l_part_def := '';    
                END IF;

                IF t.tab_tablespace IS NOT NULL THEN
                    l_part_def := ' TABLESPACE '||t.tab_tablespace;
                END IF;
-- 3.7.2012 remove physical storage parameters from subpartition definition including Oracle 11.2
--              IF t.tab_storage IS NOT NULL AND l_oraver >= 11.2 THEN
--                  l_part_def := l_part_def||' '||tab_storage(p_recname, t.tab_storage); -- 6.9.2007
--              END IF;
                IF l_part_def IS NOT NULL THEN
                    debug(l_part_def);
                    ins_line(p_type,l_part_def);
                END IF;
                l_counter := l_counter + 1;
            END LOOP;
            IF l_counter > 0 THEN
                    ins_line(p_type,')');
            END IF;

            unset_action(p_action_name=>l_action);
         END tab_listsubparts;

-------------------------------------------------------------------------------------------------------
-- generate subpartition clause on the basis of part list table
-------------------------------------------------------------------------------------------------------
PROCEDURE idx_listsubparts(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2, 
                                p_part_id VARCHAR2, p_part_name VARCHAR2) IS
            l_part_def CLOB;
            l_counter INTEGER := 0;
            l_module v$session.module%type;
            l_action v$session.action%type;
        BEGIN
            dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
            set_action(p_action_name=>'IDX_LISTSUBPARTS');

            debug_msg('idx_listsubparts:'||p_recname||'/'||p_indexid||'/'||p_part_id,6);

            -- 12.2.2008 restrict combinations of partitions
            FOR t IN(
                        SELECT  a.*
                        FROM    gfc_part_lists a, gfc_part_subparts b
                        WHERE   a.part_id = p_part_id
                        AND     b.part_id = p_part_id
                        AND     b.part_name = p_part_name
                        AND     b.subpart_name = a.part_name
                        AND     b.build = 'Y' -- if subpartition to be built in range partition
                        ORDER BY a.part_no
            ) LOOP
                IF l_counter = 0 THEN
                    l_part_def := '(';
                ELSE
                    l_part_def := ',';
                END IF;
                l_part_def := l_part_def||'SUBPARTITION '
                                        ||LOWER(p_recname||p_indexid||p_part_name||'_'||t.part_name);
                ins_line(p_type,l_part_def);

                l_part_def := ' VALUES ('||t.list_value||')';
                IF t.tab_tablespace IS NOT NULL THEN
                    l_part_def := l_part_def||' TABLESPACE '||t.idx_tablespace;
                END IF;
-- 3.7.2012 remove physical storage option from subpartition create clause including Oracle 11.2
--              IF t.idx_storage IS NOT NULL AND l_oraver >= 11.2 THEN
--                  l_part_def := l_part_def||' '||
--                                idx_storage(p_recname, p_indexid, t.idx_storage);
--              END IF;
                IF l_part_def IS NOT NULL THEN
                    debug(l_part_def);
                    ins_line(p_type,l_part_def);
                END IF;
                l_counter := l_counter + 1;
            END LOOP;
            IF l_counter > 0 THEN
                ins_line(p_type,')');
            END IF;

            unset_action(p_action_name=>l_action);
        END idx_listsubparts;

-------------------------------------------------------------------------------------------------------
-- generate partition clause on the basis of part ranges table
-------------------------------------------------------------------------------------------------------
PROCEDURE idx_rangesubparts(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2, 
                                p_part_id VARCHAR2, p_part_name VARCHAR2) IS
            l_part_def CLOB;
            l_counter INTEGER := 0;
            l_module v$session.module%type;
            l_action v$session.action%type;
        BEGIN
            dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
            set_action(p_action_name=>'IDX_RANGESUBPARTS');

            debug_msg('idx_rangesubparts:'||p_recname||'/'||p_indexid||'/'||p_part_id,6);

            -- 12.2.2008 restrict combinations of partitions
            FOR t IN(
                        SELECT  a.*
                        FROM    gfc_part_ranges a, gfc_part_subparts b
                        WHERE   a.part_id = p_part_id
                        AND     b.part_id = p_part_id
                        AND     b.part_name = p_part_name
                        AND     b.subpart_name = a.part_name
                        AND     b.build = 'Y' -- if subpartition to be built in range partition
                        ORDER BY a.part_no
            ) LOOP
                IF l_counter = 0 THEN
                    l_part_def := '(';
                ELSE
                    l_part_def := ',';
                END IF;
                l_part_def := l_part_def||'SUBPARTITION '
                                        ||LOWER(p_recname||p_indexid||p_part_name||'_'||t.part_name);
                ins_line(p_type,l_part_def);

                l_part_def := ' VALUES LESS THAN ('||t.part_value||')';
                IF t.tab_tablespace IS NOT NULL THEN
                    l_part_def := l_part_def||' TABLESPACE '||t.idx_tablespace;
                END IF;
-- 3.7.2012 remove physical storage option from subpartition create clause including Oracle 11.2
--              IF t.idx_storage IS NOT NULL AND l_oraver >= 11.2 THEN
--                  l_part_def := l_part_def||' '||
--                                idx_storage(p_recname, p_indexid, t.idx_storage);
--              END IF;
                IF l_part_def IS NOT NULL THEN
                    debug(l_part_def);
                    ins_line(p_type,l_part_def);
                END IF;
                l_counter := l_counter + 1;
            END LOOP;
            IF l_counter > 0 THEN
                ins_line(p_type,')');
            END IF;

            unset_action(p_action_name=>l_action);
        END idx_rangesubparts;

-------------------------------------------------------------------------------------------------------
-- generate partition clause on the basis of part ranges table
-------------------------------------------------------------------------------------------------------
PROCEDURE tab_part_ranges(p_type NUMBER, p_recname VARCHAR2, p_part_id VARCHAR2, 
		                  p_subpart_type VARCHAR2, p_subpartitions INTEGER,
			          p_arch_flag VARCHAR2 DEFAULT 'N', 
                                  p_owner VARCHAR2, p_table_name VARCHAR2,
	                          p_part_name VARCHAR2 DEFAULT '') IS
                l_part_def CLOB;
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'TAB_PART_RANGES');

		debug_msg('tab_part_ranges:'||p_type||'/'||p_recname
 			||'/part_id='||p_part_id
 			||'/subpart_type='||p_subpart_type
			||'/subpartititons='||p_subpartitions
			||'/arch_flag='||p_arch_flag
			||'/'||p_owner||'./'||p_table_name,6);

                FOR p_tab_part_ranges IN(
	                SELECT r.*
        	        FROM  gfc_part_ranges r
                	WHERE r.part_id = p_part_id
			AND   (r.arch_flag = p_arch_flag 
			      OR p_arch_flag IS NULL
			      OR EXISTS (
				SELECT 'x' 
				FROM  dba_tab_partitions p
				WHERE p.table_name = UPPER(p_table_name)
				AND   p.table_owner = UPPER(p_owner)
				AND   p.partition_name = UPPER(p_recname||'_'||r.part_name)
				))
	                ORDER BY r.part_no, r.part_name
                ) LOOP
			debug_msg('Part Range:'||p_tab_part_ranges.part_no||'/'||p_tab_part_ranges.part_name,6);

                        l_counter := l_counter + 1;
                        IF l_counter = 1 THEN
                                l_part_def := '(';
                        ELSE
                                l_part_def := ',';
                        END IF;
                        l_part_def := l_part_def||'PARTITION '||LOWER(NVL(p_part_name,p_recname)
			                                              ||'_'||p_tab_part_ranges.part_name);
			l_part_def := l_part_def||' VALUES LESS THAN ('||p_tab_part_ranges.part_value||')';
			IF LENGTH(l_part_def) > k_max_line_length_margin THEN
	            ins_line(p_type,l_part_def);
				l_part_def := '';
			END IF;
			IF p_tab_part_ranges.partial_index = 'Y' THEN
				l_part_def := l_part_def||' INDEXING ON';
			ELSIF p_tab_part_ranges.partial_index = 'N' THEN
				l_part_def := l_part_def||' INDEXING OFF';
			END IF;
			IF LENGTH(l_part_def) > 0 THEN
				ins_line(p_type,l_part_def);
				l_part_def := '';
			END IF;
            IF p_tab_part_ranges.tab_tablespace IS NOT NULL THEN
                l_part_def := l_part_def||' TABLESPACE '||p_tab_part_ranges.tab_tablespace;
            END IF;
            IF p_tab_part_ranges.tab_storage IS NOT NULL THEN
                l_part_def := l_part_def||' '||tab_storage(p_recname, p_tab_part_ranges.tab_storage); -- 6.9.2007
            END IF;
			IF LENGTH(l_part_def) > 0 THEN
				ins_line(p_type,l_part_def);
				l_part_def := '';
			END IF;
                        IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                                FOR l_subpartition IN 1..p_subpartitions LOOP
                                        IF l_subpartition = 1 THEN
                                                l_part_def := '(';
                                        ELSE
                                                l_part_def := ',';
                                        END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '
						||LOWER(NVL(p_part_name,p_recname)||'_'||p_tab_part_ranges.part_name
						||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                        ins_line(p_type,l_part_def);
                                END LOOP;
                                ins_line(p_type,')');
			ELSIF p_subpart_type = 'R' THEN --20.10.2014 add range sub-partitioning
				tab_rangesubparts(p_type=>p_type
					,p_recname=>p_recname
					,p_part_id=>p_part_id
					,p_part_name=>p_tab_part_ranges.part_name
					,p_part_basename=>NVL(p_part_name,p_recname)
					,p_arch_flag=>p_arch_flag); 
			ELSIF p_subpart_type = 'L' THEN
				tab_listsubparts(p_type=>p_type
					,p_recname=>p_recname
					,p_part_id=>p_part_id
					,p_part_name=>p_tab_part_ranges.part_name
					,p_part_basename=>NVL(p_part_name,p_recname)
					,p_arch_flag=>p_arch_flag); 
                        END IF;
                END LOOP;
                IF l_counter > 0 THEN
                        ins_line(p_type,')');
                END IF;

		unset_action(p_action_name=>l_action);
        END tab_part_ranges;

-------------------------------------------------------------------------------------------------------
-- generate partition clause on the basis of part ranges table
-------------------------------------------------------------------------------------------------------
PROCEDURE tab_part_lists(p_type NUMBER, p_recname VARCHAR2, p_part_id VARCHAR2, 
		         p_subpart_type VARCHAR2, p_subpartitions INTEGER, p_arch_flag VARCHAR2 DEFAULT 'N') IS
                CURSOR c_tab_part_lists(p_recname VARCHAR2) IS
                SELECT *
                FROM gfc_part_lists l
                WHERE l.part_id = p_part_id
                AND  (l.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
                ORDER BY part_no, part_name;

                p_tab_part_lists c_tab_part_lists%ROWTYPE;
                l_part_def CLOB;
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'TAB_PART_LISTS');

		debug_msg('tab_part_lists:'||p_type
 			||'/recname='||p_recname
			||'/part_id='||p_part_id
			||'/subpart_type='||p_subpart_type
			||'/subpartitions='||p_subpartitions
			||'/arch_flag='||p_arch_flag,6);
                OPEN c_tab_part_lists(p_recname);
                LOOP
                        FETCH c_tab_part_lists INTO p_tab_part_lists;
			debug_msg('Part Range:'||p_tab_part_lists.part_No||'/'||p_tab_part_lists.part_name,6);
                        EXIT WHEN c_tab_part_lists%NOTFOUND;

                        l_counter := l_counter + 1;
                        IF l_counter = 1 THEN
                                l_part_def := '(';
                        ELSE
                                l_part_def := ',';
                        END IF;
                        l_part_def := l_part_def||'PARTITION '||LOWER(p_recname||'_'||p_tab_part_lists.part_name);
                        ins_line(p_type,l_part_def);
						l_part_def := ' VALUES ('||p_tab_part_lists.list_value||')';
                        IF LENGTH(l_part_def) > k_max_line_length_margin THEN --26.11.2020 added partial index support
							ins_line(p_type,l_part_def);
							l_part_def := '';
						END IF;
						IF p_tab_part_lists.partial_index = 'Y' THEN
							l_part_def := l_part_def||' INDEXING ON';
						ELSIF p_tab_part_lists.partial_index = 'N' THEN
							l_part_def := l_part_def||' INDEXING OFF';
						END IF;
                        ins_line(p_type,l_part_def);
            			l_part_def := '';
						IF p_tab_part_lists.tab_tablespace IS NOT NULL THEN
							l_part_def := l_part_def||' TABLESPACE '||p_tab_part_lists.tab_tablespace;
                        END IF;
                        IF p_tab_part_lists.tab_storage IS NOT NULL THEN
                                l_part_def := l_part_def||' '||tab_storage(p_recname, p_tab_part_lists.tab_storage); -- 6.9.2007
                        END IF;
			IF LENGTH(l_part_def) > 0 THEN
	                        ins_line(p_type,l_part_def);
			END IF;
                        IF p_subpart_type = 'R' THEN --20.10.2014 add range sub-partitioning
				tab_rangesubparts(p_type=>p_type
					,p_recname=>p_recname
					,p_part_id=>p_part_id
					,p_part_name=>p_tab_part_lists.part_name
					,p_part_basename=>NVL(p_tab_part_lists.part_name,p_recname)
					,p_arch_flag=>p_arch_flag); 
                        ELSIF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                                FOR l_subpartition IN 1..p_subpartitions LOOP
                                        IF l_subpartition = 1 THEN
                                                l_part_def := '(';
                                        ELSE
                                                l_part_def := ',';
                                        END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '
						||LOWER(p_recname||'_'||p_tab_part_lists.part_name
						||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                        ins_line(p_type,l_part_def);
                                END LOOP;
                                ins_line(p_type,')');
                        END IF;
                END LOOP;
                IF l_counter > 0 THEN
                        ins_line(p_type,')');
                END IF;
                CLOSE c_tab_part_lists;

		unset_action(p_action_name=>l_action);
        END tab_part_lists;

-------------------------------------------------------------------------------------------------------
-- generate partition clause on the basis of part ranges table
-------------------------------------------------------------------------------------------------------
PROCEDURE ind_hashparts(p_type      NUMBER
	                       ,p_recname   VARCHAR2
	                       ,p_indexid   VARCHAR2 DEFAULT '_'
	                       ,p_num_parts INTEGER) IS
                l_part_def CLOB;
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'IND_HASHPARTS');

                IF p_num_parts > 1 THEN
                        FOR l_subpartition IN 1..p_num_parts LOOP
                                IF l_subpartition = 1 THEN
                                        l_part_def := '(';
                                ELSE
                                        l_part_def := ',';
                                END IF;
                                l_part_def := l_part_def||'PARTITION '||LOWER(p_recname||p_indexid||LTRIM(TO_CHAR(l_subpartition,'00')));
	                        ins_line(p_type,l_part_def);
                        END LOOP;
			ins_line(p_type,')');
                END IF;

		unset_action(p_action_name=>l_action);
        END ind_hashparts;

-------------------------------------------------------------------------------------------------------
PROCEDURE tab_hashparts(p_type      NUMBER
	                       ,p_recname   VARCHAR2
	                       ,p_num_parts INTEGER) IS
                l_part_def CLOB;
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
        BEGIN
		ind_hashparts(p_type, p_recname, '_', p_num_parts);
        END tab_hashparts;

-------------------------------------------------------------------------------------------------------
-- generate partition clause on the basis of part ranges table
-------------------------------------------------------------------------------------------------------
PROCEDURE ind_listparts(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2, 
                                p_part_id VARCHAR2, p_part_name VARCHAR2,
                                p_arch_flag VARCHAR2 DEFAULT ''
                               ) IS
                l_part_def CLOB;
                l_counter INTEGER := 0;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'IND_LISTPARTS');

		debug_msg('IND_LISTPARTS:recname='||p_recname
                                     ||'/indexid='||p_indexid
                                     ||'/part_id='||p_part_id
                                     ||'/part_name='||p_part_name
                                     ||'/arch_flag='||p_arch_flag
                                     ,6);

		FOR t IN(
			SELECT  a.*
			FROM	gfc_part_lists a, gfc_part_subparts b
			WHERE	a.part_id = p_part_id
			AND     b.part_id = p_part_id
			AND	b.part_name = p_part_name
			AND     b.subpart_name = a.part_name
			AND     b.build = 'Y' -- if subpartition to be built in range partition
                  AND    (a.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
			ORDER BY a.part_name
		) LOOP
			IF l_counter = 0 THEN
				l_part_def := '(';
			ELSE
				l_part_def := ',';
			END IF;
			l_part_def := l_part_def||'SUBPARTITION '
			                        ||LOWER(p_recname||p_indexid||p_part_name||'_'||t.part_name);
                        IF t.tab_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def||' TABLESPACE '||t.idx_tablespace;
                        END IF;
-- 27.11.2012 suppressed because causes ORA-14189: this physical attribute may not be specified for an index subpartition
--                      IF t.tab_storage IS NOT NULL THEN
--                              l_part_def := l_part_def||' '||idx_storage(p_recname, p_indexid, t.idx_storage);
--                      END IF;
                        ins_line(p_type,l_part_def);
			l_counter := l_counter + 1;
		END LOOP;
		IF l_counter > 0 THEN
			ins_line(p_type,')');
		END IF;

		unset_action(p_action_name=>l_action);
        END ind_listparts;

-------------------------------------------------------------------------------------------------------
-- generate partition clause for create index DDL
-------------------------------------------------------------------------------------------------------
PROCEDURE ind_part_ranges(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2, 
                            p_part_id VARCHAR2, p_subpart_type VARCHAR2, p_subpartitions NUMBER,
                            p_arch_flag VARCHAR2 DEFAULT 'N', 
                            p_owner VARCHAR2, 
                            p_index_prefix VARCHAR2 DEFAULT 'PS',
	                    p_part_name VARCHAR2 DEFAULT '') IS
                l_part_def CLOB;
                l_subpartition INTEGER;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'IND_PART_RANGES');

		debug_msg('ind_part_ranges:'||p_type
                        ||'/recname='||p_recname
                        ||'/indexid='||p_indexid
                        ||'/part_id='||p_part_id
                        ||'/subpart_type='||p_subpart_type
                        ||'/subpartitions='||p_subpartitions
                        ||'/arch_flag='||p_arch_flag
                        ||'/owner='||p_owner
                        ||'/index_prefix='||p_index_prefix
                        ||'/part_name='||p_part_name,6);

                l_part_def := '(';
                FOR p_ind_part_ranges IN(
                    SELECT r.*
        	        FROM gfc_part_ranges r
                    WHERE r.part_id = p_part_id
	              AND   (r.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
                    ORDER BY part_no, part_name
                ) LOOP
                        l_part_def := l_part_def||'PARTITION '||LOWER(NVL(p_part_name,p_recname)
									||p_indexid||p_ind_part_ranges.part_name);
                        IF p_ind_part_ranges.idx_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def ||' TABLESPACE '||p_ind_part_ranges.idx_tablespace;
                        END IF;
                        IF p_ind_part_ranges.idx_storage IS NOT NULL THEN
                                l_part_def := l_part_def ||' '||
                                      idx_storage(p_recname, p_indexid, 
                                                  p_ind_part_ranges.idx_storage,
                                                  p_subpartitions); -- 6.9.2007
                        END IF;
                        ins_line(p_type,l_part_def);
                        -- explicity define subpartitions
                        IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                                FOR l_subpartition IN 1..p_subpartitions LOOP
                                        IF l_subpartition = 1 THEN
                                                l_part_def := '(';
                                        ELSE
                                                l_part_def := ',';
                                        END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '
                                                      ||LOWER(NVL(p_part_name,p_recname)
				                              ||p_indexid||p_ind_part_ranges.part_name
                                                      ||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                        ins_line(p_type,l_part_def);
                                END LOOP;
                                ins_line(p_type,')');
			ELSIF p_subpart_type = 'L' THEN
				ind_listparts(p_type=>p_type, p_recname=>p_recname, p_indexid=>p_indexid, 
                                      p_part_id=>p_part_id, p_part_name=>p_ind_part_ranges.part_name, 
                                      p_arch_flag=>p_arch_flag);
                        END IF;
                        l_part_def := ',';
                END LOOP;
                ins_line(p_type,')');

		unset_action(p_action_name=>l_action);
        END ind_part_ranges;

-------------------------------------------------------------------------------------------------------
-- generate partition clause for create index DDL
-------------------------------------------------------------------------------------------------------
PROCEDURE ind_part_lists(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2, 
                            p_part_id VARCHAR2, p_subpart_type VARCHAR2, p_subpartitions NUMBER) IS
                CURSOR c_ind_part_lists IS
                SELECT *
                FROM gfc_part_lists r
                WHERE r.part_id = p_part_id
                ORDER BY part_no, part_name
                ;
                p_ind_part_lists c_ind_part_lists%ROWTYPE;
                l_part_def CLOB;
                l_subpartition INTEGER;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'IND_PART_LISTS');

                OPEN c_ind_part_lists;
                l_part_def := '(';
                LOOP
                        FETCH c_ind_part_lists INTO p_ind_part_lists;
                        EXIT WHEN c_ind_part_lists%NOTFOUND;

                        l_part_def := l_part_def||'PARTITION '||LOWER(p_recname||p_indexid||p_ind_part_lists.part_name);
                        IF p_ind_part_lists.idx_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def ||' TABLESPACE '||p_ind_part_lists.idx_tablespace;
                        END IF;
                        IF p_ind_part_lists.idx_storage IS NOT NULL THEN
                                l_part_def := l_part_def ||' '||
                                      idx_storage(p_recname, p_indexid, p_ind_part_lists.idx_storage,
                                                  p_subpartitions); -- 6.9.2007
                        END IF;
                        ins_line(p_type,l_part_def);
                        -- explicity define subpartitions
                        IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                        FOR l_subpartition IN 1..p_subpartitions LOOP
                                IF l_subpartition = 1 THEN
                                        l_part_def := '(';
                                ELSE
                                        l_part_def := ',';
                                END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '
                                                      ||LOWER(p_recname||p_indexid||p_ind_part_lists.part_name
                                                      ||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                ins_line(p_type,l_part_def);
                        END LOOP;
                        ins_line(p_type,')');
			ELSIF p_subpart_type = 'L' THEN
				ind_listparts(p_type, p_recname, p_indexid, p_part_id, p_ind_part_lists.part_name);
                        END IF;
                        l_part_def := ',';
                END LOOP;
                ins_line(p_type,')');
                CLOSE c_ind_part_lists;

		unset_action(p_action_name=>l_action);
        END ind_part_lists;

-------------------------------------------------------------------------------------------------------
-- enable/disable DDL trigger, added 10.10.2007
-------------------------------------------------------------------------------------------------------
	PROCEDURE ddlpermit(p_type NUMBER, p_ddlpermit BOOLEAN) IS
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDLTRIGGER');

		-- 5.2.2013 replace trigger name with commands
		IF p_ddlpermit THEN
			IF l_ddlenable IS NOT NULL THEN
				ins_line(p_type,l_ddlenable);
				ins_line(p_type,'');
			END IF;
		ELSE
			IF l_ddldisable IS NOT NULL THEN
				ins_line(p_type,l_ddldisable);
				ins_line(p_type,'');
			END IF;
		END IF;

--		IF l_ddltrigger IS NOT NULL THEN
--			IF p_ddlpermit THEN
--				ins_line(p_type,'ALTER TRIGGER '||l_ddltrigger||' DISABLE');
--			ELSE
--				ins_line(p_type,'ALTER TRIGGER '||l_ddltrigger||' ENABLE');
--			END IF;
--			ins_line(p_type,'/');
--			ins_line(p_type,'');
--		END IF;

		unset_action(p_action_name=>l_action);
	END ddlpermit;

-------------------------------------------------------------------------------------------------------
-- generate partition clause for global index
--only build partitions not set to (D)rop or (A)rchive
-------------------------------------------------------------------------------------------------------
PROCEDURE glob_ind_parts(p_type INTEGER, p_recname VARCHAR2, p_indexid VARCHAR2, p_part_id VARCHAR2, 
		            p_part_type VARCHAR2, p_subpart_type VARCHAR2, p_subpartitions INTEGER) IS
                CURSOR c_idx_parts (p_recname VARCHAR2) IS
                SELECT *
                FROM gfc_part_ranges r
                WHERE r.part_id = p_part_id
 		AND r.arch_flag = 'N' -- 27.03.2013 only build partitions in global part indexes not marked for delete or archive
                ORDER BY part_no, part_name;

                p_idx_parts c_idx_parts%ROWTYPE;
                l_part_def CLOB;
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'GLOB_IND_PARTS');

		debug_msg('glob_ind_PARTS:'||p_recname||'/'||p_part_id||'/'||p_part_type||'/'||p_subpart_type,6);
                OPEN c_idx_parts(p_recname);
                LOOP
                        FETCH c_idx_parts INTO p_idx_parts;
			debug_msg('Part Range:'||p_idx_parts.part_No||'/'||p_idx_parts.part_name,6);
                        EXIT WHEN c_idx_parts%NOTFOUND;

                        l_counter := l_counter + 1;
                        IF l_counter = 1 THEN
                                l_part_def := '(';
                        ELSE
                                l_part_def := ',';
                        END IF;
                        l_part_def := l_part_def||'PARTITION '||LOWER(p_recname||p_indexid||p_idx_parts.part_name);
			IF p_part_type = 'R' THEN
				l_part_def := l_part_def||' VALUES LESS THAN ('||p_idx_parts.part_value||')';
			ELSIF p_part_type = 'L' THEN -- list
				l_part_def := l_part_def||' VALUES ('||p_idx_parts.part_value||')';
			END IF;
                        IF p_idx_parts.idx_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def||' TABLESPACE '||p_idx_parts.idx_tablespace;
                        END IF;
                        IF p_idx_parts.idx_storage IS NOT NULL THEN
                                l_part_def := l_part_def||' '||idx_storage(p_recname, p_indexid, 
				                                           p_idx_parts.idx_storage,
				                                           p_subpartitions); -- 6.9.2007
                        END IF;
                        ins_line(p_type,l_part_def);
                        IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                                FOR l_subpartition IN 1..p_subpartitions LOOP
                                        IF l_subpartition = 1 THEN
                                                l_part_def := '(';
                                        ELSE
                                                l_part_def := ',';
                                        END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '
					                        ||LOWER(p_recname||p_indexid||p_idx_parts.part_name
					                        ||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                        ins_line(p_type,l_part_def);
                                END LOOP;
                                ins_line(p_type,')');
			ELSIF p_subpart_type = 'L' THEN
				idx_listsubparts(p_type,p_recname,p_indexid,p_part_id,p_idx_parts.part_name);
			ELSIF p_subpart_type = 'T' THEN
				idx_rangesubparts(p_type,p_recname,p_indexid,p_part_id,p_idx_parts.part_name);
                        END IF;
                END LOOP;
                IF l_counter > 0 THEN
                        ins_line(p_type,')');
                END IF;
                CLOSE c_idx_parts;

		unset_action(p_action_name=>l_action);
        END glob_ind_parts;

-------------------------------------------------------------------------------------------------------
-- generate all partitioned indexes defined on record
-- wibble do not build archive partition
-------------------------------------------------------------------------------------------------------
PROCEDURE mk_part_indexes(p_recname    VARCHAR2
                                 ,p_table_name VARCHAR2
                                 ,p_schema     VARCHAR2
                                 ,p_arch_flag  VARCHAR2 
                                 ) IS
  l_ind_def VARCHAR2(100 CHAR);
  l_type INTEGER;
  l_schema VARCHAR2(31 CHAR);
  l_ind_prefix VARCHAR2(3 CHAR);
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'MK_PART_INDEXES');

  debug_msg('mk_part_indexes:'||p_recname||'/'||p_schema||'.'||p_table_name||'/arch_flag='||p_arch_flag,6);

  -- ins_line(k_index,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
  -- ins_line(k_index,LOWER('spool gfcindex_'||l_dbname||'_'||p_recname||'.lst'));
  signature(k_index,FALSE,'gfcindex',p_recname);
  whenever_sqlerror(k_index,TRUE);
  forceddldop(k_index);
  ddlpermit(k_index,TRUE); -- added 10.10.2007
-- 25.5.2011 do not build unique index on IOT
  FOR p_indexes IN(
    SELECT g.indexid, g.uniqueflag, g.platform_ora
    ,      p.RECNAME
    ,      DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name --16.1.2022 added table name to cater for PeopleTools tables
    ,      NVL(i.part_id,p.part_id) part_id
    ,      NVL(i.part_type,p.part_type) part_type
    ,      NVL(i.part_column,p.part_column) part_column
    ,      NVL(i.subpart_type,p.subpart_type) subpart_type
    ,      NVL(i.hash_partitions,p.hash_partitions) hash_partitions
    ,      NVL(i.subpart_column,p.subpart_column) subpart_column 
    ,      NVL(i.idx_tablespace,p.idx_tablespace) idx_tablespace
    ,      NVL(i.idx_storage,p.idx_storage) idx_storage
    ,      NVL(i.override_schema,p.override_schema) override_schema
    ,      CASE WHEN p.part_type = 'N' THEN 'G' -- added 9.11.2011 to support partitioned indexes on non-partitioned tables
                WHEN i.indexid IS NULL THEN 'L' -- separated 20.11.2020 
                WHEN i.part_type = p.part_type  -- added 20.11.2020 to match table to index partitioning
                 AND i.part_column = p.part_column 
                 AND (i.subpart_type = p.subpart_type OR i.subpart_type IS NULL) 
                 AND (i.subpart_column = p.subpart_column OR i.subpart_column IS NULL) THEN 'L'
                WHEN i.part_type = 'L' THEN 'L'
                ELSE 'G' END AS ind_part_type
    ,      i.partial_index
    ,      i.name_suffix
    FROM   gfc_ps_indexdefn g -- 6.9.2007 removed psindexdefn
           LEFT OUTER JOIN gfc_part_indexes i
           ON i.recname = g.recname
           AND i.indexid = g.indexid
    ,        gfc_part_tables p
    ,        psrecdefn r
    WHERE    p.recname = g.recname
    AND      g.recname = p_recname
    AND      p.recname = p_recname
    AND      r.recname = p_recname
    AND      g.platform_ora = 1
    AND      NOT (p.organization = 'I' AND g.indexid = '_') 
    ORDER BY g.indexid)
  LOOP
    IF l_explicit_schema = 'Y' THEN
      l_schema := NVL(p_indexes.override_schema,LOWER(p_schema))||'.';
    ELSE 
      l_schema := '';
    END IF;

    --first do indexes for standard build script --
    whenever_sqlerror(k_index,FALSE);
    IF l_drop_index = 'Y' THEN 
      ins_line(k_index,'DROP INDEX '||LOWER(l_schema||'ps'||p_indexes.indexid||p_recname||p_indexes.name_suffix));
      ins_line(k_index,'/');
      ins_line(k_index,'');
    END IF;

    whenever_sqlerror(k_index,TRUE);
    FOR l_type IN 0..1 LOOP -- do the index create twice
      IF l_type = 0 THEN
        l_ind_prefix := 'gfc';
      ELSE
        l_ind_prefix := 'ps';
      END IF;
      l_ind_def := 'CREATE';
      IF p_indexes.uniqueflag = 1 THEN
        l_ind_def := l_ind_def||' UNIQUE';
      END IF;
      l_ind_def := l_ind_def||' INDEX '||LOWER(l_schema||l_ind_prefix||p_indexes.indexid||p_recname||p_indexes.name_suffix)||
                   ' ON '||LOWER(l_schema||l_ind_prefix||'_'||p_recname); --reverted 6.11.2022
--                 ' ON '||LOWER(l_schema||p_indexes.table_name);  --16.1.2022 correct table name for PeopleTools tables
      ins_line(l_type,l_ind_def);
      ind_cols(l_type,p_recname,p_indexes.indexid,l_desc_index);
      IF p_indexes.ind_part_type = 'L' THEN -- local partitioning
        ins_line(l_type,'LOCAL');
        IF p_indexes.part_type = 'L' THEN
          ind_part_lists(p_type=>l_type,
                         p_recname=>p_recname,
                         p_indexid=>p_indexes.indexid,
                         p_part_id=>p_indexes.part_id,
                         p_subpart_type=>p_indexes.subpart_type,
                         p_subpartitions=>p_indexes.hash_partitions);
        ELSIF p_indexes.part_type = 'R' THEN
          ind_part_ranges(p_type=>l_type,
                          p_recname=>p_recname,
                          p_indexid=>p_indexes.indexid,
                          p_part_id=>p_indexes.part_id,
                          p_subpart_type=>p_indexes.subpart_type,
                          p_subpartitions=>p_indexes.hash_partitions,
                          p_arch_flag=>p_arch_flag,
                          p_owner=>p_schema,
                          p_index_prefix=>'PS',
                          p_part_name=>p_recname);
        ELSIF p_indexes.part_type = 'H' AND p_indexes.hash_partitions > 1 THEN
          ind_hashparts(l_type,p_recname
                       ,p_indexes.indexid,p_indexes.hash_partitions);
        END IF;
      ELSIF p_indexes.part_type IN('R') THEN -- add global range index clause here
        IF p_indexes.part_type = 'R' THEN
          ins_line(l_type,'GLOBAL PARTITION BY RANGE ('||p_indexes.part_column||')');
        ELSE -- list
          ins_line(l_type,'GLOBAL PARTITION BY LIST('||p_indexes.part_column||')');
        END IF;
        IF p_indexes.subpart_type = 'H' AND 
           p_indexes.hash_partitions > 1 AND 
           p_indexes.subpart_column IS NOT NULL THEN
            ins_line(l_type,'SUBPARTITION BY HASH ('||p_indexes.subpart_column||') SUBPARTITIONS '||p_indexes.hash_partitions);
        ELSIF p_indexes.subpart_type = 'L' AND 
              p_indexes.subpart_column IS NOT NULL THEN
          ins_line(l_type,'SUBPARTITION BY LIST ('||p_indexes.subpart_column||')');
        END IF;
        glob_ind_parts(l_type,p_indexes.recname, p_indexes.indexid, p_indexes.part_id,
                              p_indexes.part_type, p_indexes.subpart_type,
                              p_indexes.hash_partitions);

      ELSIF p_indexes.part_type = 'H' AND p_indexes.hash_partitions > 1 THEN
        ins_line(l_type,'GLOBAL PARTITION BY HASH ('||p_indexes.part_column||')');
        ind_hashparts(p_type     =>l_type
                     ,p_recname  =>p_indexes.recname
                     ,p_indexid  =>p_indexes.indexid
                     ,p_num_parts=>p_indexes.hash_partitions);
      END IF;

      IF p_indexes.partial_index = 'Y' THEN --added 26.11.2020 partial indexing support
        ins_line(l_type,'INDEXING PARTIAL');
      END IF;

      -- index level storage clause
      IF p_indexes.idx_tablespace IS NOT NULL THEN
        ins_line(l_type,'TABLESPACE '||p_indexes.idx_tablespace);
      END IF;
      IF p_indexes.idx_storage IS NOT NULL THEN
        ins_line(l_type,idx_storage(p_recname, p_indexes.indexid, p_indexes.idx_storage, p_indexes.hash_partitions)); -- 6.9.2007
      END IF;

      -- 9.10.2003 - create index parallel
      IF l_parallel_index = 'Y' THEN
        IF l_force_para_dop IS NULL THEN
          ins_line(l_type,'PARALLEL');
        ELSE
          ins_line(l_type,'PARALLEL '||l_force_para_dop);
        END IF;
      ELSE
        ins_line(l_type,'NOPARALLEL');
      END IF;

      -- 9.10.2003 - create index nologging, then change it to logged noparallel
      IF l_logging = 'N' THEN
        ins_line(l_type,'NOLOGGING');
      END IF;
      ins_line(l_type,'/');
      ins_line(l_type,'');

      IF l_logging = 'N' THEN
        ins_line(l_type,'ALTER INDEX '||LOWER(l_schema||l_ind_prefix||p_indexes.indexid||p_recname||p_indexes.name_suffix));
        ins_line(l_type,'LOGGING');
        ins_line(l_type,'/');
      END IF;
      IF l_parallel_index = 'Y' THEN
        ins_line(l_type,'ALTER INDEX '||LOWER(l_schema||l_ind_prefix||p_indexes.indexid||p_recname||p_indexes.name_suffix));
        ins_line(l_type,'NOPARALLEL');
        ins_line(l_type,'/');
      END IF;
      ins_line(l_type,'');
    END LOOP;

    whenever_sqlerror(k_build,FALSE);

    IF l_drop_index = 'Y' THEN
      ins_line(k_build,l_noalterprefix||'DROP INDEX '||LOWER(l_schema||'ps'||p_indexes.indexid||p_recname||p_indexes.name_suffix)); -- 6.9.2007
    ELSE
      ins_line(k_build,'ALTER INDEX '||LOWER(l_schema||'ps'||p_indexes.indexid||p_recname)
                       ||' RENAME TO old'||LOWER(p_indexes.indexid||p_recname
                       ||p_indexes.name_suffix));
    END IF;
    ins_line(k_build,'/');

    whenever_sqlerror(k_build,TRUE);

    ins_line(k_build,l_noalterprefix||'ALTER INDEX ' -- 6.9.2007 
                     ||LOWER(l_schema||'gfc'||p_indexes.indexid||p_recname||p_indexes.name_suffix)
                     ||' RENAME TO ps'||LOWER(p_indexes.indexid||p_recname||p_indexes.name_suffix)); 
    ins_line(k_build,'/');
    ins_line(k_build,'');
-- 30.6.2005-bugfix-do not  drop anything after index only build
--  IF l_drop_index = 'Y' THEN
--    whenever_sqlerror(k_index,FALSE);
--    ins_line(k_index,'DROP INDEX '||LOWER(l_schema||'ps'||p_indexes.indexid||p_recname||p_indexes.name_suffix)||';');
--    whenever_sqlerror(k_index,TRUE);
--  ELSE
--    ins_line(k_index,'ALTER INDEX '||LOWER(p_schema||'ps'||p_indexes.indexid||p_recname||p_indexes.name_suffix)||' RENAME TO old'
--                                                   ||LOWER(p_indexes.indexid||p_recname||p_indexes.name_suffix)||';');
--  END IF;
--  ins_line(k_index,'ALTER INDEX '||LOWER(l_schema||'gfc'||p_indexes.indexid||p_recname||p_indexes.name_suffix)
--                ||' RENAME TO ps'||LOWER(p_indexes.indexid||p_recname||p_indexes.name_suffix)||';');
    ins_line(k_index,'');
  END LOOP;
  ddlpermit(k_index,FALSE); -- added 10.10.2007
  ins_line(k_index,'spool off');

  unset_action(p_action_name=>l_action);
END mk_part_indexes;

-------------------------------------------------------------------------------------------------------
-- generate all partitioned indexes defined on record
-------------------------------------------------------------------------------------------------------
PROCEDURE mk_arch_indexes(p_type       NUMBER
                         ,p_recname    VARCHAR2
                         ,p_schema     VARCHAR2
                         ,p_table_name VARCHAR2
                         ,p_part_name  VARCHAR2 DEFAULT ''
                         ,p_ind_prefix VARCHAR2 DEFAULT 'ARC'
                         ) IS
  l_ind_def VARCHAR2(100 CHAR);
  l_schema VARCHAR2(31 CHAR);
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'MK_ARCH_INDEXES');

		debug_msg('mk_arch_indexes:recname='||p_recname
			||'/table_name='||p_schema||'.'||p_table_name
			||'/part_name='||p_part_name
			||'/ind_prefix='||p_ind_prefix
			,6);

		l_schema := LOWER(p_schema||'.');

-- 25.5.2011 do not build unique index on IOT
-- 25.5.2012 added NOT to unique IOT index logic
                FOR p_indexes IN(
                	SELECT   g.indexid, g.uniqueflag, g.platform_ora
	                ,        p.RECNAME
					,        NVL(i.part_id,p.part_id) part_id
	                ,        NVL(i.part_type,p.part_type) part_type
	                ,        NVL(i.part_column,p.part_column) part_column
	                ,        NVL(i.subpart_type,p.subpart_type) subpart_type
	                ,        NVL(i.subpart_column,p.subpart_column) subpart_column 
					,        NVL(i.hash_partitions,p.hash_partitions) hash_partitions
					, 	     NVL(i.idx_tablespace,p.idx_tablespace) idx_tablespace
	                ,        NVL(i.idx_storage,p.idx_storage) idx_storage
	                ,        NVL(i.override_schema,p.override_schema) override_schema
					,        CASE WHEN i.indexid IS NULL OR i.part_type = 'L' THEN 'L'
 			                      ELSE 'G'
			                      END AS ind_part_type
					,        i.name_suffix
        	        FROM     gfc_ps_indexdefn g -- 6.9.2007 removed psindexdefn
			         LEFT OUTER JOIN gfc_part_indexes i
			         ON i.recname = g.recname
			         AND i.indexid = g.indexid
                	,        gfc_part_tables p
	                WHERE    p.recname = g.recname
					AND      g.recname = p_recname
                	AND      p.recname = p_recname
					AND      g.platform_ora = 1
					AND      NOT (p.organization = 'I' AND g.indexid = '_') 
					AND 	 (i.part_type = 'L' OR i.part_type IS NULL OR g.uniqueflag = 1)
					ORDER BY g.indexid)
                LOOP
			debug_msg('p_indexes:indexid='||p_indexes.indexid
				||'/part_type='||p_indexes.part_type
				||'/ind_part_type='||p_indexes.ind_part_type,7);

			IF p_ind_prefix = 'XCHG' AND p_indexes.ind_part_type = 'G' THEN
                        	ins_line(p_type,'DROP INDEX '||LOWER(p_schema||'.'||p_ind_prefix||p_indexes.indexid||p_recname||p_indexes.name_suffix));
	                        ins_line(p_type,'/');
			ELSE
	                        l_ind_def := 'CREATE';
        	                IF p_indexes.uniqueflag = 1 THEN
       	        	                l_ind_def := l_ind_def||' UNIQUE';
               	        	END IF;
	                       	l_ind_def := l_ind_def||' INDEX '||
        	                        LOWER(p_schema||'.'||p_ind_prefix||p_indexes.indexid||p_recname||p_indexes.name_suffix)||
                	                ' ON '||LOWER(p_schema||'.'||p_table_name);
                        	ins_line(p_type,l_ind_def);
	                        ind_cols(p_type,p_recname,p_indexes.indexid,l_desc_index);
				IF p_part_name IS NULL THEN
					NULL; -- do not partition the index
				ELSIF p_indexes.ind_part_type = 'L' THEN  -- local partitioning
	                       	        ins_line(p_type,'LOCAL');
					IF p_indexes.part_type = 'L' THEN
                	                	ind_part_lists(p_type=>p_type,
						          p_recname=>p_recname,
						          p_indexid=>p_indexes.indexid,
						          p_part_id=>p_indexes.part_id,
					        	  p_subpart_type=>p_indexes.subpart_type,
					        	  p_subpartitions=>p_indexes.hash_partitions);
					ELSIF p_indexes.part_type = 'R' THEN
                	                	ind_part_ranges(p_type=>p_type,
					         p_recname=>p_recname,
					         p_indexid=>p_indexes.indexid,
				        	 p_part_id=>p_indexes.part_id,
					         p_subpart_type=>p_indexes.subpart_type,
					         p_subpartitions=>p_indexes.hash_partitions,
					         p_arch_flag=>'A',
					         p_owner=>p_schema,
					         p_index_prefix=>p_ind_prefix,
				        	 p_part_name=>p_part_name);
					ELSIF p_indexes.part_type = 'H' AND 
					      p_indexes.hash_partitions > 1 THEN
						ind_hashparts(p_type,p_recname
					                    ,p_indexes.indexid,p_indexes.hash_partitions);
					END IF;
				ELSIF p_indexes.part_type IN('R') THEN -- add global range index clause here
					IF p_indexes.part_type = 'R' THEN
      					                ins_line(p_type,'GLOBAL PARTITION BY RANGE ('
  							        ||p_indexes.part_column||')');
					ELSE -- list-although oracle doesn't support this yet!
	     				                ins_line(p_type,'GLOBAL PARTITION BY LIST('
					        ||p_indexes.part_column||')');
					END IF;
	       	        	        IF p_indexes.subpart_type = 'H' AND 
					   p_indexes.hash_partitions > 1 AND 
					   p_indexes.subpart_column IS NOT NULL THEN
						ins_line(p_type,'SUBPARTITION BY HASH ('||p_indexes.subpart_column
						          ||') SUBPARTITIONS '||p_indexes.hash_partitions);
					ELSIF p_indexes.subpart_type = 'L' AND 
					      p_indexes.subpart_column IS NOT NULL THEN
						ins_line(p_type,'SUBPARTITION BY LIST ('||p_indexes.subpart_column||')');
					END IF;
		                        glob_ind_parts(p_type,p_indexes.recname, p_indexes.indexid, p_indexes.part_id,
					          p_indexes.part_type, p_indexes.subpart_type,
					          p_indexes.hash_partitions);
	
				ELSIF p_indexes.part_type = 'H' AND p_indexes.hash_partitions > 1 THEN
					ins_line(p_type,'GLOBAL PARTITION BY HASH ('||p_indexes.part_column||')');
					ind_hashparts(p_type     =>p_type
					             ,p_recname  =>p_indexes.recname
					             ,p_indexid  =>p_indexes.indexid
					             ,p_num_parts=>p_indexes.hash_partitions);
				END IF;
	
				-- index level storage clause
                	        IF p_indexes.idx_tablespace IS NOT NULL THEN
                        	       ins_line(p_type,'TABLESPACE '||p_indexes.idx_tablespace);
		                END IF;
        	       	        IF p_indexes.idx_storage IS NOT NULL THEN
                	       	        ins_line(p_type,idx_storage(p_recname, p_indexes.indexid, 
					           p_indexes.idx_storage, p_indexes.hash_partitions)); -- 6.9.2007
	                        END IF;

	                        -- 9.10.2003 - create index parallel
       		                IF l_parallel_index = 'Y' THEN
                    IF l_force_para_dop IS NULL THEN
                        ins_line(p_type,'PARALLEL');
                    ELSE
                        ins_line(p_type,'PARALLEL '||l_force_para_dop);
                    END IF;
				ELSE
					ins_line(p_type,'NOPARALLEL');
	                        END IF;

	                        -- 9.10.2003 - create index nologging, then change it to logged noparallel
       		                IF l_logging = 'N' THEN
               		                ins_line(p_type,'NOLOGGING');
                       		END IF;
	                        ins_line(p_type,'/');
        	                ins_line(p_type,'');
	
        	                IF l_logging = 'N' THEN
       	        	                ins_line(p_type,'ALTER INDEX '||
					           LOWER(l_schema||p_ind_prefix||p_indexes.indexid||p_recname
                                	           ||p_indexes.name_suffix));
	                                ins_line(p_type,'LOGGING');
       		                        ins_line(p_type,'/');
                	        END IF;
               	        	IF l_parallel_index = 'Y' THEN
	                       	       ins_line(p_type,'ALTER INDEX '||
					          LOWER(l_schema||p_ind_prefix||p_indexes.indexid||p_recname
                	                          ||p_indexes.name_suffix));
                        	       ins_line(p_type,'NOPARALLEL');
	       	                       ins_line(p_type,'/');
        	                END IF;
			END IF;
               	        ins_line(p_type,'');
                END LOOP;

		unset_action(p_action_name=>l_action);
        END mk_arch_indexes;

-------------------------------------------------------------------------------------------------------
-- generate all GLOBAL TEMPORARY indexes defined on record
-------------------------------------------------------------------------------------------------------
PROCEDURE mk_gt_indexes (p_recname VARCHAR2, p_table_name VARCHAR2, p_suffix VARCHAR2) IS
                CURSOR c_indexes (p_recname VARCHAR2) IS
                SELECT  g.indexid, g.uniqueflag, g.name_suffix
                FROM    gfc_ps_indexdefn g -- 6.9.2007 removed psindexdefn
                WHERE   g.recname = p_recname
		AND     g.platform_ora = 1 -- 20.2.2008 added
                ;
                p_indexes c_indexes%ROWTYPE;
                l_ind_def VARCHAR2(100 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'MK_GT_INDEXES');
		debug_msg('(recname='||p_recname||', table_name='||p_table_name||', suffix='||p_suffix||')');

                OPEN c_indexes(p_recname);
                LOOP
                        FETCH c_indexes INTO p_indexes;
                        EXIT WHEN c_indexes%NOTFOUND;

------------------------ index build for standard build script
			IF l_drop_index = 'Y' THEN
	                        ins_line(k_index,'DROP INDEX '||LOWER(l_schema2||'ps'||p_indexes.indexid||p_recname||p_suffix||p_indexes.name_suffix));
	                        ins_line(k_index,'/');
	                        ins_line(k_index,'');
			END IF;
                        l_ind_def := 'CREATE';
                        IF p_indexes.uniqueflag = 1 THEN
                                l_ind_def := l_ind_def||' UNIQUE';
                        END IF;
                        l_ind_def := l_ind_def||' INDEX ps'||LOWER(p_indexes.indexid||p_recname||p_suffix||p_indexes.name_suffix)
                                                   ||' ON '||LOWER(p_table_name||p_suffix);
                        ins_line(k_build,l_ind_def);
                        ins_line(k_index,l_ind_def);
                        ind_cols(k_build,p_recname,p_indexes.indexid,l_desc_index);
                        ind_cols(k_index,p_recname,p_indexes.indexid,l_desc_index);
                        ins_line(k_build,'/');
                        ins_line(k_index,'/');
                        ins_line(k_build,'');
                        ins_line(k_index,'');
                END LOOP;
                CLOSE c_indexes;

		unset_action(p_action_name=>l_action);
        END mk_gt_indexes;

-------------------------------------------------------------------------------------------------------
-- match with database
-------------------------------------------------------------------------------------------------------
	PROCEDURE match_db IS
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'MATCH_DB');

		IF l_forcebuild = 'Y' THEN
			UPDATE	gfc_ps_tables t
			SET	t.match_db = 'N'
			;
 		ELSE
			UPDATE	gfc_ps_tables t
			SET	t.match_db = 'N'
			WHERE	t.table_type = 'P'
			AND     t.match_db IS NULL
			AND	(NOT EXISTS( -- table not partitioned
					SELECT table_name 
					FROM user_part_tables o
					WHERE o.table_name = t.table_name)
			OR	NOT EXISTS( -- index not partitioned
					SELECT table_name 
					FROM user_part_indexes o
					WHERE o.table_name = t.table_name)
			OR	EXISTS(
					SELECT	otc.column_name, otc.column_id
					FROM	gfc_ora_tab_columns otc
					WHERE 	otc.table_name = t.table_name
					MINUS
					SELECT	ptc.fieldname, ptc.fieldnum
					FROM	gfc_ps_tab_columns ptc
					WHERE 	ptc.recname = t.recname)
			OR	EXISTS(
					SELECT	ptc.fieldname, ptc.fieldnum
					FROM	gfc_ps_tab_columns ptc
					WHERE 	ptc.recname = t.recname
					MINUS
					SELECT	otc.column_name, otc.column_id
					FROM	gfc_ora_tab_columns otc
					WHERE 	otc.table_name = t.table_name)
			OR 	EXISTS(
					SELECT	indexid
					FROM	psindexdefn p
					WHERE	p.recname = t.recname
					MINUS	
					SELECT	SUBSTR(i.index_name,3,1)
					FROM	user_indexes i
					WHERE	i.table_name = t.table_name)
			OR 	EXISTS(
					SELECT	SUBSTR(i.index_name,3,1)
					FROM	user_indexes i
					WHERE	i.table_name = t.table_name
					MINUS	
					SELECT	indexid
					FROM	psindexdefn p
					WHERE	p.recname = t.recname)
			);
		END IF;

		unset_action(p_action_name=>l_action);
	END match_db;

-------------------------------------------------------------------------------------------------------
-- set index tablespace to partititon tablespace
-------------------------------------------------------------------------------------------------------
	PROCEDURE set_index_tablespace(p_type NUMBER, p_recname VARCHAR2, p_part_name VARCHAR2, 
                                       p_schema VARCHAR2, p_arch_flag VARCHAR2) IS
                l_counter INTEGER := 0;
		l_module v$session.module%type;
		l_action v$session.action%type;
		l_index_prefix VARCHAR2(3);
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'SET_INDEX_TABLESPACE');

		debug_msg('SET_INDEX_TABLESPACE:recname='||p_recname
 			||'/part_name='||p_part_name
 			||'/schema='||p_schema
 			||'/arch_flag='||p_arch_flag,6);

		IF p_arch_flag = 'A' THEN
			l_index_prefix := 'ARC';
		ELSE
			l_index_prefix := 'PS';
		END IF;

		FOR p_indexes IN (	
				SELECT	i.indexid, COALESCE(r.idx_tablespace,t.idx_tablespace) idx_tablespace
				FROM	gfc_ps_indexdefn i
					LEFT OUTER JOIN gfc_part_indexes pi -- 26.5.2010 only include partitioned indexes
					ON      pi.recname = i.recname
					AND	pi.indexid = i.indexid
				,	gfc_part_tables t
				,	gfc_part_ranges r --qwert add ps defaults
				WHERE	t.recname = p_recname
				AND	r.part_name = p_part_name
				AND	i.recname = t.recname
				AND	r.part_id = t.part_id
				AND	i.platform_ora = 1 -- 1.4.2009
				AND	NVL(pi.part_type,t.part_type) != 'N'
				ORDER BY 1
				) LOOP
			debug_msg('indexid='||p_indexes.indexid
                                ||'idx_tablespace='||p_indexes.idx_tablespace,6);
			IF p_indexes.idx_tablespace IS NOT NULL THEN
				IF l_counter = 0 THEN -- 26.5.2010 added error control in case indexes are not partitioned
					whenever_sqlerror(p_type,FALSE);
				END IF;
				ins_line(p_type,'ALTER INDEX '||LOWER(p_schema||'.'||l_index_prefix||p_indexes.indexid||p_recname)||
			                ' MODIFY DEFAULT ATTRIBUTES TABLESPACE '||LOWER(p_indexes.idx_tablespace));
				ins_line(p_type,'/');
				l_counter := l_counter + 1;
			END IF;
		END LOOP;
		IF l_counter > 0 THEN
			whenever_sqlerror(p_type,TRUE);
			ins_line(p_type,'');
		END IF;

		unset_action(p_action_name=>l_action);
	END set_index_tablespace;

-------------------------------------------------------------------------------------------------------
-- set index tablespace to partititon tablespace
-------------------------------------------------------------------------------------------------------
	PROCEDURE unset_index_tablespace(p_type NUMBER, p_recname VARCHAR2, p_schema VARCHAR2, p_arch_flag VARCHAR2) IS
                l_counter INTEGER := 0;
		l_module v$session.module%type;
		l_action v$session.action%type;
		l_index_prefix VARCHAR2(3);
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'UNSET_INDEX_TABLESPACE');

		IF p_arch_flag = 'A' THEN
			l_index_prefix := 'ARC';
		ELSE
			l_index_prefix := 'PS';
		END IF;

		FOR p_indexes IN (	
				SELECT	i.indexid, t.idx_tablespace
				FROM	gfc_ps_indexdefn i
				,	gfc_part_tables t --qwert add ps defaults
				,	gfc_part_indexes pi -- 26.5.2010 only include partitioned indexes
				WHERE	t.recname = p_recname
				AND	i.recname = p_recname
				AND	i.platform_ora = 1 -- 1.4.2009
				AND	pi.recname = p_recname
				AND	pi.indexid = i.indexid
				AND	pi.part_type != 'N'
				ORDER BY 1
				) LOOP
			IF p_indexes.idx_tablespace IS NOT NULL THEN
				IF l_counter = 0 THEN
					whenever_sqlerror(p_type,FALSE);
				END IF;
				ins_line(p_type,'ALTER INDEX '||LOWER(p_schema||'.'||l_index_prefix||p_indexes.indexid||p_recname)||
			                ' MODIFY DEFAULT ATTRIBUTES TABLESPACE '||LOWER(p_indexes.idx_tablespace));
				ins_line(p_type,'/');
				l_counter := l_counter + 1;
			END IF;
		END LOOP;
		IF l_counter > 0 THEN
			whenever_sqlerror(p_type,FALSE);
			ins_line(p_type,'');
		END IF;

		unset_action(p_action_name=>l_action);
	END unset_index_tablespace;

-------------------------------------------------------------------------------------------------------
PROCEDURE subpart_update_indexes(p_type           NUMBER
                                ,p_recname        VARCHAR2
	                        ,p_part_name      VARCHAR2
	                        ,p_subpart_name   VARCHAR2
                                ,p_idx_tablespace VARCHAR2
	                        ,p_idx_storage    VARCHAR2
	                        ) IS
		l_sep     VARCHAR2(20 CHAR);
		l_closure VARCHAR2(2 CHAR);

		CURSOR c_ind_subparts(p_recname VARCHAR2) IS
		SELECT	i.*
		FROM	gfc_ps_indexdefn i
		WHERE	i.recname = p_recname
		AND	i.platform_ora = 1
		AND NOT EXISTS(
			SELECT 	'x'
			FROM	gfc_part_indexes p
			WHERE	p.recname = i.recname
			AND	p.indexid = i.indexid)
		ORDER BY i.indexid
		;

                p_ind_subparts c_ind_subparts%ROWTYPE;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'SUBPART_UPDATE_INDEXES');

                --this procedure is used to add table partitions - no need to control UPDATE INDEXES clause
		l_sep := 'UPDATE INDEXES (';
		l_closure := '';
		OPEN c_ind_subparts(p_recname);
		LOOP
       	                FETCH c_ind_subparts INTO p_ind_subparts;
                        EXIT WHEN c_ind_subparts%NOTFOUND;
			l_closure := '))';

                        ins_line(p_type,l_sep||LOWER('PS'||p_ind_subparts.indexid||p_recname)
                                             ||' (SUBPARTITION '
                                             ||LOWER(p_recname||p_ind_subparts.indexid||p_part_name||'_'||p_subpart_name)
                                        );
                        IF p_idx_tablespace IS NOT NULL THEN
       	                        ins_line(p_type,'TABLESPACE '||LOWER(p_idx_tablespace));
               	        END IF;
                        -------------------------------------------------------------------------------------------------------
                        --removed 10.3.2022 - ORA-14189: this physical attribute may not be specified for an index subpartition
                       	--IF p_idx_storage IS NOT NULL THEN
                        --        ins_line(p_type,idx_storage(p_recname, p_ind_subparts.indexid,p_idx_storage));
       	                --END IF;
                        -------------------------------------------------------------------------------------------------------
			l_sep := '),';
		END LOOP;
		CLOSE c_ind_subparts;

		IF l_closure IS NOT NULL THEN
                        ins_line(p_type,l_closure);
		END IF;

		unset_action(p_action_name=>l_action);
	END subpart_update_indexes;

-------------------------------------------------------------------------------------------------------
-- create table with which to do partition exchange in order to add partitions and replace max value partition
-- generate partition clause on the basis of part ranges table - added 11.3.2013
-- 31.7.2017 added copy for subpartitioned tables
-------------------------------------------------------------------------------------------------------
PROCEDURE create_partex
(p_type         NUMBER
,p_recname      VARCHAR2
,p_table_name   VARCHAR2 DEFAULT NULL
,p_create_table IN OUT BOOLEAN 
) IS
  l_default_partition_name VARCHAR2(30 CHAR) := '';
  l_table_name             psrecdefn.sqltablename%type;
  l_tab_tablespace         gfc_part_tables.tab_tablespace%type;
  l_tab_storage            gfc_part_tables.tab_storage%type;
  l_idx_tablespace         gfc_part_tables.idx_tablespace%type;
  l_idx_storage            gfc_part_tables.idx_storage%type;
  l_part_id                gfc_part_tables.part_id%type;
  l_subpart_type           gfc_part_tables.subpart_type%type;
  l_schema                 VARCHAR2(31 CHAR);
  l_ind_def                VARCHAR2(100 CHAR);
  l_hint                   VARCHAR2(100 CHAR);
  l_hint2                  VARCHAR2(100 CHAR);
  l_module                 v$session.module%type;
  l_action                 v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'CREATE_SUBPARTEX');

  SELECT p.partition_name
  ,     pt.subpart_type 
  ,     DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
  ,	NVL(pr.tab_tablespace,pt.tab_tablespace) tab_tablespace
  ,	NVL(pr.tab_storage,pt.tab_storage)       tab_storage
  ,	NVL(pr.idx_tablespace,pt.idx_tablespace) idx_tablespace
  ,	NVL(pr.idx_storage,pt.idx_storage)       idx_storage
  INTO	l_default_partition_name
  ,     l_subpart_type 
  ,     l_table_name
  ,	l_tab_tablespace
  ,	l_tab_storage
  ,	l_idx_tablespace
  ,	l_idx_storage
  FROM	psrecdefn r
  ,     gfc_part_tables pt
  ,	gfc_part_ranges pr
  ,	user_tab_partitions p
  WHERE	r.recname            = p_recname
  AND   r.rectype            = 0
  AND   pt.recname           = r.recname
  AND   pt.part_type         = 'R'
  AND	pr.part_id           = pt.part_id
  AND   pr.part_no           = ( -- get last range
           SELECT MAX(pr1.part_no)
           FROM   gfc_part_ranges pr1
           WHERE  pr1.part_id = pt.part_id)
  AND	UPPER(pr.part_value) LIKE '%MAXVALUE%'
  AND	p.table_name         = NVL(p_table_name,DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename))
  AND   p.partition_name     = r.recname||'_'||pr.part_name
  UNION ALL
  SELECT p.partition_name
  ,     pt.subpart_type 
  ,     DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) 
  ,	NVL(pl.tab_tablespace,pt.tab_tablespace)
  ,	NVL(pl.tab_storage,pt.tab_storage)
  ,	NVL(pl.idx_tablespace,pt.idx_tablespace)
  ,	NVL(pl.idx_storage,pt.idx_storage)
  FROM	psrecdefn r
  ,     gfc_part_tables pt
  ,	gfc_part_lists pl
  ,	user_tab_partitions p
  WHERE	r.recname            = p_recname
  AND   r.rectype            = 0
  AND   pt.recname           = r.recname
  AND   pt.part_type         = 'L'
  AND	pl.part_id           = pt.part_id
  AND	UPPER(pl.list_value) = 'DEFAULT'
  AND	p.table_name         = NVL(p_table_name,DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename))
  AND   p.partition_name     = r.recname||'_'||pl.part_name
  ;
		
  IF l_default_partition_name IS NOT NULL THEN
    IF p_create_table THEN

      -- create table to be used for partition exchange
      ins_line(p_type,'CREATE TABLE '||LOWER(l_schema2||'gfc_'||p_recname));
      tab_cols(p_type=>p_type
              ,p_recname=>p_recname
              ,p_longtoclob=>l_longtoclob
              ,p_table_name=>NVL(p_table_name,l_table_name));

      IF l_tab_tablespace IS NOT NULL THEN
        ins_line(p_type,'TABLESPACE '||l_tab_tablespace);
      END IF;

      IF l_tab_storage IS NOT NULL THEN
        ins_line(p_type,tab_storage(p_recname, l_tab_storage)); 
      END IF;
      ins_line(p_type,'/');
      ins_line(p_type,'');

      -- create indexes
      FOR p_indexes IN(
        SELECT   g.indexid, g.uniqueflag, g.platform_ora
	,        i.override_schema
        FROM     gfc_ps_indexdefn g
	  LEFT OUTER JOIN gfc_part_indexes i
          ON i.recname = g.recname
	  AND i.indexid = g.indexid
        WHERE    g.recname = p_recname
        AND      g.platform_ora = 1
        AND      i.part_id IS NULL -- added 13.3.2013 to suppress build of global indexes
        AND     (l_subpart_type = 'N' OR g.uniqueflag = 1)
        ORDER BY g.indexid)
      LOOP
        IF l_explicit_schema = 'Y' THEN
          l_schema := NVL(p_indexes.override_schema,LOWER(l_schema1))||'.';
        ELSE 
          l_schema := '';
        END IF;
        
        l_ind_def := 'CREATE';
        IF p_indexes.uniqueflag = 1 THEN
          l_ind_def := l_ind_def||' UNIQUE';
        END IF;
        l_ind_def := l_ind_def||' INDEX '||LOWER(l_schema||'gfc'||p_indexes.indexid||p_recname)||
                                   ' ON '||LOWER(l_schema||'gfc_'||p_recname);
        ins_line(p_type,l_ind_def);
        ind_cols(p_type,p_recname,p_indexes.indexid,l_desc_index);

        -- index level storage clause
        IF l_idx_tablespace IS NOT NULL THEN
          ins_line(p_type,'TABLESPACE '||l_idx_tablespace);
        END IF;
        IF l_idx_storage IS NOT NULL THEN
          ins_line(p_type,idx_storage(p_recname, p_indexes.indexid,l_idx_storage));
        END IF;
        ins_line(p_type,'/');
        ins_line(p_type,'');
                                
      END LOOP;
      p_create_table := FALSE;
    END IF;

    -- exchange
    IF l_subpart_type = 'N' THEN
      ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
      ins_line(p_type,'EXCHANGE PARTITION '||LOWER(l_default_partition_name));
      ins_line(p_type,'WITH TABLE '||LOWER(l_schema||'gfc_'||p_recname));
      ins_line(p_type,'INCLUDING INDEXES WITH VALIDATION UPDATE GLOBAL INDEXES');
      ins_line(p_type,'/');
      ins_line(p_type,'');
    ELSE /*31.7.2017 if any subpartitioning copy the table*/
      l_hint := 'APPEND';
      IF l_logging = 'N' THEN
        l_hint := l_hint||' NOLOGGING';
      END IF;

      IF l_parallel_table = 'Y' THEN 
        l_hint  := l_hint||' PARALLEL(T)';
        l_hint2 :=          'PARALLEL(S)';
      ELSE
        l_hint2 := '';
      END IF;

      IF l_hint IS NOT NULL THEN
        l_hint := ' /*+'||l_hint||'*/';
      END IF;
      IF l_hint2 IS NOT NULL THEN
        l_hint2 := ' /*+'||l_hint2||'*/';
      END IF;

      ins_line(p_type,'INSERT'||l_hint||' INTO '||LOWER(l_schema||'gfc_'||p_recname)||' t'
                   ||' SELECT'||l_hint2||' * FROM '||LOWER(l_schema2||p_table_name)||' PARTITION ('||LOWER(l_default_partition_name)||') s');
      ins_line(p_type,'/');
      ins_line(p_type,'COMMIT');
      ins_line(p_type,'/');
      ins_line(p_type,'');
    END IF;

    -- drop default
    ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
    ins_line(p_type,'DROP PARTITION '||LOWER(l_default_partition_name));
    ins_line(p_type,'UPDATE GLOBAL INDEXES');
    ins_line(p_type,'/');
    ins_line(p_type,'');
  END IF;

  unset_action(p_action_name=>l_action);

EXCEPTION
  WHEN no_data_found THEN unset_action(p_action_name=>l_action);

END create_partex;
-------------------------------------------------------------------------------------------------------
-- generate partition clause on the basis of part lists table
-- wibble - need to add range support here for 11g
-------------------------------------------------------------------------------------------------------
PROCEDURE create_subpartex
(p_type         NUMBER
,p_recname      VARCHAR2
,p_table_name   VARCHAR2
,p_part_name    VARCHAR2
,p_create_table IN OUT BOOLEAN 
) IS
  l_default_subpartition_name VARCHAR2(30 CHAR) := '';
  l_tab_tablespace VARCHAR2(30 CHAR);
  l_tab_storage VARCHAR2(100 CHAR);
  l_idx_tablespace VARCHAR2(30 CHAR);
  l_idx_storage VARCHAR2(100 CHAR);
  l_schema VARCHAR2(31 CHAR);
  l_ind_def VARCHAR2(100 CHAR);
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'CREATE_SUBPARTEX');

  SELECT s.subpartition_name
  ,      pt.tab_tablespace
  ,      pt.tab_storage
  ,      pt.idx_tablespace
  ,      pt.idx_storage
  INTO   l_default_subpartition_name
  ,      l_tab_tablespace
  ,      l_tab_storage
  ,      l_idx_tablespace
  ,      l_idx_storage
  FROM   gfc_part_tables pt
  ,      gfc_part_lists pl
  ,      user_tab_subpartitions s
  WHERE  pt.recname           = p_recname
  AND    pl.part_id           = pt.part_id
  AND    s.table_name         = p_table_name
  AND    s.partition_name     = p_recname||'_'||p_part_name
  AND    s.subpartition_name  = p_recname||'_'||p_part_name||'_'||pl.part_name
  AND    pt.subpart_Type = 'L'
  AND    UPPER(pl.list_value) = 'DEFAULT'
  ;
		
  IF l_default_subpartition_name IS NOT NULL THEN
    IF p_create_table THEN
      -- create table
      ins_line(p_type,'CREATE TABLE '||LOWER(l_schema||'gfc_'||p_recname));
      tab_cols(p_type,p_recname,l_longtoclob);

      IF l_tab_tablespace IS NOT NULL THEN
        ins_line(p_type,'TABLESPACE '||l_tab_tablespace);
      END IF;

      IF l_tab_storage IS NOT NULL THEN
        ins_line(p_type,tab_storage(p_recname, l_tab_storage)); 
      END IF;
      ins_line(p_type,'/');
      ins_line(p_type,'');

      -- create indexes
      FOR p_indexes IN(
        SELECT   g.indexid, g.uniqueflag, g.platform_ora
	,        i.override_schema
        FROM     gfc_ps_indexdefn g
	  LEFT OUTER JOIN gfc_part_indexes i
          ON i.recname = g.recname
	  AND i.indexid = g.indexid
        WHERE    g.recname = p_recname
        AND      g.platform_ora = 1
        ORDER BY g.indexid)
      LOOP
        IF l_explicit_schema = 'Y' THEN
          l_schema := NVL(p_indexes.override_schema,LOWER(l_schema1))||'.';
        ELSE 
          l_schema := '';
        END IF;
        
        l_ind_def := 'CREATE';
        IF p_indexes.uniqueflag = 1 THEN
          l_ind_def := l_ind_def||' UNIQUE';
        END IF;
        l_ind_def := l_ind_def||' INDEX '||LOWER(l_schema||'gfc'||p_indexes.indexid||p_recname)||
                                   ' ON '||LOWER(l_schema||'gfc_'||p_recname);
        ins_line(p_type,l_ind_def);
        ind_cols(p_type,p_recname,p_indexes.indexid,l_desc_index);

        -- index level storage clause
        IF l_idx_tablespace IS NOT NULL THEN
          ins_line(p_type,'TABLESPACE '||l_idx_tablespace);
        END IF;
        IF l_idx_storage IS NOT NULL THEN
          ins_line(p_type,idx_storage(p_recname, p_indexes.indexid,l_idx_storage));
        END IF;
        ins_line(p_type,'/');
        ins_line(p_type,'');
                                
      END LOOP;
      p_create_table := FALSE;
    END IF;

    -- exchange
    ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
    ins_line(p_type,'EXCHANGE SUBPARTITION '||LOWER(l_default_subpartition_name));
    ins_line(p_type,'WITH TABLE '||LOWER(l_schema||'gfc_'||p_recname));
    ins_line(p_type,'INCLUDING INDEXES WITH VALIDATION UPDATE GLOBAL INDEXES');
    ins_line(p_type,'/');
    ins_line(p_type,'');

    -- drop default
    ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
    ins_line(p_type,'DROP SUBPARTITION '||LOWER(l_default_subpartition_name));
    ins_line(p_type,'/');
    ins_line(p_type,'');
  END IF;

  unset_action(p_action_name=>l_action);

EXCEPTION
  WHEN no_data_found THEN unset_action(p_action_name=>l_action);

END create_subpartex;
-------------------------------------------------------------------------------------------------------
-- generate partition clause on the basis of part ranges table
-------------------------------------------------------------------------------------------------------
PROCEDURE drop_subpartex
(p_type       NUMBER
,p_recname    VARCHAR2
,p_table_name VARCHAR2
,p_part_name  VARCHAR2
,p_drop_table BOOLEAN 
) IS
  l_subpart_name   VARCHAR2(30 CHAR);
  l_tab_tablespace VARCHAR2(30 CHAR);
  l_tab_storage    VARCHAR2(100 CHAR);
  l_idx_tablespace VARCHAR2(30 CHAR);
  l_idx_storage    VARCHAR2(100 CHAR);
  l_schema         VARCHAR2(31 CHAR);
  l_module         v$session.module%type;
  l_action         v$session.action%type;
  l_hint           VARCHAR2(100 CHAR);
  l_hint2          VARCHAR2(100 CHAR);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'DROP_SUBPARTEX');

  -- 18.2.2011 - hint logic moved inside loop here
  l_hint := 'APPEND';
  IF l_logging = 'N' THEN
     l_hint := l_hint||' NOLOGGING';
  END IF;

  -- 2.10.2010 - make copy go parallel, moved head 16.1.2011
  IF l_parallel_table = 'Y' THEN -- 16.1.2011 removed alter and used 2 hints
    l_hint  := l_hint||' PARALLEL(T)';
    l_hint2 :=          'PARALLEL(S)';
  ELSE
    l_hint2 := '';
  END IF;

  IF l_hint IS NOT NULL THEN
    l_hint := ' /*+'||l_hint||'*/';
  END IF;
  IF l_hint2 IS NOT NULL THEN
    l_hint2 := ' /*+'||l_hint2||'*/';
  END IF;

                SELECT	pl.part_name 
		,	pl.tab_tablespace
		,	pl.tab_storage
		,	pl.idx_tablespace
		,	pl.idx_storage
		INTO	l_subpart_name
		,	l_tab_tablespace
		,	l_tab_storage
		,	l_idx_tablespace
		,	l_idx_storage
		FROM	gfc_part_tables pt
		,	gfc_part_lists pl
		,	user_tab_subpartitions s
		WHERE	pt.recname = p_recname
		AND	pl.part_id = pt.part_id
		AND	UPPER(pl.list_value) = 'DEFAULT'
		AND 	s.table_name         = p_table_name
		AND	s.partition_name     = p_recname||'_'||p_part_name
		AND	s.subpartition_name  = p_recname||'_'||p_part_name||'_'||pl.part_name
		;

		
		IF l_subpart_name IS NOT NULL THEN
			-- add default partiton
			ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
			ins_line(p_type,'MODIFY PARTITION '||LOWER(p_recname||'_'||p_part_name)
			             ||' ADD SUBPARTITION '||LOWER(p_recname||'_'||p_part_name||'_'||l_subpart_name));
			ins_line(p_type,'VALUES (DEFAULT)');
			subpart_update_indexes(p_type, p_recname, p_part_name
                                              ,l_subpart_name
			                      ,l_idx_tablespace
			                      ,l_idx_storage);
                        ins_line(p_type,'/');
       	                ins_line(p_type,'');
	

			-- exchange default partiton back into part table
			IF l_repopdfltsub = 'N' THEN -- added 29.3.2011 
	                        ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
	       	                ins_line(p_type,'EXCHANGE SUBPARTITION '
					||LOWER(p_recname||'_'||p_part_name||'_'||l_subpart_name));
                       		ins_line(p_type,'WITH TABLE '||LOWER(l_schema||'gfc_'||p_recname));
                	        ins_line(p_type,'INCLUDING INDEXES WITH VALIDATION UPDATE GLOBAL INDEXES');
        	                ins_line(p_type,'/');
	       	                ins_line(p_type,'');
			ELSE -- added 29.3.2011 optionally force repopulation of default partition
                        	ins_line(p_type,'INSERT'||l_hint||' INTO '||LOWER(l_schema2||p_table_name)||' t');
                        	ins_line(p_type,'SELECT'||l_hint2||' * FROM '||LOWER(l_schema||'gfc_'||p_recname)||' s');
                        	ins_line(p_type,'/');
        	                ins_line(p_type,'TRUNCATE TABLE '||LOWER(l_schema||'gfc_'||p_recname));
        	                ins_line(p_type,'/');
	       	                ins_line(p_type,'');
			END IF;

			IF p_drop_table THEN
				-- drop partex table
        	                ins_line(p_type,'DROP TABLE '||LOWER(l_schema||'gfc_'||p_recname)||l_drop_purge_suffix);
                	        ins_line(p_type,'/');
       	                	ins_line(p_type,'');
			END IF;

  END IF;
  unset_action(p_action_name=>l_action);

EXCEPTION
  WHEN no_data_found THEN unset_action(p_action_name=>l_action);


END drop_subpartex;
-------------------------------------------------------------------------------------------------------
-- generate partition clause on the basis of part ranges table
-- 23.1.2014 added check that parent partition exists, otherwise subpartitions will be added with partition
-------------------------------------------------------------------------------------------------------
PROCEDURE add_tab_subparts
(p_type NUMBER
, p_recname VARCHAR2
,p_table_owner  VARCHAR2
,p_table_name   VARCHAR2
,p_part_id      VARCHAR2
,p_part_type    VARCHAR2
,p_subpart_type VARCHAR2
,p_part_name    VARCHAR2 DEFAULT ''
,p_arch_flag    VARCHAR2 DEFAULT 'N' -- added 15.3.2013
) IS
  CURSOR c_tab_subparts(p_table_name VARCHAR2
		       ,p_recname    VARCHAR2) IS
  SELECT pr.part_no part_no
  ,      pr.part_name part_name
  ,      pl.PART_NO subpart_no       
  ,      pl.PART_NAME subpart_name
  ,      pl.LIST_VALUE     
  ,      pl.TAB_TABLESPACE
  ,      pl.IDX_TABLESPACE
  ,      pl.TAB_STORAGE    
  ,      pl.IDX_STORAGE    
  FROM   gfc_part_ranges pr
  ,	   gfc_part_lists pl
  ,      gfc_part_subparts prl
  ,      all_tab_partitions tp --added 23.1.2014
  WHERE  pr.part_id = p_part_id
  AND    pl.part_id = p_part_id
  AND    (pr.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
  AND    (pl.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
  AND    prl.part_id = p_part_id
  AND    prl.part_name = pr.part_name
  AND    prl.subpart_name = pl.part_name
  AND    prl.build = 'Y'
  AND    tp.table_owner = UPPER(p_table_owner)
  AND    tp.table_name = UPPER(p_table_name)
  AND    tp.partition_name = UPPER(p_recname||'_'||pr.part_name)
  AND NOT EXISTS(
         SELECT 'x'
         FROM   all_tab_subpartitions ts
         WHERE  ts.table_owner = tp.table_owner
         AND    ts.table_name = tp.table_name
         AND    ts.partition_name = tp.partition_name
         AND    ts.subpartition_name = UPPER(p_part_name||'_'||pr.part_name||'_'||pl.part_name))
  ORDER BY pr.part_no, pl.part_no
  ;

  p_tab_subparts c_tab_subparts%ROWTYPE;

  l_last_part_name VARCHAR2(30 CHAR) := '';
  l_create_flag BOOLEAN := TRUE;
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'ADD_TAB_SUBPARTS');

  OPEN c_tab_subparts(p_table_name, p_recname);
  LOOP
    FETCH c_tab_subparts INTO p_tab_subparts;
    EXIT WHEN c_tab_subparts%NOTFOUND;

    IF l_last_part_name IS NULL OR 
      l_last_part_name != p_tab_subparts.part_name THEN
      IF l_last_part_name IS NOT NULL THEN
	 drop_subpartex(p_type
                       ,p_recname
                       ,p_table_name
                       ,l_last_part_name
                       ,FALSE);
      END IF;
      l_last_part_name := p_tab_subparts.part_name;
      create_subpartex(p_type
	              ,p_recname
                      ,p_table_name
                      ,p_tab_subparts.part_name
                      ,l_create_flag);
    END IF;

    ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
    ins_line(p_type,'MODIFY PARTITION '||
                  LOWER(p_recname||'_'||p_tab_subparts.part_name)||
                  ' ADD SUBPARTITION '||
                  LOWER(p_recname||'_'||p_tab_subparts.part_name||'_'||p_tab_subparts.subpart_name));

    IF p_subpart_type = 'L' THEN -- list
     ins_line(p_type,'VALUES ('||p_tab_subparts.list_value||')');
    END IF;

    IF p_tab_subparts.tab_tablespace IS NOT NULL THEN
      ins_line(p_type,'TABLESPACE '||LOWER(p_tab_subparts.tab_tablespace));
    END IF;

--07.03.2013-no storage options on subpartitions
--  IF p_tab_subparts.tab_storage IS NOT NULL THEN
--    ins_line(p_type,tab_storage(p_recname, p_tab_subparts.tab_storage));
--  END IF;

    subpart_update_indexes(p_type, p_recname
                          ,p_tab_subparts.part_name
                          ,p_tab_subparts.subpart_name
                          ,p_tab_subparts.idx_tablespace
                          ,p_tab_subparts.idx_storage);

    ins_line(p_type,'/');
    ins_line(p_type,'');

---qwert--- could consider adding move rows for this list partition from exchange table 
  END LOOP;
  CLOSE c_tab_subparts;

  IF l_last_part_name IS NOT NULL THEN
    drop_subpartex(p_type
                  ,p_recname
                  ,p_table_name
                  ,l_last_part_name
                  ,TRUE);
  END IF;

  unset_action(p_action_name=>l_action);
END add_tab_subparts;

-------------------------------------------------------------------------------------------------------
-- generate partition clause on the basis of part ranges table
-------------------------------------------------------------------------------------------------------
PROCEDURE add_tab_parts
(p_type             NUMBER
,p_recname          VARCHAR2
,p_table_owner      VARCHAR2
,p_table_name       VARCHAR2
,p_part_id          VARCHAR2
,p_part_type        VARCHAR2
,p_subpart_type     VARCHAR2
,p_subpartitions    INTEGER
,p_arch_flag        VARCHAR2 DEFAULT 'N'
,p_part_name        VARCHAR2 DEFAULT ''
) IS
  l_part_def CLOB;
  l_counter INTEGER := 0;
  l_subpartition INTEGER;
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'ADD_TAB_PARTS');

  debug_msg('ADD_TAB_PARTS:recname='||p_recname
                                    ||'/table='||p_table_owner||'.'||p_table_name
                                    ||'/part_id='||p_part_id
                                    ||'/part_type='||p_part_type
                                    ||'/subpart_type='||p_subpart_type
                                    ||'/subpartitions='||p_subpartitions
                                    ||'/arch_flag='||p_arch_flag
                                    ||'/part_name='||p_part_name,6);

  FOR p_tab_parts IN(
    SELECT pr.*
    FROM   gfc_part_ranges pr
    ,      dba_tables t
    WHERE  t.owner = UPPER(p_table_owner)
    AND    t.table_name = UPPER(p_table_name)
    AND    t.partitioned = 'YES'
    AND    pr.part_id = p_part_id
    AND    (pr.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
    AND NOT EXISTS( /*partition does not already exist*/
      SELECT 'x'
      FROM   dba_tab_partitions tp
      WHERE  tp.table_owner = t.owner
      AND    tp.table_name = t.table_name 
      AND    tp.partition_name = p_part_name||'_'||pr.part_name
      )
    AND NOT EXISTS( /*higher partition does not exist*/ 
      SELECT 'x'
      FROM   gfc_part_ranges pr1
      ,      dba_tab_partitions tp1 /*enhancement 23.4.2010*/
      WHERE  pr1.part_id = pr.part_id
      AND    pr1.part_no > pr.part_no
      AND    tp1.table_owner = t.owner
      AND    tp1.table_name = t.table_name
      AND    tp1.partition_name = p_part_name||'_'||pr1.part_name
      )
    AND NOT EXISTS( /*31.7.2017 a maxvalue partition does not exist in metadata*/
      SELECT 'x'
      FROM   gfc_part_ranges pr2
      WHERE  pr2.part_id = pr.part_id
      AND    pr2.part_no > pr.part_no
      AND    UPPER(pr2.part_value) LIKE '%MAXVALUE%'
      )
    ORDER BY part_no, part_name
  ) LOOP
    debug_msg('Part Range:'||p_tab_parts.part_No||'/'||p_tab_parts.part_name,6);
    l_counter := l_counter + 1;

    -- alter indexes to set tablespace to partition tablespace
    set_index_tablespace(p_type, p_recname, p_tab_parts.part_name, p_table_owner, p_arch_flag);

    ins_line(p_type,'ALTER TABLE '||LOWER(p_table_owner||'.'||p_table_name)
                                  ||' ADD PARTITION '||LOWER(p_part_name||'_'||p_tab_parts.part_name));
    IF p_part_type = 'R' THEN
      ins_line(p_type,'VALUES LESS THAN ('||p_tab_parts.part_value||')');
    ELSIF p_part_type = 'L' THEN -- list
      ins_line(p_type,'VALUES ('||p_tab_parts.part_value||')');
    END IF;

    IF p_tab_parts.tab_tablespace IS NOT NULL THEN
      ins_line(p_type,'TABLESPACE '||p_tab_parts.tab_tablespace);
    END IF;

    IF p_tab_parts.tab_storage IS NOT NULL THEN
      ins_line(p_type,tab_storage(p_recname, p_tab_parts.tab_storage)); -- 6.9.2007
    END IF;

    IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
      FOR l_subpartition IN 1..p_subpartitions LOOP
        IF l_subpartition = 1 THEN
          l_part_def := '(';
        ELSE
          l_part_def := ',';
        END IF;
        l_part_def := l_part_def||'SUBPARTITION '||LOWER(p_part_name||'_'
                                ||p_tab_parts.part_name||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
        ins_line(p_type,l_part_def);
      END LOOP;
      ins_line(p_type,')');
    ELSIF p_subpart_type = 'R' THEN
      tab_rangesubparts(p_type, p_recname
                   ,p_tab_parts.part_id
                   ,p_tab_parts.part_name
                   ,p_part_name);
    ELSIF p_subpart_type = 'L' THEN
      tab_listsubparts(p_type, p_recname
                   ,p_tab_parts.part_id
                   ,p_tab_parts.part_name
                   ,p_part_name);
    END IF;
    ins_line(p_type,'/');
    ins_line(p_type,'');
  END LOOP;

  IF l_counter > 0 THEN
    -- reset index tablespace at table level
    unset_index_tablespace(p_type, p_recname, p_table_owner, p_arch_flag);
    -- ins_line(p_type,'spool off');
  END IF;

  unset_action(p_action_name=>l_action);
END add_tab_parts;

-------------------------------------------------------------------------------------------------------
-- 11.3.2013 add procdure to exchange maxvalue range partition out
-------------------------------------------------------------------------------------------------------
PROCEDURE add_tab_parts_newmax
(p_type             NUMBER
,p_recname          VARCHAR2
,p_table_owner      VARCHAR2
,p_table_name       VARCHAR2
,p_part_id          VARCHAR2
,p_part_type        VARCHAR2
,p_subpart_type     VARCHAR2
,p_subpartitions    INTEGER
,p_arch_flag        VARCHAR2 DEFAULT 'N'
,p_part_name        VARCHAR2 DEFAULT ''
) IS
  l_part_def     VARCHAR2(200 CHAR);
  l_counter      INTEGER := 0;
  l_subpartition INTEGER;
  l_create_table BOOLEAN := TRUE; -- set this flag true just once
  l_hint         VARCHAR2(100 CHAR);
  l_hint2        VARCHAR2(100 CHAR);

  l_part_no         NUMBER;
  l_part_name       VARCHAR2(30);
  l_part_value      VARCHAR2(100);
  l_tab_tablespace  VARCHAR2(30);
  l_idx_tablespace  VARCHAR2(30);
  l_tab_storage     VARCHAR2(100);
  l_idx_storage     VARCHAR2(100);

  l_module       v$session.module%type;
  l_action       v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'ADD_TAB_PARTS_NEWMAX');

  debug_msg('ADD_TAB_PARTS_NEWMAX'
          ||':recname='      ||p_recname
          ||'/table='        ||p_table_owner||'.'||p_table_name
          ||'/part_id='      ||p_part_id
          ||'/part_type='    ||p_part_type
          ||'/subpart_type=' ||p_subpart_type
          ||'/subpartitions='||p_subpartitions
          ||'/arch_flag='    ||p_arch_flag
          ||'/part_name='    ||p_part_name,6);

  BEGIN
    -- get details of maxvalue partition
    SELECT  r.part_no
    ,       r.part_name
    ,       r.part_value
    ,       r.tab_tablespace
    ,       r.idx_tablespace
    ,       r.tab_storage
    ,       r.idx_storage 
    INTO    l_part_no         
    ,       l_part_name       
    ,       l_part_value      
    ,       l_tab_tablespace  
    ,       l_idx_tablespace  
    ,       l_tab_storage     
    ,       l_idx_storage     
    FROM    gfc_part_ranges r
    WHERE   r.part_id = p_part_id
    AND     r.part_no = ( -- get the maxvalue partition
              SELECT MAX(r1.part_no)
              FROM   gfc_part_ranges r1
              WHERE  r1.part_id = p_part_id)
    AND     UPPER(r.part_value) LIKE '%MAXVALUE%';

    debug_msg('maxvalue='||l_part_no||','||l_part_name||','||l_part_value,6);

    FOR p_tab_parts IN(
      SELECT pr.*
      FROM   gfc_part_tables pt
      ,      gfc_part_ranges pr
      ,      dba_tables t
      WHERE  t.owner = UPPER(p_table_owner)
      AND    t.table_name = UPPER(p_table_name)
      AND    t.partitioned = 'YES'
      AND    pt.recname = p_recname
      AND    pt.part_type = 'R' -- range partitioned only
--    AND    pt.subpart_type = 'N' --exclude subpartitioned tables --31.7.2017 permit subpartitioned tables again
      AND    pt.part_id = p_part_id
      AND    pr.part_id = pt.part_id
      AND    (pr.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
      AND    pr.part_no < l_part_no
      AND NOT EXISTS( -- this partition
        SELECT 'x'
        FROM   dba_tab_partitions tp
        WHERE  tp.table_owner = t.owner
        AND    tp.table_name = t.table_name 
        AND    tp.partition_name = p_part_name||'_'||pr.part_name
        )
      AND NOT EXISTS( -- any higher partition other than maxvalue partition
        SELECT 'x'
        FROM   gfc_part_ranges pr1
        ,      dba_tab_partitions tp1 
        WHERE  pr1.part_id = pr.part_id
        AND    pr1.part_no > pr.part_no
        AND    pr1.part_no < l_part_no -- but excluding the maxvalue partition
        AND    tp1.table_owner = t.owner
        AND    tp1.table_name = t.table_name
        AND    tp1.partition_name = p_part_name||'_'||pr1.part_name
        )
      AND EXISTS( /*this is not the maxvalue partition*/
        SELECT 'x'
        FROM   gfc_part_ranges pr2
        ,      dba_tab_partitions tp2
        WHERE  pr2.part_id = pr.part_id
        AND    pr2.part_no = l_part_no -- the maxvalue partition
        AND    tp2.table_owner = t.owner
        AND    tp2.table_name = t.table_name
        AND    tp2.partition_name = p_part_name||'_'||pr2.part_name
        )
      ORDER BY pr.part_no, pr.part_name
    ) LOOP
      debug_msg('Part Range:'||p_tab_parts.part_No||'/'||p_tab_parts.part_name,6);
      l_counter := l_counter + 1;
  
      IF l_counter = 1 THEN
        -- create a part exchange table first time round only because l_create_tables is set to false in package
        create_partex(p_type=>p_type, p_recname=>p_recname, p_table_name=>p_table_name, p_create_table=>l_create_table);
      END IF;
  
      -- alter indexes to set tablespace to partition tablespace
      set_index_tablespace(p_type, p_recname, p_tab_parts.part_name, p_table_owner, p_arch_flag);

      ins_line(p_type,'ALTER TABLE '||LOWER(p_table_owner||'.'||p_table_name)
                                    ||' ADD PARTITION '||LOWER(p_part_name||'_'||p_tab_parts.part_name));
--    IF p_part_type = 'R' THEN
        ins_line(p_type,'VALUES LESS THAN ('||p_tab_parts.part_value||')');
--    ELSIF p_part_type = 'L' THEN -- list
--      ins_line(p_type,'VALUES ('||p_tab_parts.part_value||')');
--    END IF;

      IF p_tab_parts.tab_tablespace IS NOT NULL THEN
        ins_line(p_type,'TABLESPACE '||p_tab_parts.tab_tablespace);
      END IF;

      IF p_tab_parts.tab_storage IS NOT NULL THEN
        ins_line(p_type,tab_storage(p_recname, p_tab_parts.tab_storage)); -- 6.9.2007
      END IF;

      IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
        FOR l_subpartition IN 1..p_subpartitions LOOP
          IF l_subpartition = 1 THEN
            l_part_def := '(';
          ELSE
            l_part_def := ',';
          END IF;
          l_part_def := l_part_def||'SUBPARTITION '||LOWER(p_part_name||'_'
                                  ||p_tab_parts.part_name||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
          ins_line(p_type,l_part_def);
        END LOOP;
        ins_line(p_type,')');
      ELSIF p_subpart_type = 'R' THEN
        tab_rangesubparts(p_type, p_recname
                     ,p_tab_parts.part_id
                     ,p_tab_parts.part_name
                     ,p_part_name);
      ELSIF p_subpart_type = 'L' THEN
        tab_listsubparts(p_type, p_recname
                     ,p_tab_parts.part_id
                     ,p_tab_parts.part_name
                     ,p_part_name);
      END IF;
      ins_line(p_type,'/');
      ins_line(p_type,'');

      -- reset index tablespace at table level
      unset_index_tablespace(p_type, p_recname, p_table_owner, p_arch_flag);
    END LOOP;
 
   IF l_counter > 0 THEN -- if we did anything at all, do the end stuff
      -- alter indexes to set tablespace to partition tablespace for max value partition
      set_index_tablespace(p_type, p_recname, l_part_name, p_table_owner, p_arch_flag);

      -- add new maxvalue partition
      ins_line(p_type,'ALTER TABLE '||LOWER(p_table_owner||'.'||p_table_name)
                                    ||' ADD PARTITION '||LOWER(p_part_name||'_'||l_part_name));
      ins_line(p_type,'VALUES LESS THAN ('||l_part_value||')');
      IF l_tab_tablespace IS NOT NULL THEN
        ins_line(p_type,'TABLESPACE '||l_tab_tablespace);
      END IF;
      IF l_tab_storage IS NOT NULL THEN
        ins_line(p_type,tab_storage(p_recname, l_tab_storage)); -- 6.9.2007
      END IF;

      IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
        FOR l_subpartition IN 1..p_subpartitions LOOP
          IF l_subpartition = 1 THEN
            l_part_def := '(';
          ELSE
            l_part_def := ',';
          END IF;
          l_part_def := l_part_def||'SUBPARTITION '||LOWER(p_part_name||'_'
                                ||l_part_name||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
          ins_line(p_type,l_part_def);
        END LOOP;
        ins_line(p_type,')');
      ELSIF p_subpart_type = 'R' THEN
        tab_rangesubparts(p_type, p_recname
                     ,p_part_id
                     ,l_part_name
                     ,p_part_name);
      ELSIF p_subpart_type = 'L' THEN
        tab_listsubparts(p_type, p_recname
                     ,p_part_id
                     ,l_part_name
                     ,p_part_name);
      END IF;
      ins_line(p_type,'/');
      ins_line(p_type,'');

      -- reset index tablespace at table level
      unset_index_tablespace(p_type, p_recname, p_table_owner, p_arch_flag);

      --hint logic 
      l_hint := 'APPEND';
      IF l_logging = 'N' THEN
        l_hint := l_hint||' NOLOGGING';
      END IF;
      IF l_parallel_table = 'Y' THEN 
        l_hint  := l_hint||' PARALLEL(T)';
        l_hint2 :=          'PARALLEL(S)';
      ELSE
        l_hint2 := '';
      END IF;

      IF l_hint IS NOT NULL THEN
        l_hint := ' /*+'||l_hint||'*/';
      END IF;
      IF l_hint2 IS NOT NULL THEN
        l_hint2 := ' /*+'||l_hint2||'*/';
      END IF;


      -- insert back from maxvalue
      ins_line(p_type,'INSERT'||l_hint||' INTO '||LOWER(l_schema2||p_table_name)||' t');
      ins_line(p_type,'SELECT'||l_hint2||' * FROM '||LOWER(l_schema2||'gfc_'||p_recname)||' s');
      ins_line(p_type,'/');

      -- drop partex table
      ins_line(p_type,'DROP TABLE '||LOWER(l_schema2||'gfc_'||p_recname)||l_drop_purge_suffix);
      ins_line(p_type,'/');
      ins_line(p_type,'');
    END IF;

  EXCEPTION 
    WHEN no_data_found THEN NULL; -- if no max partition abandon package
  END;

  unset_action(p_action_name=>l_action);
END add_tab_parts_newmax;


-------------------------------------------------------------------------------------------------------
-- generate split partition clause on the basis of part ranges table -- procedure added 23.4.2010
-------------------------------------------------------------------------------------------------------
PROCEDURE split_tab_parts
(p_type          NUMBER
,p_recname       VARCHAR2
,p_table_owner   VARCHAR2
,p_table_name    VARCHAR2
,p_part_id       VARCHAR2
,p_part_type     VARCHAR2
,p_subpart_type  VARCHAR2
,p_subpartitions INTEGER
,p_arch_flag     VARCHAR2
,p_part_name     VARCHAR2
) IS
  l_part_def CLOB;
  l_counter INTEGER := 0;
  l_subpartition INTEGER;
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'SPLIT_TAB_PARTS');
  debug_msg('SPLIT_TAB_PARTS:recname='||p_recname
                           ||'/table='||p_table_owner||'.'||p_table_name
                           ||'/part_id='||p_part_id
                           ||'/part_type='||p_part_type
                           ||'/subpart_type='||p_subpart_type
                           ||'/subpartitions='||p_subpartitions
                           ||'/arch_flag='||p_arch_flag
                           ||'/part_name='||p_part_name,6);

  FOR p_tab_parts IN (
    SELECT /*+LEADING(t pr) USE_NL(pr pr2)*/ pr.*
    ,      pr2.part_name      part_name2
    ,      pr2.tab_tablespace tab_tablespace2
    ,      pr2.idx_tablespace idx_tablespace2
    ,      pr2.tab_storage    tab_storage2
    ,      pr2.idx_storage    idx_storage2
    FROM   gfc_part_ranges pr  -- the missing partition
    ,      gfc_part_ranges pr2 -- the next partition that exists
    ,      dba_tables t
    WHERE  t.owner = p_table_owner
    AND    t.table_name = p_table_name
    AND    t.partitioned = 'YES'
    AND    (pr.arch_flag = p_arch_flag OR p_arch_flag IS NULL)
    AND    pr.part_id = p_part_id
    AND    pr2.part_id = pr.part_id
    AND    pr2.part_no = ( -- next partition that exists
           SELECT /*+QB_NAME(PR2A) UNNEST PUSH_SUBQ*/ 
                  MIN(pr2a.part_no)
           FROM   gfc_part_ranges pr2a
           ,      dba_tab_partitions tp2
           WHERE  pr2a.part_id = pr.part_id
           AND    pr2a.part_no > pr.part_no
           AND    tp2.table_owner = t.owner
           AND    tp2.table_name = t.table_name
           AND    tp2.partition_name = p_part_name||'_'||pr2a.part_name)
    AND NOT EXISTS( -- partition in question exists
           SELECT /*+QB_NAME(PR) UNNEST PUSH_SUBQ*/ 
                  'x'
           FROM   dba_tab_partitions tp
           WHERE  tp.table_owner = t.owner
           AND    tp.table_name = t.table_name
           AND    tp.partition_name = p_part_name||'_'||pr.part_name
           AND    rownum = 1)
    AND EXISTS( -- a subsequent partition exists-this might be tortology
           SELECT /*+QB_NAME(PR1) UNNEST PUSH_SUBQ*/ 
                  'x'
           FROM   gfc_part_ranges pr1
           ,      dba_tab_partitions tp1
           WHERE  pr1.part_id = pr.part_id
           AND    pr1.part_no > pr.part_no
           AND    tp1.table_owner = t.owner
           AND    tp1.table_name = t.table_name
           AND    tp1.partition_name = p_part_name||'_'||pr1.part_name
           AND    rownum = 1)
    AND    NOT (   l_repopnewmax = 'Y' 
               AND UPPER(pr2.part_value) LIKE '%MAXVALUE%'
--             AND p_subpart_type = 'N' /*31.7.2017 do not check subpartitioning*/
               )
    ORDER BY pr.part_no
  ) LOOP
    debug_msg('Part Range:'||p_tab_parts.part_No||'/'||p_tab_parts.part_name,6);

    l_counter := l_counter + 1;

    -- alter indexes to set tablespace to partition tablespace
    set_index_tablespace(p_type, p_recname, p_tab_parts.part_name, p_table_owner, p_arch_flag);

    -- rename high partition - 6.2.2012
    IF p_subpart_Type = 'H' AND p_subpartitions > 1 THEN
       rename_subparts(p_type=>p_type, p_table_name=>p_table_name, p_part_name=>UPPER(p_part_name||'_'||p_tab_parts.part_name2), p_drop_index=>'N');
    END IF;

-- start partition split command
    ins_line(p_type,'ALTER TABLE '||LOWER(p_table_owner||'.'||p_table_name)
                                  ||' SPLIT PARTITION '||LOWER(p_part_name  ||'_'||p_tab_parts.part_name2));
    ins_line(p_type,'AT ('||p_tab_parts.part_value||') INTO');

--first partition in split
    ins_line(p_type,'(PARTITION '||LOWER(p_part_name||'_'||p_tab_parts.part_name));
    IF p_tab_parts.tab_tablespace IS NOT NULL THEN
      ins_line(p_type,'TABLESPACE '||p_tab_parts.tab_tablespace);
    END IF;
    IF p_tab_parts.tab_storage IS NOT NULL THEN
      ins_line(p_type,tab_storage(p_recname, p_tab_parts.tab_storage)); -- 6.9.2007
    END IF;

    -- build hash subpartition definitions
    IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
      FOR l_subpartition IN 1..p_subpartitions LOOP
        IF l_subpartition = 1 THEN
          l_part_def := '(';
        ELSE
          l_part_def := ',';
        END IF;
        l_part_def := l_part_def||'SUBPARTITION '||LOWER(p_part_name||'_'
	                        ||p_tab_parts.part_name||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
        ins_line(p_type,l_part_def);
      END LOOP;
      ins_line(p_type,')');
    ELSIF p_subpart_type = 'L' THEN
      tab_listsubparts(p_type, p_recname
                   ,p_tab_parts.part_id
                   ,p_tab_parts.part_name
                   ,p_part_name);
    ELSIF p_subpart_type = 'R' THEN
      tab_rangesubparts(p_type, p_recname
                   ,p_tab_parts.part_id
                   ,p_tab_parts.part_name
                   ,p_part_name);
    END IF;

-- second partition in split
    ins_line(p_type,',PARTITION '||LOWER(p_part_name||'_'||p_tab_parts.part_name2));
    IF p_tab_parts.tab_tablespace2 IS NOT NULL THEN
      ins_line(p_type,'TABLESPACE '||p_tab_parts.tab_tablespace2);
    END IF;
    IF p_tab_parts.tab_storage2 IS NOT NULL THEN -- 11.3.2013-- change to test tab_Storage2
      ins_line(p_type,tab_storage(p_recname, p_tab_parts.tab_storage2)); -- 6.9.2007
    END IF;

    -- build hash subpartition definitions
    IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
      FOR l_subpartition IN 1..p_subpartitions LOOP
        IF l_subpartition = 1 THEN
          l_part_def := '(';
        ELSE
          l_part_def := ',';
        END IF;
        l_part_def := l_part_def||'SUBPARTITION '||LOWER(p_part_name||'_'
	                        ||p_tab_parts.part_name2||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
        ins_line(p_type,l_part_def);
      END LOOP;
      ins_line(p_type,')');
    ELSIF p_subpart_type = 'L' THEN
      tab_listsubparts(p_type, p_recname
                   ,p_tab_parts.part_id
                   ,p_tab_parts.part_name2
                   ,p_part_name);
    ELSIF p_subpart_type = 'R' THEN
      tab_rangesubparts(p_type, p_recname
                   ,p_tab_parts.part_id
                   ,p_tab_parts.part_name2
                   ,p_part_name);
    END IF;

-- tail piece
    IF l_split_index_update = 'GLOBAL' THEN --14.3.2022
      ins_line(p_type,') UPDATE GLOBAL INDEXES');
    ELSIF l_split_index_update = 'NONE' THEN
      ins_line(p_type,') /*NO INDEX UPDATE*/');
    ELSIF l_split_index_update = 'ALL' THEN
      ins_line(p_type,') UPDATE INDEXES');
    ELSE
      ins_line(p_type,') UPDATE INDEXES');
    END IF;
    ins_line(p_type,'/');

    IF l_split_index_update IN('GLOBAL','NONE') THEN --QWERT rebuild indexes
      FOR p_ind_parts IN ( --find local index partitions
        WITH x AS (
          SELECT i.indexid
          ,      NVL(pi.part_id,pt.part_id) part_id
          ,      NVL(pi.part_type,pt.part_type) part_type
          ,      CASE WHEN pi.part_Type = 'N' THEN 'NONE'
                      WHEN pi.part_id != pt.part_id OR pi.part_Type != pt.part_type THEN 'GLOBAL'
                      WHEN pi.part_type IS NULL AND pt.part_type != 'N' THEN 'LOCAL'
                 END as locality
          FROM gfc_part_tables pt
          ,    gfc_ps_indexdefn i
            LEFT OUTER JOIN gfC_part_indexes pi
              ON pi.recname = i.recname
              AND pi.indexid = i.indexid
          WHERE pt.recname = p_recname
          AND   pt.recname = i.recname    
        )
        SELECT x.*, pr.part_name
        FROM   x
          LEFT OUTER JOIN gfc_part_ranges pr
          ON pr.part_id = x.part_id
        WHERE x.locality = 'LOCAL'
        AND   pr.part_name IN(p_tab_parts.part_name,p_tab_parts.part_name2)
      ) LOOP
        ins_line(p_type,'ALTER INDEX '||LOWER('PS'||p_ind_parts.indexid||p_recname)||
                        ' REBUILD PARTITION '||LOWER(p_recname||'_'||p_ind_parts.part_name)||' PARALLEL;');
      END LOOP;
    END IF;
    ins_line(p_type,'');
  END LOOP;

  IF l_counter > 0 THEN
    -- reset index tablespace at table level
    unset_index_tablespace(p_type, p_recname, p_table_owner, p_arch_flag);
    -- ins_line(p_type,'spool off');
  END IF;
  unset_action(p_action_name=>l_action);
END split_tab_parts;

-------------------------------------------------------------------------------------------------------
-- process partitioned tables
-------------------------------------------------------------------------------------------------------
PROCEDURE part_tables
(p_part_id VARCHAR2 DEFAULT ''
,p_recname VARCHAR2 DEFAULT ''
) IS
  l_hint      VARCHAR2(100 CHAR);
  l_hint2     VARCHAR2(100 CHAR);
  l_sess_para VARCHAR2(100 CHAR); -- added 1.11.2012 to hold session parallel dml command
  l_counter   INTEGER := 0;
  l_degree    VARCHAR2(100 CHAR);
  l_schema    VARCHAR2(30 CHAR);
  l_schema2   VARCHAR2(30 CHAR);
  l_module    v$session.module%type;
  l_action    v$session.action%type;
  l_arch_flag VARCHAR2(1);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'PART_TABLES');

  FOR p_tables IN (
    SELECT  t.table_name
    ,       t.table_type
    ,       p.*
    ,       o.table_name ora_table_name
    FROM    gfc_ps_tables t
      LEFT OUTER JOIN user_tables o
      ON o.table_name = t.table_name
    ,       gfc_part_tables p
    WHERE   t.table_type = 'P'
    AND     t.match_db = 'N'
    AND     t.rectype =0
    AND     t.recname = p.recname
--    AND   EXISTS( -- check that partitioning column exists
--          SELECT 'x'
--          FROM   gfc_ps_tab_columns c
--          WHERE  c.fieldname = p.part_column
--          AND    c.recname = p.recname
--          )
    AND (NOT t.recname IN( -- supress partitioning of tables with long columns
          SELECT c.recname 
          FROM   gfc_ps_tab_columns c 
          ,      psdbfield f
          WHERE  c.fieldname = f.fieldname
          AND    ( (f.fieldtype IN(1) AND l_longtoclob = 'N') --allow partitioning on CLOB 2.3.2015
                 OR f.fieldtype IN(8,9)) --other raw
          AND      (f.length = 0 OR f.length > 2000)
          )
      OR    t.override_schema IS NOT NULL 
      OR  l_longtoclob = 'Y'
    )
    AND  (p.recname LIKE p_recname OR p_recname IS NULL)
    AND  (p.part_id LIKE p_part_id OR p_part_id IS NULL) -- added 11.2.2013
    ORDER BY t.recname    
  ) LOOP
    IF l_explicit_schema = 'Y' THEN
      l_schema := NVL(p_tables.override_schema,LOWER(l_schema1))||'.';
    ELSE 
      l_schema := '';
    END IF;

    IF p_tables.arch_flag IN('A','D') THEN
      l_arch_flag := 'N'; -- build the non-arch partitions only
    ELSE
      l_arch_flag := ''; -- build all partitions
    END IF;

    signature(k_build,FALSE,l_scriptid,p_tables.recname);
    ins_line(k_build,'rem Partitioning Scheme '||p_tables.part_id);
    whenever_sqlerror(k_build,TRUE);
    forceddldop(k_build);
    ddlpermit(k_build,TRUE); -- added 10.10.2007

    -- ins_line(k_alter,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
    -- ins_line(k_alter,LOWER('spool gfcalter_'||l_dbname||'_'||p_tables.recname||'.lst'));
    signature(k_alter,TRUE,'gfcalter',p_tables.recname);
     ins_line(k_alter,'rem Partitioning Scheme '||p_tables.part_id);
    whenever_sqlerror(k_alter,TRUE);
    forceddldop(k_alter);
    
    IF p_tables.src_table_name IS NULL THEN -- added 8.6.2010  
      whenever_sqlerror(k_build,FALSE);
      ins_line(k_build,'DROP TABLE '||LOWER(l_schema||'old_'||p_tables.recname)||l_drop_purge_suffix);
      ins_line(k_build,'/');
      ins_line(k_build,'');
      whenever_sqlerror(k_build,TRUE);
      rename_parts(k_build,p_tables.table_name,l_drop_index);
      IF (   (p_tables.subpart_type = 'L') 
          OR (p_tables.subpart_type = 'H' AND p_tables.hash_partitions > 1)
         ) AND p_tables.subpart_column IS NOT NULL THEN
        rename_subparts(p_type=>0, p_table_name=>p_tables.table_name, p_drop_index=>l_drop_index);
      END IF;
    ELSE
      whenever_sqlerror(k_build,FALSE);
      ins_line(k_build,'DROP TABLE '||LOWER(l_schema||'gfc_'||p_tables.recname)||l_drop_purge_suffix);
      ins_line(k_build,'/');
      ins_line(k_build,'DROP TABLE '||LOWER(l_schema||p_tables.table_name)||l_drop_purge_suffix);
      ins_line(k_build,'/');
      ins_line(k_build,'');
    END IF;
    pause_sql(k_build);
    whenever_sqlerror(k_build,TRUE);
--qwert valid existance of unique index?  otherwise force organisation back to table
    ins_line(k_build,'CREATE TABLE '||LOWER(l_schema||'gfc_'||p_tables.recname));
    tab_cols(k_build,p_tables.recname,l_longtoclob,p_tables.organization,'gfc_');

    IF p_tables.organization = 'I' THEN -- 25.5.2011 add organisation here if IOT
      ins_line(k_build,'ORGANIZATION INDEX');
    END IF;

    IF p_tables.tab_tablespace IS NOT NULL THEN
      ins_line(k_build,'TABLESPACE '||p_tables.tab_tablespace);
    END IF;

    IF p_tables.organization = 'I' THEN -- use index storage for IOT
      IF p_tables.idx_storage IS NOT NULL THEN
        ins_line(k_build,idx_storage(p_tables.recname, '_', p_tables.idx_storage)); -- 25.5.2011
      END IF;
    ELSE 
      IF p_tables.tab_storage IS NOT NULL THEN
        ins_line(k_build,tab_storage(p_tables.recname, p_tables.tab_storage)); -- 6.9.2007
      END IF;
    END IF;

    IF p_tables.part_type = 'R' THEN
      ins_line(k_build,'PARTITION BY RANGE('||p_tables.part_column||')');
      IF p_tables.organization = 'I' THEN
        NULL; -- 25.5.2011 cannot subpartition IOTs
      ELSIF p_tables.subpart_type = 'H' AND 
         p_tables.hash_partitions > 1 AND 
         p_tables.subpart_column IS NOT NULL THEN
        ins_line(k_build,'SUBPARTITION BY HASH ('||p_tables.subpart_column
             ||') SUBPARTITIONS '||p_tables.hash_partitions);
      ELSIF p_tables.subpart_type = 'L' AND 
            p_tables.subpart_column IS NOT NULL THEN
          ins_line(k_build,'SUBPARTITION BY LIST ('||p_tables.subpart_column||')');
      END IF;
      tab_part_ranges(k_build,p_tables.recname,p_tables.part_id,
                      p_tables.subpart_type,p_tables.hash_partitions,l_arch_flag,
                      l_schema1,p_tables.src_table_name);
    ELSIF p_tables.part_type = 'I' AND l_oraver >= 11 THEN --interval partitioning added 2.3.2015
      ins_line(k_build,'PARTITION BY RANGE('||p_tables.part_column||')');
      --subpartitioning can only be added with a template
      ins_line(k_build,'INTERVAL ('||p_tables.interval_expr||')');
      tab_part_ranges(k_build,p_tables.recname,p_tables.part_id,
                      p_tables.subpart_type,p_tables.hash_partitions,l_arch_flag,
                      l_schema1,p_tables.src_table_name);
    ELSIF p_tables.part_type = 'L' THEN
      ins_line(k_build,'PARTITION BY LIST('||p_tables.part_column||')');
      IF p_tables.subpart_type = 'R' AND --21.10.2014 add list sub range partitioning
         p_tables.subpart_column IS NOT NULL THEN
        ins_line(k_build,'SUBPARTITION BY RANGE ('||p_tables.subpart_column||')');
      END IF;
      tab_part_lists(k_build,p_tables.recname,p_tables.part_id,
          p_tables.subpart_type,p_tables.hash_partitions,l_arch_flag);
    ELSIF p_tables.part_type = 'H' AND 
          p_tables.hash_partitions > 1 THEN
      ins_line(k_build,'PARTITION BY HASH ('||p_tables.part_column||')');
      tab_hashparts(p_type     =>0
                   ,p_recname  =>p_tables.recname
                   ,p_num_parts=>p_tables.hash_partitions);
    ELSIF p_tables.subpart_type = 'L' AND 
          p_tables.subpart_column IS NOT NULL THEN
      ins_line(k_build,'PARTITION BY LIST ('||p_tables.subpart_column||')');
    END IF;

    ins_line(k_build,'ENABLE ROW MOVEMENT');
-- 9.10.2003 - create table with parallelism enabled
    IF l_parallel_index = 'Y' THEN
      IF l_force_para_dop IS NULL THEN
        ins_line(k_build,'PARALLEL');
      ELSE
        ins_line(k_build,'PARALLEL '||l_force_para_dop);
      END IF;
    ELSE
      ins_line(k_build,'NOPARALLEL');
    END IF;
    IF l_logging = 'N' THEN
      ins_line(k_build,'NOLOGGING');
    END IF;
    ins_line(k_build,'/');
    ins_line(k_build,'');
-- 9.10.2003 - was UBS specific but made generic
    IF l_roles = 'Y' THEN
      IF l_read_all IS NOT NULL THEN --if there is a read role
        ins_line(k_build,'GRANT SELECT ON '||LOWER(l_schema||'gfc_'||p_tables.recname)||' TO '||LOWER(l_read_all));
        ins_line(k_build,'/');
      END IF;
      IF l_update_all IS NOT NULL THEN --if there is an update role
        ins_line(k_build,'GRANT INSERT, UPDATE, DELETE ON '||LOWER(l_schema||'gfc_'||p_tables.recname)||' TO '||LOWER(l_update_all));
        ins_line(k_build,'/');
      END IF;
      ins_line(k_build,'');
    END IF;

    -- 18.2.2011 - hint logic moved inside loop here
    l_hint := 'APPEND';
    IF l_logging = 'N' THEN
      l_hint := l_hint||' NOLOGGING';
    END IF;

    -- 2.10.2010 - make copy go parallel, moved head 16.1.2011
    l_sess_para := 'ALTER SESSION '; -- added 1.11.2012 set session parallelism
    IF l_parallel_table = 'Y' THEN -- 16.1.2011 removed alter and used 2 hints
      l_hint  := l_hint||' PARALLEL(T)';
      l_hint2 :=          'PARALLEL(S)';
      l_sess_para := l_sess_para||'ENABLE'; -- added 1.11.2012 set session parallelism
    ELSE
      l_hint2 := '';
      l_sess_para := l_sess_para||'DISABLE'; -- added 1.11.2012 set session parallelism
    END IF;
    l_sess_para := l_sess_para||' PARALLEL DML';

    IF l_hint IS NOT NULL THEN
      l_hint := ' /*+'||l_hint||'*/';
    END IF;
    IF l_hint2 IS NOT NULL THEN
      l_hint2 := ' /*+'||l_hint2||'*/';
    END IF;

-- 18.9.2003-added trigger to prevent updates on tables whilst being rebuilt - will be dropped when table is dropped
-- 5.5.2010-only query source table if it exists
    IF p_tables.src_table_name IS NOT NULL THEN
      ins_line(k_build,l_sess_para); -- added 1.11.2012
      ins_line(k_build,'/');
      ins_line(k_build,'');
 
      ins_line(k_build,'BEGIN'); -- 1.10.2010 insert in PL/SQL block
      ins_line(k_build,'INSERT'||l_hint||' INTO '||LOWER(l_schema||'gfc_'||p_tables.recname)||' t (');
      tab_col_list(k_build,p_tables.recname,p_column_name=>TRUE);
      ins_line(k_build,') SELECT'||l_hint2);
      tab_col_list(k_build,p_tables.recname,p_tables.src_table_name,p_column_name=>FALSE); 
      -- 20.10.2008 - added criteria option
      IF p_tables.criteria IS NOT NULL THEN
        ins_line(k_build,'FROM '||LOWER(l_schema||p_tables.src_table_name)||' s');
        ins_line(k_build,p_tables.criteria||';');
      ELSE
        ins_line(k_build,'FROM '||LOWER(l_schema||p_tables.src_table_name)||' s;');
      END IF;
      ins_line(k_build,'dbms_output.put_line(TO_CHAR(SQL%ROWCOUNT)||'' rows inserted.'');');
      ins_line(k_build,'COMMIT;');
      ins_line(k_build,'END;');
      ins_line(k_build,'/'); 
      pause_sql(k_build);

    ELSIF p_tables.ora_table_name IS NOT NULL THEN
      ins_line(k_build,'LOCK TABLE '||LOWER(l_schema||p_tables.ora_table_name)||' IN EXCLUSIVE MODE'); -- lock table to ensure trigger creates
      ins_line(k_build,'/');
      ins_line(k_build,'');

      ins_line(k_build,'CREATE OR REPLACE TRIGGER '||LOWER(l_schema||p_tables.recname)||'_nochange');
      ins_line(k_build,'BEFORE INSERT OR UPDATE OR DELETE ON '||LOWER(l_schema||p_tables.ora_table_name));
      ins_line(k_build,'BEGIN');
      ins_line(k_build,'   RAISE_APPLICATION_ERROR(-20100,''NO DML OPERATIONS ALLOWED ON '||UPPER(l_schema||p_tables.ora_table_name)||''');');
      ins_line(k_build,'END;');
      ins_line(k_build,'/');
      ins_line(k_build,'');

      ins_line(k_build,'COMMIT'); -- explicit commit added 27.11.2012
      ins_line(k_build,'/');
      ins_line(k_build,l_sess_para); -- added 1.11.2012, 27.11.2012 moved in front of lock
      ins_line(k_build,'/');
      ins_line(k_build,'');

      ins_line(k_build,'LOCK TABLE '||LOWER(l_schema||p_tables.ora_table_name)||' IN EXCLUSIVE MODE'); -- lock table to prevent consistent reads on query
      ins_line(k_build,'/');
      ins_line(k_build,'');

      ins_line(k_build,'BEGIN'); -- 1.10.2010 insert in PL/SQL block
      ins_line(k_build,'INSERT'||l_hint||' INTO '||LOWER(l_schema||'gfc_'||p_tables.recname)||' t (');
      tab_col_list(k_build,p_tables.recname,p_column_name=>TRUE);
      ins_line(k_build,') SELECT'||l_hint2);
      tab_col_list(k_build,p_tables.recname,p_tables.ora_table_name,p_column_name=>FALSE);
-- 20.10.2008 - added criteria option
      IF p_tables.criteria IS NOT NULL THEN
        ins_line(k_build,'FROM '||LOWER(l_schema||p_tables.ora_table_name)||' s');
        ins_line(k_build,p_tables.criteria||';');
      ELSE
        ins_line(k_build,'FROM '||LOWER(l_schema||p_tables.ora_table_name||' s;'));
      END IF;
      ins_line(k_build,'dbms_output.put_line(TO_CHAR(SQL%ROWCOUNT)||'' rows inserted.'');');
      ins_line(k_build,'COMMIT;');
      ins_line(k_build,'END;');
      ins_line(k_build,'/');
      ins_line(k_build,'');
      pause_sql(k_alter);
    END IF;

    mk_part_indexes(p_tables.recname,p_tables.table_name, l_schema1, l_arch_flag); 
    pause_sql(k_alter);
-- 9.10.2003 - alter table to logging and noparallel
    whenever_sqlerror(k_build,TRUE);
    IF p_tables.organization = 'I' THEN -- 25.5.2011 add organisation here if IOT
      ins_line(k_build,l_noalterprefix||'ALTER TABLE '||LOWER(l_schema||'gfc_'||p_tables.recname)||' LOGGING NOPARALLEL'); -- 25.5.2011
      ins_line(k_build,'/');
    ELSE
      -- changed 1.5.2010 to make new table noparallel logging
      ins_line(k_build,l_noalterprefix||'ALTER TABLE '||LOWER(l_schema||'gfc_'||p_tables.recname)||' LOGGING NOPARALLEL MONITORING'); -- 6.9.2007
      ins_line(k_build,'/');
    END IF;

    whenever_sqlerror(k_build,FALSE);
    -- 5.5.2010 only if old table exists
    IF p_tables.ora_table_name IS NOT NULL THEN
      IF p_tables.organization = 'I' THEN -- 25.5.2011 alter old index and contraint on IOT
        ins_line(k_build,l_noalterprefix||'ALTER INDEX '||LOWER(l_schema||p_tables.table_name)||' RENAME TO old_'||LOWER(p_tables.recname)); 
        ins_line(k_build,'/');
        ins_line(k_build,l_noalterprefix||'ALTER TABLE '||LOWER(l_schema||p_tables.table_name)
                                        ||' RENAME CONSTRAINT '||LOWER(p_tables.table_name)
                                        ||' TO old_'||LOWER(p_tables.recname)); 
        ins_line(k_build,'/');
      END IF;
      ins_line(k_build,l_noalterprefix||'ALTER TABLE '||LOWER(l_schema||p_tables.table_name)
                                      ||' RENAME TO old_'||LOWER(p_tables.recname)); -- 6.9.2007
      ins_line(k_build,'/');
      ins_line(k_build,'');
    END IF;

    whenever_sqlerror(k_build,TRUE);
    IF p_tables.organization = 'I' THEN -- 25.5.2011 add organisation here if IOT
      ins_line(k_build,l_noalterprefix||'ALTER INDEX '||LOWER(l_schema||'gfc_'||p_tables.recname)
                                      ||' RENAME TO '||LOWER(p_tables.table_name)); 
      ins_line(k_build,'/');
      ins_line(k_build,l_noalterprefix||'ALTER TABLE '||LOWER(l_schema||'gfc_'||p_tables.recname)
                                      ||' RENAME CONSTRAINT '||LOWER('gfc_'||p_tables.recname)
                                      ||' TO '||LOWER(p_tables.table_name)); 
      ins_line(k_build,'/');
    END IF;
    ins_line(k_build,l_noalterprefix||'ALTER TABLE '||LOWER(l_schema||'gfc_'||p_tables.recname)
                                    ||' RENAME TO '||LOWER(p_tables.table_name)); -- 6.9.2007
    ins_line(k_build,'/');
    ins_line(k_build,'');
    pause_sql(k_build);
--  ins_line(k_build,'ANALYZE TABLE '||LOWER(l_schema||p_tables.table_name)
--                                   ||' ESTIMATE STATISTICS SAMPLE 1 PERCENT;');
    signature(k_stats,FALSE,'gfcstats',p_tables.recname);
    IF l_build_stats = 'Y' THEN
      l_counter := 0; -- do build stats command in table build script
    ELSE 
      l_counter := 2; -- do not  build stats command in table build script
    END IF;
    WHILE l_counter <= 2 LOOP
      ins_line(l_counter,'');
      IF p_tables.stats_type = 'Y' THEN
        IF l_oraver >= 8.173 THEN
          ins_line(l_counter,'BEGIN');
          ins_line(l_counter,'sys.dbms_stats.gather_table_stats');
          ins_line(l_counter,'(ownname=>'''||UPPER(l_schema1)||'''');
          ins_line(l_counter,',tabname=>'''||UPPER(p_tables.table_name)||'''');
          ---------- sample size ----------
          IF l_oraver < 9 THEN /*Oracle 8i*/
            IF p_tables.sample_size IS NULL THEN
              ins_line(l_counter,',estimate_percent=>0.1');
            ELSE
              ins_line(l_counter,',estimate_percent=>'||p_tables.sample_size);
            END IF;
            -- 30.10.2007: added method opt override
            ins_line(l_counter,',method_opt=>'''||NVL(p_tables.method_opt,'FOR ALL INDEXED COLUMNS SIZE 1')||'''');
          ELSIF l_oraver >= 9 AND l_oraver < 11 THEN /*supress stats size in 11g or higher - use table preferences*/
            IF p_tables.sample_size IS NULL THEN
              ins_line(l_counter,',estimate_percent=>DBMS_STATS.AUTO_SAMPLE_SIZE');
            ELSE
              ins_line(l_counter,',estimate_percent=>'||p_tables.sample_size); -- 6.9.2007
            END IF;
            -- 30.10.2007: added method opt override
            ins_line(l_counter,',method_opt=>'''||NVL(p_tables.method_opt,'FOR ALL COLUMNS SIZE AUTO')||'''');
          END IF;
          ---------- block sample ----------
          IF l_block_sample = 'Y' THEN
            ins_line(l_counter,',block_sample=>TRUE'); 
          END IF;
          ---------- granularity ----------
          IF l_oraver >= 11 THEN /*supress parameter generation in 11g or higher - use table preferences*/
            NULL;
          ELSIF l_oraver >= 10 THEN 
            ins_line(l_counter,',granularity=>''ALL'''); 
          ELSE
            IF p_tables.subpart_type = 'H' AND 
               p_tables.hash_partitions > 1 THEN
              ins_line(l_counter,',granularity=>''ALL'''); 
            ELSE
              ins_line(l_counter,',granularity=>''ALL'''); 
            END IF;
          END IF;
          ---------- degree ----------
          IF l_force_para_dop IS NOT NULL THEN --12.7.2021 for all versions
            ins_line(l_counter,',degree=>'||l_force_para_dop||'');
--        ELSE
--          ins_line(l_counter,',degree=>DBMS_STATS.DEFAULT_DEGREE');
          END IF;
          ---------- cascade ----------
          IF l_oraver < 11 THEN /*supress parameter generation in 11g or higher - use table preferences*/
            ins_line(l_counter,',cascade=>TRUE');
          END IF;
          ---------- force ----------
          IF l_oraver >= 11 THEN /*override locked stats*/
            ins_line(l_counter,',force=>TRUE');
          END IF;
          ---------- end ----------
          ins_line(l_counter,');');
          ins_line(l_counter,'END;');
          ins_line(l_counter,'/');
        ELSE -- use analyze on 8.1.7.2
          ins_line(l_counter,'ANALYZE TABLE '||l_schema1||'.'||LOWER(p_tables.table_name)  
                                             ||' ESTIMATE STATISTICS SAMPLE 1 PERCENT;');
          ins_line(l_counter,'/');
        END IF;
      ELSIF p_tables.stats_type = 'D' THEN
        ins_line(l_counter,'BEGIN');
        ins_line(l_counter,' sys.dbms_stats.delete_table_stats');
        ins_line(l_counter,' (ownname=>'''||UPPER(l_schema1)||'''');
        ins_line(l_counter,' ,tabname=>'''||UPPER(p_tables.table_name)||'''');
        IF l_oraver >= 10 THEN
          ins_line(l_counter,' ,force=>TRUE');
          ins_line(l_counter,' );');
          ins_line(l_counter,' sys.dbms_stats.lock_table_stats');
          ins_line(l_counter,' (ownname=>'''||UPPER(l_schema1)||'''');
          ins_line(l_counter,' ,tabname=>'''||UPPER(p_tables.table_name)||'''');
        END IF;
        ins_line(l_counter,' );');
        ins_line(l_counter,'END;');
        ins_line(l_counter,'/');
      END IF;
      ins_line(l_counter,'');
      l_counter := l_counter + 2;
    END LOOP;
    pause_sql(k_build);
    whenever_sqlerror(k_build,FALSE); -- 6.9.2007
    ins_line(k_build,l_noalterprefix||'DROP TABLE ' 
                                    ||LOWER(l_schema||'old_'||p_tables.recname)
                                    ||l_drop_purge_suffix); -- 6.9.2007
    ins_line(k_build,'/');
    ins_line(k_build,'');
    ddlpermit(k_build,FALSE); -- added 10.10.2007
    ins_line(k_build,'DROP TRIGGER '||LOWER(l_schema||p_tables.recname)||'_nochange'); -- 6.9.2007
    ins_line(k_build,'/');
    ins_line(k_build,'');

    -- 12.2.2008 append/split missing partitions - but what about indexes
    IF p_tables.part_type IN('R','L') THEN
      whenever_sqlerror(k_alter,TRUE);
      ddlpermit(k_alter,TRUE); 
      ins_line(k_alter,'');

      -- add new range partitions where it is simply QWERT
      add_tab_parts(k_alter, p_tables.recname, l_schema1, p_tables.table_name, 
                    p_tables.part_id, p_tables.part_type, 
                    p_tables.subpart_type, p_tables.hash_partitions, 
                    'N', p_tables.recname);

      -- add new range partitions by splitting-11.3.2013 adjusted to omit maxvalue partition
      split_tab_parts(k_alter, p_tables.recname, l_schema1, p_tables.table_name, 
                      p_tables.part_id, p_tables.part_type, 
                      p_tables.subpart_type, p_tables.hash_partitions, 
                      'N', p_tables.recname);

      IF l_repopnewmax = 'Y' THEN -- added 11.3.2013
      --31.7.2017 allow on subpartitioned tables, remove: AND p_tables.subpart_type = 'N' 
        add_tab_parts_newmax(k_alter, p_tables.recname, l_schema1, p_tables.table_name, 
                             p_tables.part_id, p_tables.part_type, 
                             p_tables.subpart_type, p_tables.hash_partitions, 
                            'N', p_tables.recname);
      ELSIF p_tables.subpart_type = 'L' THEN /*09.09.2017 - only if not repopulating maxvalue partition*/
        add_tab_subparts(k_alter, p_tables.recname, l_schema1, p_tables.table_name, 
                         p_tables.part_id, p_tables.part_type, 
                         p_tables.subpart_type, p_tables.recname,
                         p_arch_flag=>'N');
      END IF;

      ddlpermit(k_alter,FALSE); 
      ins_line(k_alter,'');
    END IF;

    -- 5.5.2010-only query source table if it exists      
-------------------------------------------------------------------------------------------------------
-- 13.3.2013-- remved this block of code because it is a dup of something above that should not have been copied here
-- 22.3.2013-- put it back because need it at Hays
-------------------------------------------------------------------------------------------------------
    IF p_tables.src_table_name IS NOT NULL THEN

      ins_line(k_alter,l_sess_para); -- added 1.11.2012
      ins_line(k_alter,'/');
      ins_line(k_alter,'');

      ins_line(k_alter,'BEGIN'); -- 1.10.2010 insert in PL/SQL block
      ins_line(k_alter,'INSERT'||l_hint||' INTO '
                      ||LOWER(l_schema||'ps_'||p_tables.recname)||' t ('); -- 19.4.2013 insert into PS not GFC
      tab_col_list(k_alter,p_tables.recname,p_column_name=>TRUE);
      ins_line(k_alter,') SELECT'||l_hint2);
      tab_col_list(k_alter,p_tables.recname,p_tables.src_table_name,p_column_name=>FALSE);

      -- 20.10.2008 - added criteria option
      IF p_tables.criteria IS NOT NULL THEN
        ins_line(k_alter,'FROM '||LOWER(l_schema||p_tables.src_table_name)||' s');
        ins_line(k_alter,p_tables.criteria||';');
      ELSE
        ins_line(k_alter,'FROM '||LOWER(l_schema||p_tables.src_table_name)||' s;');
      END IF;
      ins_line(k_alter,'dbms_output.put_line(TO_CHAR(SQL%ROWCOUNT)||'' rows inserted.'');');
      ins_line(k_alter,'COMMIT;');
      ins_line(k_alter,'END;');
      ins_line(k_alter,'/');
      ins_line(k_alter,'');
      pause_sql(k_alter);
    END IF;
-------------------------------------------------------------------------------------------------------
    ins_line(k_build,'spool off');
    ins_line(k_stats,'spool off');
    ins_line(k_alter,'spool off');

  END LOOP;
  unset_action(p_action_name=>l_action);
END part_tables;
-------------------------------------------------------------------------------------------------------
-- process archive partitioned tables
-------------------------------------------------------------------------------------------------------
PROCEDURE arch_tables
(p_part_id VARCHAR2 DEFAULT ''
,p_recname VARCHAR2 DEFAULT '') IS
  l_module          v$session.module%type;
  l_action          v$session.action%type;
  l_arch_schema     VARCHAR2(30);
  l_arch_table_name VARCHAR2(30);
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'ARCH_TABLES');

  FOR p_tables IN (
    SELECT  t.table_name
    ,       t.table_type
    ,       p.*
    ,       DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) arch_sqltablename
    ,       DECODE(b.sqltablename,' ','PS_'||b.recname,b.sqltablename) base_sqltablename
    FROM    gfc_ps_tables t
            LEFT OUTER JOIN user_tables o
              ON o.table_name = t.table_name
    ,       psrecdefn b
    ,       gfc_part_tables p
            LEFT OUTER JOIN psrecdefn r
              ON r.recname = p.arch_recname
    WHERE   t.table_type = 'P'
    AND     t.match_db = 'N'
    AND     t.rectype = 0
    AND     t.recname = p.recname
    AND     b.recname = p.recname
    AND     (p.recname LIKE p_recname OR p_recname IS NULL)
    AND     (p.part_id LIKE p_part_id OR p_part_id IS NULL)
    AND     p.arch_flag IN('A','D')
    AND     (NOT t.recname IN( -- supress partitioning of tables with long columns
              SELECT c.recname 
              FROM   gfc_ps_tab_columns c 
              ,      psdbfield f
              WHERE  c.fieldname = f.fieldname
              AND    (  (f.fieldtype IN(1) AND l_longtoclob = 'N') --allow partitioning on CLOB 2.3.2015
                     OR  f.fieldtype IN(8,9)) --other raw 
              AND    (f.length = 0 OR f.length > 2000))
             OR    t.override_schema IS NOT NULL 
             OR  l_longtoclob = 'Y'
            )
      ORDER BY t.recname
  ) LOOP
    debug_msg('ARCH_TABLES:'||'/recname='||p_tables.recname
                            ||'/part_id='||p_tables.part_id
             ,6);

    l_arch_schema     := COALESCE(p_tables.arch_schema,p_tables.override_schema,l_schema1);
    l_arch_table_name := COALESCE(p_tables.arch_table_name,p_tables.arch_sqltablename,p_tables.table_name);

	    -- 11.2.2013 if table is only deleted but has a noarch condition then we need an exchange table
    IF p_tables.arch_flag = 'A' OR (p_tables.arch_flag = 'D' AND p_tables.noarch_condition IS NOT NULL) THEN

      signature(k_arch1,FALSE,'gfcarch1',p_tables.recname);
      ins_line(k_arch1,'rem Partitioning Scheme '||p_tables.part_id);
      whenever_sqlerror(k_arch1,FALSE);
      forceddldop(k_arch1);
      -- create the exchange table
      ins_line(k_arch1,'CREATE TABLE '||LOWER(l_arch_schema||'.XCHG_'||p_tables.recname));
      tab_cols(k_arch1,p_tables.recname,l_longtoclob,p_tables.organization,'arch_');

      IF p_tables.organization = 'I' THEN -- 25.5.2011 add organisation here if IOT
        ins_line(k_arch1,'ORGANIZATION INDEX');
      END IF;

      IF p_tables.tab_tablespace IS NOT NULL THEN
        ins_line(k_arch1,'TABLESPACE '||p_tables.tab_tablespace);
      END IF;

      IF p_tables.organization = 'I' THEN -- use index storage for IOT
        IF p_tables.idx_storage IS NOT NULL THEN
          ins_line(k_arch1,idx_storage(p_tables.recname, '_', p_tables.idx_storage)); -- 25.5.2011
        END IF;
      ELSE 
        IF p_tables.tab_storage IS NOT NULL THEN
          ins_line(k_arch1,tab_storage(p_tables.recname, p_tables.tab_storage)); -- 6.9.2007
        END IF;
      END IF;

      ins_line(k_arch1,'LOGGING NOPARALLEL'); -- always no parallel because we are going to partition exchange
      ins_line(k_arch1,'/');
      ins_line(k_arch1,'');

--    ins_line(k_arch1,'GRANT SELECT ON '||'XCHG_'||p_tables.recname||' TO '||LOWER(l_schema1));
--    ins_line(k_arch1,'/');
--    ins_line(k_arch1,'');

      mk_arch_indexes(k_arch1,p_tables.recname, l_arch_schema, 'XCHG_'||p_tables.recname, NULL, 'XCHG'); 
    END IF;

    -- if archive flag then create partitioned archive table
    IF p_tables.arch_flag = 'A' THEN
      -- create the partitioned archive table
      ins_line(k_arch1,'CREATE TABLE '||LOWER(l_arch_schema||'.'||l_arch_table_name));
      tab_cols(k_arch1,p_tables.recname,l_longtoclob,p_tables.organization,'arch_');

      IF p_tables.organization = 'I' THEN -- 25.5.2011 add organisation here if IOT
        ins_line(k_arch1,'ORGANIZATION INDEX');
      END IF;

      IF p_tables.tab_tablespace IS NOT NULL THEN
        ins_line(k_arch1,'TABLESPACE '||p_tables.tab_tablespace);
      END IF;

      IF p_tables.organization = 'I' THEN -- use index storage for IOT
        IF p_tables.idx_storage IS NOT NULL THEN
          ins_line(k_arch1,idx_storage(p_tables.recname, '_', p_tables.idx_storage)); -- 25.5.2011
        END IF;
      ELSE 
        IF p_tables.tab_storage IS NOT NULL THEN
          ins_line(k_arch1,tab_storage(p_tables.recname, p_tables.tab_storage)); -- 6.9.2007
        END IF;
      END IF;

      IF p_tables.part_type = 'R' THEN
        ins_line(k_arch1,'PARTITION BY RANGE('||p_tables.part_column||')');
        IF p_tables.organization = 'I' THEN
          NULL; -- 25.5.2011 cannot subpartition IOTs
        ELSIF p_tables.subpart_type = 'H' AND 
          p_tables.hash_partitions > 1 AND 
          p_tables.subpart_column IS NOT NULL THEN
          ins_line(k_arch1,'SUBPARTITION BY HASH ('||p_tables.subpart_column
                                                   ||') SUBPARTITIONS '||p_tables.hash_partitions);
        ELSIF p_tables.subpart_type = 'L' AND 
              p_tables.subpart_column IS NOT NULL THEN
          ins_line(k_arch1,'SUBPARTITION BY LIST ('||p_tables.subpart_column||')');
        END IF;
        tab_part_ranges(k_arch1,p_tables.recname,p_tables.part_id,
             p_tables.subpart_type,p_tables.hash_partitions,'A',
            l_arch_schema,l_arch_table_name,
            COALESCE(p_tables.arch_recname,p_tables.recname,l_arch_table_name));
      ELSIF p_tables.part_type = 'L' THEN
        ins_line(k_arch1,'PARTITION BY LIST('||p_tables.part_column||')');
        tab_part_lists(k_arch1,p_tables.recname,p_tables.part_id,
            p_tables.subpart_type,p_tables.hash_partitions,'A');
      ELSIF p_tables.part_type = 'H' AND 
            p_tables.hash_partitions > 1 THEN
        ins_line(k_arch1,'PARTITION BY HASH ('||p_tables.part_column||')');
        tab_hashparts(p_type=>k_arch1
               ,p_recname  =>p_tables.recname
               ,p_num_parts=>p_tables.hash_partitions);
      ELSIF p_tables.subpart_type = 'L' AND 
            p_tables.subpart_column IS NOT NULL THEN
        ins_line(k_arch1,'PARTITION BY LIST ('||p_tables.subpart_column||')');
      END IF;

      ins_line(k_arch1,'ENABLE ROW MOVEMENT');
      ins_line(k_arch1,'LOGGING NOPARALLEL'); -- always no parallel because we are going to partition exchange
      ins_line(k_arch1,'/');
      ins_line(k_arch1,'');

      ins_line(k_arch1,'GRANT SELECT ON '||LOWER(l_arch_schema||'.'||l_arch_table_name)||' TO '||LOWER(l_schema1));
      ins_line(k_arch1,'/');

      -- added 18.10.2012, removed 30.10.2012
      -- ins_line(k_arch1,'GRANT ALTER ON '||LOWER(l_arch_schema||'.'||l_arch_table_name)||' TO '||LOWER(l_schema1));
      ins_line(k_arch1,'');

      mk_arch_indexes(k_arch1,p_tables.recname, l_arch_schema, l_arch_table_name, 
          NVL(p_tables.arch_recname,p_tables.recname),'ARC'); 
      pause_sql(k_build);

      IF p_tables.part_type IN('R','L') THEN
        ins_line(k_arch1,'');
        whenever_sqlerror(k_arch1,FALSE); 

        add_tab_parts(k_arch1, p_tables.recname, l_arch_schema, l_arch_table_name, 
                p_tables.part_id, p_tables.part_type, 
                p_tables.subpart_type, p_tables.hash_partitions, 
                'A', NVL(p_tables.arch_recname,p_tables.recname));
        split_tab_parts(k_arch1, p_tables.recname, l_arch_schema, l_arch_table_name, 
                p_tables.part_id, p_tables.part_type, 
                p_tables.subpart_type, p_tables.hash_partitions, 
                'A', NVL(p_tables.arch_recname,p_tables.recname));
        IF p_tables.subpart_type = 'L' THEN
          add_tab_subparts(k_arch1, p_tables.recname, l_arch_schema, l_arch_table_name, 
               p_tables.part_id, p_tables.part_type, 
               p_tables.subpart_type,
               NVL(p_tables.arch_recname,p_tables.recname),
               p_arch_flag=>'A');
        END IF;
      END IF;
    END IF;

    ins_line(k_arch1,'spool off');

    IF l_schema1 != l_arch_schema THEN

      -- grants required to enable PSARCH to do exchange
      -- ins_line(k_arch2,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
      -- ins_line(k_arch2,LOWER('spool gfcarch2_'||l_dbname||'_'||p_tables.recname||'.lst'));

      signature(k_arch2,FALSE,'gfcarch2',p_tables.recname);
      ins_line(k_arch2,'rem Partitioning Scheme '||p_tables.part_id);
      forceddldop(k_arch2);

      -- grant select, alter on base table to archive schema
      ins_line(k_arch2,'GRANT SELECT, ALTER ON '||l_schema2||p_tables.base_sqltablename||' TO '||LOWER(l_arch_schema));
      ins_line(k_arch2,'/');

      IF p_tables.noarch_condition IS NOT NULL OR p_tables.arch_flag = 'D' THEN -- 8.5.2013 insert priv required for exchange
        -- grant insert on base table to archive schema if there is a no archive condition
        ins_line(k_arch2,'GRANT INSERT ON '||l_schema2||p_tables.base_sqltablename||' TO '||LOWER(l_arch_schema));
        ins_line(k_arch2,'/');
        ins_line(k_arch2,'');
        -- grant insert on base table to archive schema if preserve clause 
      END IF;
      ins_line(k_arch2,'spool off');
    END IF;

  END LOOP;
  unset_action(p_action_name=>l_action);
END arch_tables;

-------------------------------------------------------------------------------------------------------
-- process global temp tables
-------------------------------------------------------------------------------------------------------
PROCEDURE temp_tables  
(p_recname VARCHAR2 DEFAULT ''
) IS
  l_tempinstance  INTEGER;
  l_counter       INTEGER := 0;
  l_counter_start INTEGER := 0;
  l_suffix        VARCHAR2(3 CHAR);
  l_module   v$session.module%type;
  l_action   v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  set_action(p_action_name=>'TEMP_TABLES');

  debug_msg('TEMP_TABLES:recname='||p_recname);

  IF l_build_stats = 'N' AND l_deletetempstats = 'Y' THEN
    l_counter_start := 2; -- do not  build stats command in table build script
  ELSE 
    l_counter_start := 0; -- do build stats command in table build script
  END IF;

  FOR p_tables IN(
    SELECT   *
    FROM     gfc_ps_tables
    WHERE    table_type = 'T'
    AND	(recname LIKE p_recname OR p_recname IS NULL)
    ORDER BY recname)
  LOOP
--  ins_line(k_build,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
--  ins_line(k_build,LOWER('spool '||LOWER(l_scriptid)||'_'||l_dbname||'_'||p_tables.recname||'.lst'));
    signature(k_build,FALSE,l_scriptid,p_tables.recname);
    forceddldop(k_build);
    ddlpermit(k_build,TRUE); -- added 29.10.2007

--  ins_line(k_index,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
--  ins_line(k_index,LOWER('spool gfcindex_'||l_dbname||'_'||p_tables.recname||'.lst'));
    signature(k_index,FALSE,'gfcindex',p_tables.recname);
    forceddldop(k_index);
    FOR l_tempinstance IN 0..p_tables.temptblinstances LOOP

      	                        IF l_tempinstance > 0 THEN
        	                        l_suffix := LTRIM(TO_CHAR(l_tempinstance,'999'));
	                               	whenever_sqlerror(k_build,FALSE); -- ignore drop error 
                                ELSE
                                        l_suffix := '';
                                END IF;

                                ins_line(k_build,'DROP TABLE '||LOWER(l_schema2||p_tables.table_name||l_suffix)
				                        ||l_drop_purge_suffix);
				ins_line(k_build,'/');
                              	ins_line(k_build,'');
                               	whenever_sqlerror(k_build,TRUE);
                               	ins_line(k_build,'CREATE GLOBAL TEMPORARY TABLE '
				         ||LOWER(l_schema2||p_tables.table_name||l_suffix));
                                tab_cols(k_build,p_tables.recname, 'N');
                                ins_line(k_build,'ON COMMIT PRESERVE ROWS');
				ins_line(k_build,'/');
                                ins_line(k_build,'');

                              	mk_gt_indexes(p_tables.recname,p_tables.table_name,l_suffix);
                                ins_line(k_build,'');

                	        IF l_deletetempstats = 'Y' THEN
					l_counter := l_counter_start;

		                        WHILE l_counter <= 2 LOOP
						IF l_counter = 2 AND l_tempinstance = 0 THEN
--				                        ins_line(k_stats,'set echo on pause off verify on feedback on timi on autotrace off pause off lines '||k_max_line_length);
--				      	                ins_line(k_stats,LOWER('spool gfcstats_'||l_dbname||'_'||p_tables.recname||'.lst'));
				               	        signature(k_stats,FALSE,'gfcstats',p_tables.recname);
						END IF;
	        	                        ins_line(l_counter,'');
        	                                ins_line(l_counter,'BEGIN');
                	                        ins_line(l_counter,'sys.dbms_stats.delete_table_stats');
                        	                ins_line(l_counter,'(ownname=>'''||UPPER(l_schema1)||'''');
	                                        ins_line(l_counter,',tabname=>'''||UPPER(p_tables.table_name||l_suffix)||'''');
						IF l_oraver >= 10 THEN
		                                        ins_line(l_counter,',force=>TRUE);');
                 		                        ins_line(l_counter,'sys.dbms_stats.lock_table_stats');
                        		                ins_line(l_counter,'(ownname=>'''||UPPER(l_schema1)||'''');
                                                        ins_line(l_counter,',tabname=>'''||
							                     UPPER(p_tables.table_name||l_suffix)||''');');
						END IF;
       	                        	        ins_line(l_counter,'END;');
               	                        	ins_line(l_counter,'/');
                        	        	ins_line(l_counter,'');
                                		l_counter := l_counter + 2;
                        	        END LOOP;
				END IF;
	                        pause_sql(k_build);
    END LOOP;
    ddlpermit(k_build,FALSE); -- added 29.10.2007

    l_counter := l_counter_start;

    WHILE l_counter <= 2 LOOP
      ins_line(l_counter,'spool off');
      ins_line(l_counter,'');
      l_counter := l_counter + 2;
    END LOOP;

    ins_line(k_index,'spool off');
    ins_line(k_index,'');

  END LOOP;
  unset_action(p_action_name=>l_action);
END temp_tables;

---------------------------------------------------------------
PROCEDURE exec_sql
(p_sql VARCHAR2
) IS
BEGIN
  EXECUTE IMMEDIATE p_sql;
END exec_sql;
---------------------------------------------------------------
-- drop named table
-------------------------------------------------------------------------------------------------------
	PROCEDURE drop_table
	(p_table_name VARCHAR2
	) IS
		table_does_not_exist EXCEPTION;
		PRAGMA EXCEPTION_INIT(table_does_not_exist,-942);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		dbms_application_info.set_action(SUBSTR('DROP_TABLE '||p_table_name,1,32));

		EXECUTE IMMEDIATE 'DROP TABLE '||p_table_name||l_drop_purge_suffix;
		unset_action(p_action_name=>l_action);

	EXCEPTION
		WHEN table_does_not_exist THEN unset_action(p_action_name=>l_action);
	END drop_table;

---------------------------------------------------------------
-- gfc_ps_tab_columns holds a list of columns for tables to be recreated.   Any sub-records will be expanded recursively
-------------------------------------------------------------------------------------------------------
	PROCEDURE ddl_gfc_ps_tab_columns
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PS_TAB_COLUMNS');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ps_tab_columns';
		l_sql := l_sql||l_lf||'(recname    VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',fieldname  VARCHAR2(18 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',useedit    NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',fieldnum   NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',subrecname VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_tab_columns PRIMARY KEY(recname, fieldname)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
-- gfc_ora_tab_columns
-------------------------------------------------------------------------------------------------------
	PROCEDURE ddl_gfc_ora_tab_columns
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_ORA_TAB_COLUMNS');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ora_tab_columns';
		l_sql := l_sql||l_lf||'(table_name  VARCHAR2(30 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',column_name VARCHAR2(30 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',column_id   NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ora_tab_columns PRIMARY KEY(table_name, column_name)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ora_tab_columns_idx2 UNIQUE(table_name, column_id)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
-- to hold override parameters for function based indexes specified in partdata - 19.10.2007
-------------------------------------------------------------------------------------------------------
	PROCEDURE ddl_gfc_ps_idxddlparm
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PS_IDXDDLPARM');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ps_idxddlparm';
		l_sql := l_sql||l_lf||'(recname   VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',indexid	  VARCHAR2(18 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',parmname  VARCHAR2(8 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',parmvalue VARCHAR2(128 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_idxddlparm PRIMARY KEY(recname,indexid,parmname)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_part_ranges
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PART_RANGES');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_part_ranges';
		l_sql := l_sql||l_lf||'(part_id        VARCHAR2(8 CHAR) NOT NULL'; -- iD of partitioning schema
		l_sql := l_sql||l_lf||',part_no        NUMBER NOT NULL'; -- sequence number of range
		l_sql := l_sql||l_lf||',part_name      VARCHAR2(30 CHAR) NOT NULL'; -- this goes into the partition names
		l_sql := l_sql||l_lf||',part_value     VARCHAR2(100 CHAR) NOT NULL'; -- range less than value
		l_sql := l_sql||l_lf||',tab_tablespace VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',idx_tablespace VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',tab_storage    VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',idx_storage    VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_ranges PRIMARY KEY (part_id, part_no)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_ranges2 UNIQUE(part_id, part_name)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
-- gfc_ps_tables holds the records for which DDL scripts are to be regeneated by this script
-------------------------------------------------------------------------------------------------------
	PROCEDURE ddl_gfc_ps_tables
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PS_TABLES');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ps_tables';
		l_sql := l_sql||l_lf||'(recname          VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',table_name       VARCHAR2(18 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',table_type       VARCHAR2(1 CHAR)';
		l_sql := l_sql||l_lf||',rectype          NUMBER';
		l_sql := l_sql||l_lf||',temptblinstances NUMBER';
		l_sql := l_sql||l_lf||',override_schema  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',match_db         VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_tables PRIMARY KEY(recname)';
		l_sql := l_sql||l_lf||')';

		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
-- gfc_ps_indexdefn - expanded version of psindexdefn
-------------------------------------------------------------------------------------------------------
	PROCEDURE ddl_gfc_ps_indexdefn
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PS_INDEXDEFN');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ps_indexdefn';
		l_sql := l_sql||l_lf||'(recname      VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',indexid      VARCHAR2(1 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',subrecname   VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',subindexid   VARCHAR2(1 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',platform_ora NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',custkeyorder NUMBER NOT NULL'; -- 6.9.2007
		l_sql := l_sql||l_lf||',uniqueflag   NUMBER NOT NULL'; -- 6.9.2007    
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_indexdefn PRIMARY KEY(recname, indexid)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_indexdefn2 UNIQUE(subrecname, subindexid)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
-- gfc_ps_keydefn - expanded version of pskeydefn
-------------------------------------------------------------------------------------------------------
	PROCEDURE ddl_gfc_ps_keydefn
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PS_KEYDEFN');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ps_keydefn';
		l_sql := l_sql||l_lf||'(recname   VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',indexid   VARCHAR2(1 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',keyposn   NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',fieldname VARCHAR2(100 CHAR) NOT NULL'; -- 6.9.2007
		l_sql := l_sql||l_lf||',ascdesc   NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_keydefn PRIMARY KEY(recname,indexid,keyposn)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_keydefn2 UNIQUE(recname,indexid,fieldname)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_part_tables
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PART_TABLES');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_part_tables';
		l_sql := l_sql||l_lf||'(recname         VARCHAR2(30 CHAR) NOT NULL'; -- peoplesoft record name
		l_sql := l_sql||l_lf||',part_id         VARCHAR2(8 CHAR) NOT NULL'; -- iD of partitioning strategy.  Many tables can share one 
		l_sql := l_sql||l_lf||',part_column     VARCHAR2(100 CHAR) NOT NULL'; -- range partitioning column, or comma separated columns
		l_sql := l_sql||l_lf||',part_type       VARCHAR2(1 CHAR) NOT NULL'; -- (R)ange or (L)ist or (H)ash only 
		l_sql := l_sql||l_lf||' CONSTRAINT tables_part_type CHECK (part_type IN(''R'',''L'',''H''))';
		l_sql := l_sql||l_lf||',subpart_type    VARCHAR2(1 CHAR) 	DEFAULT ''N'''; -- (L)ist or (H)ash only 
		l_sql := l_sql||l_lf||' CONSTRAINT tables_subpart_type CHECK (subpart_type IN(''L'',''H'',''N''))';
		l_sql := l_sql||l_lf||',subpart_column  VARCHAR2(100 CHAR)'; -- sub partitioning column
		l_sql := l_sql||l_lf||',hash_partitions NUMBER DEFAULT 0 NOT NULL'; -- number of hash partitions
		l_sql := l_sql||l_lf||' CONSTRAINT tables_hash_partitions_pos CHECK(hash_partitions>=0)';
		l_sql := l_sql||l_lf||',tab_tablespace  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',idx_tablespace  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',tab_storage     VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',idx_storage     VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',stats_type	VARCHAR2(1 CHAR) DEFAULT ''Y''';
		l_sql := l_sql||l_lf||' CONSTRAINT tables_stats_type CHECK (stats_type IN(''Y'',''N'',''D''))';
		l_sql := l_sql||l_lf||',sample_size     NUMBER'; -- analyze sample size : null means auto sample size
		l_sql := l_sql||l_lf||',method_opt      VARCHAR2(100 CHAR)'; --override statistics clause in gather_table_stats
		l_sql := l_sql||l_lf||',override_schema VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_tables PRIMARY KEY(recname)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_tables_columns';
                l_sql := l_sql||       ' CHECK(part_column IS NOT NULL OR subpart_column IS NOT NULL)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_tables_types';
                l_sql := l_sql||       ' CHECK(part_type != subpart_type OR subpart_type = ''N'')';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_part_indexes
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PART_INDEXES');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_part_indexes';
		l_sql := l_sql||l_lf||'(recname         VARCHAR2(30 CHAR) NOT NULL'; -- peoplesoft record name
		l_sql := l_sql||l_lf||',indexid         VARCHAR2(1 CHAR) NOT NULL'; -- peoplesoft index id
		l_sql := l_sql||l_lf||',part_id         VARCHAR2(8 CHAR) NOT NULL'; -- iD of partitioning strategy.
		l_sql := l_sql||l_lf||',part_column     VARCHAR2(100 CHAR) NOT NULL'; -- range partitioning column, or comma separated columns
		l_sql := l_sql||l_lf||',part_type       VARCHAR2(1 CHAR) NOT NULL'; -- (R)ange or (L)ist or (H)ash only 
		l_sql := l_sql||       ' CONSTRAINT index_part_type CHECK (part_type IN(''R'',''L'',''H''))';
		l_sql := l_sql||l_lf||',subpart_type    VARCHAR2(1 CHAR) DEFAULT ''N'''; -- (L)ist or (H)ash only 
		l_sql := l_sql||       ' CONSTRAINT index_subpart_type CHECK (subpart_type IN(''L'',''H'',''N''))';
		l_sql := l_sql||l_lf||',subpart_column  VARCHAR2(100 CHAR)'; -- sub partitioning column
		l_sql := l_sql||l_lf||',hash_partitions NUMBER'; -- number of hash partitions
		l_sql := l_sql||       ' CONSTRAINT indexes_hash_partitions_pos CHECK(hash_partitions>=0)';
		l_sql := l_sql||l_lf||',idx_tablespace  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',idx_storage     VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',override_schema VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',CONSTRAINT index_gfc_part_tables PRIMARY KEY(recname, indexid)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_part_lists 
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PART_LISTS');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_part_lists';
		l_sql := l_sql||l_lf||'(part_id         VARCHAR2(8 CHAR) NOT NULL'; -- iD of partitioning schema
		l_sql := l_sql||l_lf||',part_no         NUMBER NOT NULL'; -- sequence number of range
		l_sql := l_sql||l_lf||',part_name       VARCHAR2(30 CHAR) NOT NULL'; -- this goes into the partition names
		l_sql := l_sql||l_lf||',list_value      VARCHAR2(1000) NOT NULL'; -- list value
		l_sql := l_sql||l_lf||',tab_tablespace  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',idx_tablespace  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',tab_storage     VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',idx_storage     VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_lists PRIMARY KEY (part_id, part_no)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_lists2 UNIQUE(part_id, part_name)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_part_subparts
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PART_RANGE_LISTS');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_part_subparts';
		l_sql := l_sql||l_lf||'(part_id         VARCHAR2(8 CHAR) NOT NULL'; -- iD of partitioning schema
		l_sql := l_sql||l_lf||',part_name       VARCHAR2(30 CHAR) NOT NULL'; -- this goes into the partition names
		l_sql := l_sql||l_lf||',subpart_name    VARCHAR2(30 CHAR) NOT NULL'; -- this goes into the partition names
		l_sql := l_sql||l_lf||',build           VARCHAR2(1 CHAR) DEFAULT ''Y'' NOT NULL';
		l_sql := l_sql||l_lf||' CONSTRAINT gfc_part_subparts_build CHECK (build IN(''Y'',''N''))';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_subparts PRIMARY KEY(part_id, part_name, subpart_name)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_temp_tables
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_TEMP_TABLES');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_temp_tables';
		l_sql := l_sql||l_lf||'(recname VARCHAR2(30 CHAR) NOT NULL'; -- peoplesoft record name
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_temp_tables PRIMARY KEY(recname)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_ddl_script
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_DDL_SCRIPT');

		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ddl_script';
		l_sql := l_sql||l_lf||'(type   NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',lineno NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',line   VARCHAR2(4000 CHAR)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_ps_alt_ind_cols
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PS_ALT_IND_COLS');

		l_sql := 'CREATE OR REPLACE VIEW gfc_ps_alt_ind_cols AS';
		l_sql := l_sql||l_lf||'SELECT   c.recname';
		l_sql := l_sql||l_lf||        ',LTRIM(TO_CHAR(RANK() over (PARTITION BY c.recname';
		l_sql := l_sql||l_lf||                                        'ORDER BY c.fieldnum)-1,''9'')) indexid';
		l_sql := l_sql||l_lf||        ',c.subrecname';
		l_sql := l_sql||l_lf||        ',LTRIM(TO_CHAR(RANK() over (PARTITION BY c.recname, c.subrecname ';
		l_sql := l_sql||l_lf||                                        'ORDER BY c.fieldnum)-1,''9'')) subindexid';
		l_sql := l_sql||l_lf||        ',c.fieldname';
		l_sql := l_sql||l_lf||        ',MOD(FLOOR(useedit/64),2) ascdesc';
		l_sql := l_sql||l_lf||'FROM     gfc_ps_tab_columns c';
		l_sql := l_sql||l_lf||'WHERE    MOD(FLOOR(useedit/16),2) = 1';
-- aND    RECNAME= 'JOB'
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

---------------------------------------------------------------
-- rem 11.2.2003 - view corrected to handled user indexes
-------------------------------------------------------------------------------------------------------
	PROCEDURE ddl_gfc_ps_keydefn_vw
	IS
		l_sql VARCHAR2(1000 CHAR);
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		set_action(p_action_name=>'DDL_GFC_PS_KEYDEFN_VW');

		l_sql := 'CREATE OR REPLACE VIEW gfc_ps_keydefn_vw AS';
		l_sql := l_sql||l_lf||'SELECT  j.recname, j.indexid';
		l_sql := l_sql||l_lf||       ',RANK() OVER (PARTITION BY j.recname, j.indexid';
		l_sql := l_sql||                          ' ORDER BY DECODE(j.custkeyorder,1,k.keyposn,c.fieldnum))';
		l_sql := l_sql||                          ' as keyposn'; -- 6.9.2007
		l_sql := l_sql||l_lf||       ',k.fieldname';
		l_sql := l_sql||l_lf||       ',c.fieldnum';
		l_sql := l_sql||l_lf||       ',RANK() OVER (PARTITION BY j.recname, j.indexid';
		l_sql := l_sql||                          ' ORDER BY c.fieldnum) as fieldposn';
		l_sql := l_sql||l_lf||       ',k.ascdesc';
		l_sql := l_sql||l_lf||'FROM    gfc_ps_indexdefn j';
		l_sql := l_sql||l_lf||       ',gfc_ps_tab_columns c'; -- 6.9.2007 removed psindexdefn
		l_sql := l_sql||l_lf||       ',pskeydefn k';
		l_sql := l_sql||l_lf||'WHERE   j.indexid = ''_''';
		l_sql := l_sql||l_lf||'AND     c.recname = j.recname';
		l_sql := l_sql||l_lf||'AND     k.recname = c.subrecname';
		l_sql := l_sql||l_lf||'AND     k.indexid = j.subindexid';
		l_sql := l_sql||l_lf||'AND     k.fieldname = c.fieldname';
		l_sql := l_sql||l_lf||'UNION ALL';
		l_sql := l_sql||l_lf||'SELECT  j.recname, j.indexid';
		l_sql := l_sql||l_lf||       ',RANK() OVER (PARTITION BY j.recname, j.indexid';
		l_sql := l_sql||                          ' ORDER BY k.keyposn) as keyposn';
		l_sql := l_sql||l_lf||       ',k.fieldname';
		l_sql := l_sql||l_lf||       ',c.fieldnum';
		l_sql := l_sql||l_lf||       ',RANK() OVER (PARTITION BY j.recname, j.indexid';
		l_sql := l_sql||                          ' ORDER BY c.fieldnum) as fieldposn';
		l_sql := l_sql||l_lf||       ',k.ascdesc';
		l_sql := l_sql||l_lf||'FROM    gfc_ps_indexdefn j';
		l_sql := l_sql||l_lf||       ',gfc_ps_tab_columns c'; -- 6.9.2007 removed psindexdefn
		l_sql := l_sql||l_lf||       ',pskeydefn k';
		l_sql := l_sql||l_lf||'WHERE   j.indexid BETWEEN ''A'' AND ''Z''';
		l_sql := l_sql||l_lf||'AND     c.recname = j.recname';
		l_sql := l_sql||l_lf||'AND     k.recname = c.recname';
--                                     AND     k.recname = c.subrecname???qwert
		l_sql := l_sql||l_lf||'AND     k.indexid = j.subindexid';
		l_sql := l_sql||l_lf||'AND     k.fieldname = c.fieldname';
		exec_sql(l_sql);

		unset_action(p_action_name=>l_action);
	END;

------------------------------------------------------------------------------------------------------
-- will make these public if I can disable syntax checking
------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------
-- spool script
-------------------------------------------------------------------------------------------------------
FUNCTION spooler
(p_type NUMBER DEFAULT 0) 
RETURN outrecset PIPELINED IS 
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>'SPOOLER');

  FOR i IN (
    SELECT * FROM gfc_ddl_script
    WHERE type = p_type
    ORDER BY lineno
  ) LOOP
    PIPE ROW(i.line);
  END LOOP;

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
  RETURN;
END spooler;
---------------------------------------------------------------
-- create all working storage tables
-------------------------------------------------------------------------------------------------------
PROCEDURE create_tables
(p_gtt BOOLEAN DEFAULT FALSE) 
IS
BEGIN
  ddl_gfc_ps_tables(p_gtt);
  ddl_gfc_ps_tab_columns(p_gtt);
  ddl_gfc_ora_tab_columns(p_gtt);
  ddl_gfc_ps_indexdefn(p_gtt);
  ddl_gfc_ps_keydefn(p_gtt);
  ddl_gfc_ps_idxddlparm(p_gtt);
  ddl_gfc_part_ranges(p_gtt);
  ddl_gfc_part_tables(p_gtt);
  ddl_gfc_part_indexes(p_gtt);
  ddl_gfc_part_lists(p_gtt);
  ddl_gfc_part_subparts(p_gtt);
  ddl_gfc_temp_tables(p_gtt);
  ddl_gfc_ddl_script(p_gtt);
 
  ddl_gfc_ps_alt_ind_cols;
  ddl_gfc_ps_keydefn_vw;

END create_tables;

---------------------------------------------------------------
-- drop named table
-------------------------------------------------------------------------------------------------------
PROCEDURE drop_tables IS
BEGIN
  read_context;
  oraver;

  drop_table('gfc_ps_tables');
  drop_table('gfc_ps_tab_columns');
  drop_table('gfc_ora_tab_columns');
  drop_table('gfc_ps_indexdefn');
  drop_table('gfc_ps_keydefn');
  drop_table('gfc_ddl_script');
  drop_table('gfc_ps_idxddlparm');
  drop_table('gfc_part_tables');
  drop_table('gfc_part_indexes');
  drop_table('gfc_part_ranges');
  drop_table('gfc_temp_tables');
  drop_table('gfc_part_lists');
  drop_table('gfc_part_subparts');
END drop_tables;

------------------------------------------------------------------------------------------------------
-- public procedures and functions
------------------------------------------------------------------------------------------------------
PROCEDURE banner IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>'BANNER');

  sys.dbms_output.enable(NULL);

  sys.dbms_output.put_line('GFC_PSPART - Partitioned/Global Temporary Table DDL generator for PeopleSoft');
  sys.dbms_output.put_line('(c)Go-Faster Consultancy - www.go-faster.co.uk 2001-2022');

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END;

------------------------------------------------------------------------------------------------------
-- read defaults from contexts
------------------------------------------------------------------------------------------------------
PROCEDURE display_defaults IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>'DISPLAY_DEFAULTS');

  banner;
  read_context;
  sys.dbms_output.put_line(CHR(10)||'PACKAGE DEFAULTS');
  sys.dbms_output.put_line(CHR(10)||
                           'Character VARCHAR2 definition          : '||l_chardef);
  sys.dbms_output.put_line('Rebuild tables with redo logging       : '||l_logging);
  sys.dbms_output.put_line('Enable parallelism for table copy      : '||l_parallel_table);
  sys.dbms_output.put_line('Enable parallel index build            : '||l_parallel_index);
  sys.dbms_output.put_line('Force Parallel DDL Degree              : '||l_force_para_dop);
  sys.dbms_output.put_line('Grant privileges to roles              : '||l_roles);
  sys.dbms_output.put_line('Name of update all role                : '||l_update_all);
  sys.dbms_output.put_line('Name of select all role                : '||l_read_all);
  sys.dbms_output.put_line('ID Prefix in files and project         : '||l_scriptid);
  sys.dbms_output.put_line('Drop indexes                           : '||l_drop_index);
  sys.dbms_output.put_line('Pause commands in build script         : '||l_pause);
  sys.dbms_output.put_line('Explicitly specify schema              : '||l_explicit_schema);
  sys.dbms_output.put_line('Block sample table statistics          : '||l_block_sample);
  sys.dbms_output.put_line('Analyze table immediately after rebuild: '||l_build_stats);
  sys.dbms_output.put_line('Delete and lock statistics on GTTs     : '||l_deletetempstats);
  sys.dbms_output.put_line('Force LONGs to CLOBs                   : '||l_longtoclob);
--sys.dbms_output.put_line('Name of DDL trigger to disable on build: '||l_ddltrigger);
  sys.dbms_output.put_line('Command to enable DDL lock trigger     : '||l_ddlenable);
  sys.dbms_output.put_line('Command to disable DDL lock trigger    : '||l_ddldisable);
  sys.dbms_output.put_line('Drop tables with PURGE option          : '||l_drop_purge);
--sys.dbms_output.put_line('No alter prefix                        : '||l_noalterprefix);
  sys.dbms_output.put_line('Force rebuild if no change             : '||l_forcebuild);
  sys.dbms_output.put_line('Force descending index                 : '||l_desc_index);
  sys.dbms_output.put_line('Repopulate default list sub-partition  : '||l_repopdfltsub);
  sys.dbms_output.put_line('Repopulate new max value partition     : '||l_repopnewmax);
  sys.dbms_output.put_line('Rename partitions and subpartitions    : '||l_rename_parts);--20.05.2014
  sys.dbms_output.put_line('Index rebuild on split partition       : '||l_split_index_update);--13.4.2014
  sys.dbms_output.put_line('Debug Level (0=disabled)               : '||l_debug_level);

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END display_defaults;

------------------------------------------------------------------------------------------------------
-- read defaults from contexts
------------------------------------------------------------------------------------------------------
PROCEDURE reset_defaults IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>'RESET_DEFAULTS');
  sys.dbms_output.enable(NULL);

  reset_variables;
  write_context;

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END reset_defaults;

------------------------------------------------------------------------------------------------------
-- set new defaults to contexts
------------------------------------------------------------------------------------------------------
PROCEDURE set_defaults
(p_chardef            VARCHAR2 DEFAULT ''
,p_logging            VARCHAR2 DEFAULT ''
,p_parallel_table     VARCHAR2 DEFAULT ''
,p_parallel_index     VARCHAR2 DEFAULT ''
,p_force_para_dop     VARCHAR2 DEFAULT ''
,p_roles              VARCHAR2 DEFAULT ''
,p_scriptid           VARCHAR2 DEFAULT ''
,p_update_all         VARCHAR2 DEFAULT ''
,p_read_all           VARCHAR2 DEFAULT ''
,p_drop_index         VARCHAR2 DEFAULT ''
,p_pause              VARCHAR2 DEFAULT ''
,p_explicit_schema    VARCHAR2 DEFAULT ''
,p_block_sample       VARCHAR2 DEFAULT ''
,p_build_stats        VARCHAR2 DEFAULT ''
,p_deletetempstats    VARCHAR2 DEFAULT ''
,p_longtoclob         VARCHAR2 DEFAULT ''
--,p_ddltrigger         VARCHAR2 DEFAULT '*'
,p_ddlenable          VARCHAR2 DEFAULT ''
,p_ddldisable         VARCHAR2 DEFAULT ''
,p_drop_purge         VARCHAR2 DEFAULT ''
--,p_noalterprefix      VARCHAR2 DEFAULT ''
,p_forcebuild         VARCHAR2 DEFAULT ''
,p_desc_index         VARCHAR2 DEFAULT ''
,p_repopdfltsub       VARCHAR2 DEFAULT ''
,p_repopnewmax        VARCHAR2 DEFAULT ''
,p_rename_parts       VARCHAR2 DEFAULT ''
,p_split_index_update VARCHAR2 DEFAULT '' --14.3.2022
,p_debug_level        INTEGER DEFAULT NULL
        ) IS
  l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		dbms_application_info.set_module(module_name=>k_module, action_name=>'SET_DEFAULTS');
		sys.dbms_output.enable(NULL);

		read_context; -- read current defaults
		IF p_chardef IS NOT NULL THEN
			l_chardef := p_chardef;
		END IF;

		IF p_logging IS NOT NULL THEN
			l_logging := p_logging;
		END IF;

		IF p_parallel_table IS NOT NULL THEN
			l_parallel_table := p_parallel_table;
		END IF;

		IF p_parallel_index IS NOT NULL THEN
			l_parallel_index := p_parallel_index;
		END IF;

  IF p_force_para_dop IS NOT NULL THEN
    l_force_para_dop := p_force_para_dop;
		END IF;
        
		IF p_roles IS NOT NULL THEN
			l_roles := p_roles;
		END IF;

		IF p_scriptid IS NOT NULL THEN
			l_scriptid := p_scriptid;
		END IF;

		IF p_update_all IS NOT NULL THEN
			l_update_all := p_update_all;
		END IF;

		IF p_read_all IS NOT NULL THEN
			l_read_all := p_read_all;
		END IF;

		IF p_drop_index IS NOT NULL THEN
			l_drop_index := p_drop_index;
		END IF;

		IF p_pause IS NOT NULL THEN
			l_pause := p_pause;
		END IF;

		IF p_explicit_schema IS NOT NULL THEN
			l_explicit_schema := p_explicit_schema;
		END IF;

		IF p_block_sample IS NOT NULL THEN
			l_block_sample := p_block_sample;
		END IF;

		IF p_build_stats IS NOT NULL THEN
			l_build_stats := p_build_stats;
		END IF;

		IF p_deletetempstats IS NOT NULL THEN
			l_deletetempstats := p_deletetempstats;
		END IF;

		IF p_longtoclob IS NOT NULL THEN
			l_longtoclob := p_longtoclob;
		END IF;

--  IF p_ddltrigger IS NULL OR p_ddltrigger != '*' THEN
--   l_ddltrigger := p_ddltrigger;
--  END IF;

		IF p_ddlenable IS NOT NULL THEN
			l_ddlenable := p_ddlenable;
		END IF;

		IF p_ddldisable IS NOT NULL THEN
			l_ddldisable := p_ddldisable;
		END IF;

		IF p_drop_purge IS NOT NULL THEN
			l_drop_purge := p_drop_purge;
		END IF;

--IF p_noalterprefix IS NOT NULL THEN
--  l_noalterprefix := p_noalterprefix;
--END IF;

		IF p_forcebuild IS NOT NULL THEN
			l_forcebuild := p_forcebuild;
		END IF;

		IF p_desc_index IS NOT NULL THEN
			l_desc_index := p_desc_index;
		END IF;

		IF p_repopdfltsub IS NOT NULL THEN
			l_repopdfltsub := p_repopdfltsub;
		END IF;

		IF p_repopnewmax IS NOT NULL THEN
			l_repopnewmax := p_repopnewmax;
		END IF;

		IF p_rename_parts IS NOT NULL THEN
			l_rename_parts := p_rename_parts; --20.05.2014
		END IF;

		IF p_split_index_update IS NOT NULL THEN --14.3.2022
			l_split_index_update := p_split_index_update;
		END IF;

		IF p_debug_level IS NOT NULL THEN
			l_debug_level := p_debug_level;
		END IF;
		write_context;

		dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
	END set_defaults;

------------------------------------------------------------------------------------------------------
---- make sure that working storage tables are empty
-------------------------------------------------------------------------------------------------------
	PROCEDURE truncate_tables 
	(p_all BOOLEAN DEFAULT FALSE
	) IS
		l_all NUMBER := 0;
		l_module v$session.module%type;
		l_action v$session.action%type;
	BEGIN
		dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
		dbms_application_info.set_module(module_name=>k_module, action_name=>'TRUNCATE_TABLES');
		sys.dbms_output.enable(NULL);

		IF p_all THEN
			l_all := 1;
		END IF;

		FOR p_tables IN (
			SELECT 	table_name
			FROM 	user_tables
			WHERE	(	l_all = 1
				AND	table_name IN( -- set up by partdata script
						'GFC_PS_IDXDDLPARM',
						'GFC_PART_TABLES',
						'GFC_PART_INDEXES',
						'GFC_PART_RANGES',
						'GFC_TEMP_TABLES',
						'GFC_PART_LISTS',
						'GFC_PART_RANGE_LISTS', --20.10.2014 left for legacy
						'GFC_PART_SUBPARTS', --20.10.2014 added
						'GFC_PS_INDEXDEFN', -- 16.6.2010 moved to truncate when all metadata cleared
						'GFC_PS_KEYDEFN' -- 16.6.2010 moved to truncate when all metadata cleared
					)
				)
			OR 	table_name IN( -- maintained by package
					'GFC_PS_TABLES',
					'GFC_PS_TAB_COLUMNS',
					'GFC_ORA_TAB_COLUMNS',
					'GFC_DDL_SCRIPT'
					)
		) LOOP

			EXECUTE IMMEDIATE 'TRUNCATE TABLE '||p_tables.table_name;

	 	END LOOP;

		dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
	END truncate_tables;

-------------------------------------------------------------------------------------------------------
-- this is the start of the processing
-------------------------------------------------------------------------------------------------------
PROCEDURE main 
(p_part_id     VARCHAR2 DEFAULT ''  -- build matching list of PART_IDs
,p_recname     VARCHAR2 DEFAULT ''  -- name of table(s) to be built-pattern matching possible-default null implies all
,p_rectype     VARCHAR2 DEFAULT 'A' -- build (P)artitioned tables, Global (T)emp tables, a(R)chive tables or (A)ll tables - default ALL
,p_projectname VARCHAR2 DEFAULT ''  -- build records in named Application Designer Project
)IS
  l_module v$session.module%type;
  l_action v$session.action%type;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>'MAIN');
  sys.dbms_output.enable(NULL);

  read_context;

  IF p_rectype IS NULL OR NOT p_rectype IN('A','T','P','R') THEN
   RAISE_APPLICATION_ERROR(-20001,'GFCBUILD: Parameter p_rectype, invalid value '''||p_rectype||'''');
  END IF;

         oraver;
         dbname;
         ptver;

  IF p_part_id IS NULL AND p_recname IS NULL AND p_rectype = 'A' THEN --only clear output tables before generating all records
          truncate_tables;
  END IF;

  gfc_ps_tables(p_part_id=>p_part_id
               ,p_recname=>p_recname
               ,p_rectype=>p_rectype);

  gfc_project(p_projectname=>p_projectname);

  gfc_ps_tab_columns(p_part_id=>p_part_id
                    ,p_recname=>p_recname);

-- expand_sbr; -- 7.3.2012 if use psrecfielddb do not  need to expand sub-records
-- shuffle_long;
  match_db;

  gfc_ps_indexdefn(p_part_id=>p_part_id
                  ,p_recname=>p_recname);
  gfc_ps_keydefn(p_recname=>p_recname);

  IF p_rectype IN('A','P') OR p_rectype IS NULL THEN
    create_roles;
    part_tables(p_part_id=>p_part_id
               ,p_recname=>p_recname);
  END IF;

  IF p_rectype IN('A','P','R') OR p_rectype IS NULL THEN
    arch_tables(p_part_id=>p_part_id
                ,p_recname=>p_recname);
  END IF;

  IF p_rectype IN('A','T') OR p_rectype IS NULL THEN
    temp_tables(p_recname=>p_recname);
  END IF;

  commit;

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
 END main;

END gfc_pspart;
/
set echo off termout on
show errors
spool off
