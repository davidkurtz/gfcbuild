clear screen
set serveroutput on echo on timi on
spool x

-----------------------------------------------------------------------------------------------------------
--The archiving package requires the following explicit privileges in xchg_privs
-----------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE psarch.x AS
 PROCEDURE main
	(p_max_parts INTEGER  DEFAULT NULL
	,p_recname   VARCHAR2 DEFAULT NULL
	,p_partname  VARCHAR2 DEFAULT NULL
	,p_testmode  BOOLEAN  DEFAULT FALSE);
 PROCEDURE fix
	(p_recname   VARCHAR2 DEFAULT NULL
	,p_testmode  BOOLEAN  DEFAULT FALSE);
END x;
/
show errors


CREATE OR REPLACE PACKAGE BODY psarch.x AS
 l_errno       INTEGER := -20000; /* set a valid default in case of error in trigger*/
 l_msg         VARCHAR2(200) := 'Unexpected Error has occured';
 e_generate_message EXCEPTION;

FUNCTION num_rows(p_table VARCHAR2) RETURN INTEGER IS
  l_sql VARCHAR2(100);
  l_num_rows INTEGER;
BEGIN
  l_sql := 'SELECT COUNT(*) num_rows FROM '||p_table;
--dbms_output.put_line(p_table);
  EXECUTE IMMEDIATE l_sql INTO l_num_rows;
  dbms_output.put_line('Info: '||p_table||': '||l_num_rows||' rows');
  RETURN l_num_rows;
END num_rows;

PROCEDURE msg(p_msg VARCHAR2) IS
BEGIN
    dbms_output.put_line(TO_CHAR(SYSDATE,'hh24:mi:ss dd.mm.yyyy')||':'||p_msg);
END msg;

PROCEDURE check_empty(p_table VARCHAR2, p_testmode BOOLEAN DEFAULT FALSE) IS
  l_num_rows INTEGER;
BEGIN
  l_num_rows := num_rows(p_table);
  IF l_num_rows > 0 THEN
   l_msg := 'Table '||p_table||' has '||l_num_rows||' rows when it should be empty.';
   IF p_testmode THEN
    msg('Test Mode:'||l_msg);
   ELSE
    RAISE e_generate_message;
   END IF;
  END IF;
END check_empty;

PROCEDURE exec_sql(p_sql VARCHAR2, p_testmode BOOLEAN DEFAULT FALSE) IS
BEGIN
    IF p_testmode THEN NULL;
     msg('Test SQL: '||p_sql);
    ELSE
     msg(p_sql);
     EXECUTE IMMEDIATE p_sql;
    END IF;
END exec_sql;


PROCEDURE fix
	(p_recname   VARCHAR2 DEFAULT NULL
	,p_testmode  BOOLEAN  DEFAULT FALSE) IS
  l_sql VARCHAR2(1000);
  l_query VARCHAR2(500);
  l_counter INTEGER;
BEGIN
  FOR x IN (
	SELECT	ta.owner arch_table_owner
	,	ta.table_name arch_table_name
	,	p.recname
	,	p.noarch_condition
	,	p.part_id
	,	p.part_column
	,	DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename) table_name
	FROM	sysadm.gfc_part_tables p
		LEFT OUTER JOIN sysadm.psrecdefn ra
		ON ra.recname = p.arch_recname
	,	sysadm.psrecdefn r
	,	dba_tables ta
	WHERE	r.recname = p.recname
	AND	p.arch_flag = 'A'
	AND	p.noarch_condition IS NOT NULL
	AND	ta.owner = COALESCE(p.arch_schema,p.override_schema,'SYSADM')
	AND	ta.table_name = COALESCE(p.arch_table_name
					,DECODE(ra.sqltablename,' ','PS_'||ra.recname, ra.sqltablename)
					,DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename))
	AND	(r.recname LIKE p_recname OR p_recname IS NULL)
	ORDER BY p.recname
  ) LOOP
    l_sql := 'LOCK TABLE '||x.arch_table_owner||'.'||x.arch_table_name||' IN EXCLUSIVE MODE';
    exec_sql(l_sql,p_testmode);

    l_query := 'FROM '||x.arch_table_owner||'.'||x.arch_table_name||' X WHERE '||x.noarch_condition;

    l_sql := 'INSERT INTO sysadm.'||x.table_name||' SELECT * '||l_query;
    exec_sql(l_sql,p_testmode);
    l_counter := SQL%ROWCOUNT;
    msg(TO_CHAR(l_counter)||' rows inserted.');

    IF l_counter > 0 THEN
      l_sql := 'DELETE '||l_query;
      exec_sql(l_sql,p_testmode);
      msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
    END IF;

    commit;

  END LOOP;
END;


PROCEDURE main
	(p_max_parts INTEGER  DEFAULT NULL
	,p_recname   VARCHAR2 DEFAULT NULL
	,p_partname  VARCHAR2 DEFAULT NULL
	,p_testmode  BOOLEAN  DEFAULT FALSE) IS
  l_sql VARCHAR2(1000);
  l_criteria VARCHAR2(200);
  l_table VARCHAR2(200);
  l_counter INTEGER;
  l_high_value VARCHAR2(32767);
BEGIN
  FOR x IN (
	SELECT	p1.table_owner
	,	p1.table_name 
	,	p1.partition_name
	,	pr.part_name
	,	ta.owner arch_table_owner
	,	tx.table_name xchg_table_name
	,	ta.table_name arch_table_name
	,	p2.partition_name arch_partition_name
	, 	p2.partition_position
	,	p.recname
	,	p.noarch_condition
	,	p.part_id
	,	p.part_column
	FROM	sysadm.gfc_part_tables p
		LEFT OUTER JOIN sysadm.psrecdefn ra
		ON ra.recname = p.arch_recname
	,	sysadm.gfc_part_ranges pr
	,	sysadm.psrecdefn r
	,	dba_tables tx
	,	dba_tables ta
	,	dba_tab_partitions p1
	,	dba_tab_partitions p2
	WHERE	ta.owner = COALESCE(p.arch_schema,p.override_schema,'SYSADM')
	AND	ta.table_name = COALESCE(p.arch_table_name
					,DECODE(ra.sqltablename,' ','PS_'||ra.recname, ra.sqltablename)
					,DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename))
	AND	r.recname = p.recname
	AND	pr.part_id = p.part_id
	AND	pr.arch_flag = 'A'
	AND	tx.owner = ta.owner
	AND	tx.table_name = 'XCHG_'||p.recname
	AND	p1.table_owner = 'SYSADM'
	AND	p1.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname, r.sqltablename)
	AND	p1.partition_name = r.recname||'_'||pr.part_name
	AND	p2.table_owner = ta.owner
	AND	p2.table_name = ta.table_name
	AND	p2.partition_name = COALESCE(p.arch_table_name,p.arch_recname,p.recname)||'_'||pr.part_name
--
	AND	(rownum <= p_max_parts OR p_max_parts IS NULL OR p_max_parts<0) --restrict number of partitions processed
	AND	(r.recname LIKE p_recname OR p_recname IS NULL)
 	AND 	(p2.partition_name LIKE p_partname OR p_partname IS NULL)
--testing criteria
--	AND	p.noarch_condition IS NULL --restrict to tables where no data is retain
	ORDER BY p.recname, pr.part_no
  ) LOOP
    msg('About to archive '||x.table_owner||'.'||x.table_name||' partition '||x.partition_name
				||' via table '||x.arch_table_owner||'.'||x.xchg_table_name
				||' to '||x.arch_table_owner||'.'||x.arch_table_name||' partition '||x.arch_partition_name);  


    --check exchange table is empty
    l_table := x.arch_table_owner||'.'||x.xchg_table_name;
    check_empty(l_table, p_testmode);
    --exchange partition in base table with xchange table
    l_sql := 'ALTER TABLE '||x.table_owner||'.'||x.table_name
				||' EXCHANGE PARTITION '||x.partition_name
				||' WITH TABLE '||x.arch_table_owner||'.'||x.xchg_table_name
				||' INCLUDING INDEXES WITH VALIDATION UPDATE GLOBAL INDEXES';
    exec_sql(l_sql,p_testmode);

    --check partition in app table is empty
    l_table := x.table_owner||'.'||x.table_name||' PARTITION('||x.partition_name||')';
    check_empty(l_table, p_testmode);

    --drop empty partition in app table
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
      msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
      l_sql := 'DELETE FROM '||x.arch_table_owner||'.'||x.xchg_table_name||' x'
				||' WHERE '||x.noarch_condition;
      exec_sql(l_sql,p_testmode);
      msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
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

    IF x.part_id = 'AUD' THEN
     NULL; --dont check for dups in audit
    ELSE
     --check for rows in exchange table and also in archive table
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

     l_sql := 'DELETE /*+LEADING(X)*/ FROM '||x.arch_table_owner||'.'||x.arch_table_name
             ||' a WHERE ('||l_criteria||') IN (SELECT '||l_criteria||' FROM '||x.arch_table_owner||'.'||x.xchg_table_name||' x';
     IF l_high_value IS NOT NULL THEN
      l_sql := l_sql||' WHERE x.'||x.part_column||' < '||l_high_Value;
     END IF;
     l_sql := l_sql||')';
     exec_sql(l_sql,p_testmode);
     msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
    END IF;

    IF l_high_value IS NOT NULL THEN
      --REPLACE(l_high_Value,'''','''''')
      l_criteria := 'FROM '||x.arch_table_owner||'.'||x.xchg_table_name||' x WHERE x.'||x.part_column||' < '||l_high_Value;
      l_sql := 'INSERT INTO '||x.arch_table_owner||'.'||x.arch_table_name||' SELECT * '||l_criteria;
      exec_sql(l_sql,p_testmode);
      msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
      l_sql := 'DELETE '||l_criteria;
      exec_sql(l_sql,p_testmode);
      msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
    END IF;


    --exchange data from exchange staging table into archive table
    l_sql := 'ALTER TABLE '||x.arch_table_owner||'.'||x.arch_table_name
				||' EXCHANGE PARTITION '||x.arch_partition_name
				||' WITH TABLE '||x.arch_table_owner||'.'||x.xchg_table_name
				||' INCLUDING INDEXES WITH VALIDATION UPDATE GLOBAL INDEXES';
    exec_sql(l_sql,p_testmode);

    --check that the exchange table is empty
    l_table := x.arch_table_owner||'.'||x.xchg_table_name;
    check_empty(l_table, p_testmode);

    msg('Partition '||x.part_name||' Archived');   
    dbms_output.put_line('.');   

  END LOOP;
EXCEPTION
  WHEN e_generate_message THEN /*reraise custom exception*/
    RAISE_APPLICATION_ERROR(l_errno,l_msg);
END main;
END x;
/


show errors
pause


----------------------------------------------------------------------------------------------------
--script generate commands that will be issued during archive, but without issuing them
----------------------------------------------------------------------------------------------------

set wrap on long 5000 lines 500 serveroutput on 
spool x-test
execute psarch.x.main(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_ADHOC_DTL');
execute psarch.x.main(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_MNG_SCH_TBL');
execute psarch.x.main(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_ASSIGN');
execute psarch.x.main(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_DEFN_DTL');
execute psarch.x.main(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_DEFN_ROTATN');
execute psarch.x.main(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_DEFN_SHFT');
execute psarch.x.main(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_DEFN_TBL');
execute psarch.x.main(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'AUDIT_SCH_TBL');
spool off
pause

set serveroutput on timi on
pause



/*--------------------------------------------------------------------------------------------------
--The following commands in this script are comments provided as an example
----------------------------------------------------------------------------------------------------
--These are the global indexes to be dropped during archiving and replaced afterwards.  This will 
--also improve performance of archiving process.
----------------------------------------------------------------------------------------------------

drop index SYSADM.PS_SCH_DEFN_DTL
/
CREATE UNIQUE INDEX sysadm.ps_sch_defn_dtl ON sysadm.ps_sch_defn_dtl
(schedule_id ,setid ,sch_adhoc_ind ,effdt ,daynum ,shiftnum )
TABLESPACE PSINDEX PCTFREE 1 COMPRESS 4 PARALLEL NOLOGGING
/
ALTER INDEX sysadm.ps_sch_defn_dtl LOGGING
/
ALTER INDEX sysadm.ps_sch_defn_dtl NOPARALLEL
/



/*--------------------------------------------------------------------------------------------------
--script to calculate size of archive tables
----------------------------------------------------------------------------------------------------
column table_owner format a12
column table_name format a18
column partition_name format a25
compute sum of MB on report
break on report
select	tablespace_name
, 	sum(bytes)/1024/1024 Mb
,	sum(extents) extents
,	count(*) partitions
,	count(distinct segment_name) segments
from	dba_segments
where	partition_name IS NOT NULL
and	owner = 'PSARCH'
group by tablespace_name
order by 1
/




/*--------------------------------------------------------------------------------------------------
--commands to check for and correct over enthusiastic archivng.  If the archive is run, and then the
--archive meta data is changed, this will take rows from the archive and move them back into the 
--live table
--WARNING - it is slow
----------------------------------------------------------------------------------------------------

set wrap on long 5000 lines 500 serveroutput on timi on
spool x-fix
execute psarch.x.fix;
--execute psarch.x.fix(p_recname=>'SCH_DEFN_TBL');
--execute psarch.x.fix(p_recname=>'SCH_DEFN_DTL');
--execute psarch.x.fix(p_recname=>'SCH_DEFN_ROTATN');
--execute psarch.x.fix(p_recname=>'SCH_DEFN_SHFT');
--execute psarch.x.fix(p_recname=>'SCH_ASSIGN');
spool off



/*


----------------------------------------------------------------------------------------------------
--useful query to report on progress of package
----------------------------------------------------------------------------------------------------
set long 5000 lines 120
SELECT 	x.sql_id
,	x.sql_plan_hash_value
,	x.ash_secs
,	x.max_sample_time
,	(x.max_sample_time - LAG(x.max_sample_time,1) 
	OVER (PARTITION BY x.session_id ORDER BY x.max_sample_time))*86400 secs
,	x.xid
,	NVL(s.sql_fulltext,t.sql_text) sql_text
FROM 	(
	select d.dbid, h.session_id 
	, h.sql_id, h.sql_plan_hash_value, h.sql_child_number
 	, h.xid
	, count(*) ash_secs
	, min(h.sample_time)+0 min_sample_time
	, max(h.sample_time)+0 max_sample_time
	from dba_users u
	, v$database d
	, v$active_session_history h
	where u.username = 'PSARCH'
	and u.user_id = h.user_id
	group by d.dbid, h.session_id, h.sql_id, h.sql_plan_hash_value, h.sql_child_number, h.xid
	) x
 	LEFT OUTER JOIN v$sql s ON s.sql_id = x.sql_id AND s.child_number = x.sql_child_number
	LEFT OUTER JOIN dba_hist_sqltext t ON t.sql_id = x.sql_id AND t.dbid = x.dbid
WHERE max_sample_time >= SYSDATE - 30/1440
--WHERE sql_text IS NOT NULL
order by max_sample_time
/



----------------------------------------------------------------------------------------------------
--This is an example of how to archive 1 partition from each table
----------------------------------------------------------------------------------------------------

set long 5000 wrap on lines 111 timi on serveroutput on
spool x-run1
execute psarch.x.main(p_max_parts=>1,p_recname=>'SCH_ADHOC_DTL');
execute psarch.x.main(p_max_parts=>1,p_recname=>'SCH_MNG_SCH_TBL');
execute psarch.x.main(p_max_parts=>1,p_recname=>'SCH_ASSIGN');
execute psarch.x.main(p_max_parts=>1,p_recname=>'SCH_DEFN_DTL');
execute psarch.x.main(p_max_parts=>1,p_recname=>'SCH_DEFN_ROTATN');
execute psarch.x.main(p_max_parts=>1,p_recname=>'SCH_DEFN_SHFT');
execute psarch.x.main(p_max_parts=>1,p_recname=>'SCH_DEFN_TBL');
execute psarch.x.main(p_max_parts=>1,p_recname=>'AUDIT_SCH_TBL');
spool off


----------------------------------------------------------------------------------------------------
--example PL/SQL calls to refresh stats on tables
----------------------------------------------------------------------------------------------------
begin
    sys.dbms_stats.gather_table_Stats
      (ownname => 'SYSADM'
      ,tabname => 'PS_SCH_MNG_SCH_TBL'
      ,estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE
      ,method_opt => 'FOR ALL COLUMNS SIZE 1'
      ,cascade => TRUE
      ,granularity => 'GLOBAL'
      );
end;
/
begin
    sys.dbms_stats.gather_table_Stats
      (ownname => 'PSARCH'
      ,tabname => 'PS_AUDIT_SCH_TBL'
      ,estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE
      ,method_opt => 'FOR ALL COLUMNS SIZE 1'
--      ,block_sample => TRUE
      ,cascade => TRUE
      ,granularity => 'GLOBAL'
      );
end;
/
*/



*/
