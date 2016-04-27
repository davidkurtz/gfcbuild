clear screen
set serveroutput on echo on timi on
spool wms_archive

----------------------------------------------------------------------------------------------------
--script generate commands that will be issued during archive, but without issuing them
----------------------------------------------------------------------------------------------------

set wrap on long 5000 lines 500 serveroutput on 
spool x-test
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_ADHOC_DTL');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_MNG_SCH_TBL');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_ASSIGN');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_DEFN_DTL');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_DEFN_ROTATN');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_DEFN_SHFT');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'SCH_DEFN_TBL');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>2,p_testmode=>TRUE,p_recname=>'AUDIT_SCH_TBL');

set wrap on long 5000 lines 500 serveroutput on 
execute /*AUDW6M*/  sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_SCH_TBL');

execute /*AUDMP6M*/  sysadm.gfc_archive.purge_range_part(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_NAMES'); 

execute /*AUDM1Y2Y*/ sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_GPACMUSER',p_exchvald=>TRUE); 
execute /*AUDM1Y2Y*/ sysadm.gfc_archive.purge_arch_range_part(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_GPACMUSER'); 

execute /*AUDW1Y2Y*/ sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_TLRPTTIME',p_exchvald=>TRUE); 
execute /*AUDW1Y2Y*/ sysadm.gfc_archive.purge_arch_range_part(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_TLRPTTIME'); 

execute /*AUDMA3Y*/  sysadm.gfc_archive.arch_range_part(p_max_parts=>10,p_testmode=>TRUE,p_recname=>'AUDIT_JOB',p_exchvald=>TRUE); 


execute /*AUDW6M*/   sysadm.gfc_archive.main(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_SCH_TBL');
execute /*AUDMP6M*/  sysadm.gfc_archive.main(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_NAMES'); 
execute /*AUDM1Y2Y*/ sysadm.gfc_archive.main(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_GPACMUSER',p_exchvald=>TRUE); 
execute /*AUDW1Y2Y*/ sysadm.gfc_archive.main(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_TLRPTTIME',p_exchvald=>TRUE); 
execute /*AUDMA3Y*/  sysadm.gfc_archive.main(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_JOB',p_exchvald=>TRUE); 



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
drop INDEX sysadm.ps_sch_assign 
/



CREATE UNIQUE INDEX sysadm.ps_sch_defn_dtl ON sysadm.ps_sch_defn_dtl
(schedule_id ,setid ,sch_adhoc_ind ,effdt ,daynum ,shiftnum )
TABLESPACE PSINDEX PCTFREE 1 COMPRESS 4 PARALLEL NOLOGGING
/
ALTER INDEX sysadm.ps_sch_defn_dtl LOGGING
/
ALTER INDEX sysadm.ps_sch_defn_dtl NOPARALLEL
/
CREATE UNIQUE INDEX sysadm.ps_sch_assign ON sysadm.ps_sch_assign
(emplid, empl_rcd, effdt) 
TABLESPACE PSINDEX PCTFREE 1 COMPRESS 2 PARALLEL NOLOGGING
/
ALTER INDEX sysadm.ps_sch_assign LOGGING
/
ALTER INDEX sysadm.ps_sch_assign NOPARALLEL
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
spool x-fix-overarchive
execute sysadm.gfc_archive.fix_overarchive;
--execute sysadm.gfc_archive.fix_overarchive(p_recname=>'SCH_DEFN_TBL');
--execute sysadm.gfc_archive.fix_overarchive(p_recname=>'SCH_DEFN_DTL');
--execute sysadm.gfc_archive.fix_overarchive(p_recname=>'SCH_DEFN_ROTATN');
--execute sysadm.gfc_archive.fix_overarchive(p_recname=>'SCH_DEFN_SHFT');
--execute sysadm.gfc_archive.fix_overarchive(p_recname=>'SCH_ASSIGN');
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
WHERE max_sample_time >= SYSDATE - 10/1440
--WHERE sql_text IS NOT NULL
order by max_sample_time
/


----------------------------------------------------------------------------------------------------
--Query to report on size of partitions
----------------------------------------------------------------------------------------------------

set lines 120
column num_rows format 9,999,999,999
column Mb format 999,999
column recname format a15
column table_name format a18
column min_part_name format a21
column max_part_name format a21
column parts  format 999 heading 'Used|Prts'
column parts2 format 999 heading 'All|Prts'
WITH x AS (
	SELECT	t.recname
	,	DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
	, 	arch_schema
	FROM	sysadm.gfc_part_tables t
	,	sysadm.psrecdefn r
	WHERE	t.arch_schema IS NOT NULL
	AND	r.recname = t.recname
	)
, y AS (
	SELECT 	x.recname, x.table_name
	,	MIN(partition_name) min_part_name
	,	MAX(partition_name) max_part_name
	,	SUM(SIGN(num_rows)) parts, COUNT(*) parts2, SUM(blocks)/128 Mb, SUM(num_rows) nuM_rows
	FROM 	x
	,	dba_tab_partitions a
	WHERE	a.table_owner = 'SYSADM'
	AND	x.table_name = a.table_name
	GROUP BY x.recname, x.table_name
	)
, z AS (
	SELECT 	x.recname, x.table_name
	,	MIN(partition_name) min_part_name
	,	MAX(partition_name) max_part_name
	,	SUM(SIGN(num_rows)) parts, COUNT(*) parts2, SUM(blocks)/128 Mb, SUM(num_rows) nuM_rows
	FROM 	x
	,	dba_tab_partitions a
	WHERE	a.table_owner = x.arch_schema
	AND	a.table_name = x.table_name
	GROUP BY x.recname, x.table_name
	)
SELECT y.min_part_name
, y.parts, y.parts2, y.mb, y.num_rows
, z.max_part_name
, z.parts, z.parts2, z.mb, z.num_rows
FROM y
	LEFT OUTER JOIN z
	ON y.table_name = z.table_name
ORDER BY 1
/

----------------------------------------------------------------------------------------------------
--Query to report table partition DDLs in the last 10 minutes
----------------------------------------------------------------------------------------------------

compute avg of secs_since_prev on object_name
break on owner on object_name skip 1
column secs_since_prev format 9999
column owner format a8
column object_name format a18
column subobject_name format a21
select * from (
select owner
, objecT_name, subobject_name
, lasT_ddl_time
, (last_ddl_time-lag(last_ddl_time,1) over (partition by owner, object_name order by subobject_name))*86400 as secs_since_prev
from dba_objects
where object_type = 'TABLE PARTITION'
and last_ddl_time > sysdate - 35/1440
) where last_ddl_time > sysdate - 30/1440
/




----------------------------------------------------------------------------------------------------
--This is an example of how to archive 1 partition from each table
----------------------------------------------------------------------------------------------------

set long 5000 wrap on lines 111 timi on serveroutput on
spool x-run1
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'SCH_ADHOC_DTL');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'SCH_MNG_SCH_TBL');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'SCH_ASSIGN');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'SCH_DEFN_DTL');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'SCH_DEFN_ROTATN');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'SCH_DEFN_SHFT');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'SCH_DEFN_TBL');
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'AUDIT_SCH_TBL');
spool off


set wrap on long 5000 lines 500 serveroutput on timi on
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'AUDIT_SCH_TBL');
execute sysadm.gfc_archive.purge_range_part(p_max_parts=>1,p_recname=>'AUDIT_NAMES'); 

execute sysadm.gfc_archive.purge_range_part(p_max_parts=>1,p_recname=>'AUDIT_GPABSEVTJ'); 
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'AUDIT_GPACMUSER',p_exchvald=>TRUE); 
execute sysadm.gfc_archive.purge_arch_range_part(p_max_parts=>1,p_recname=>'AUDIT_GPACMUSER'); 

execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'AUDIT_TLRPTTIME',p_exchvald=>TRUE); 
execute sysadm.gfc_archive.purge_arch_range_part(p_max_parts=>1,p_recname=>'AUDIT_TLRPTTIME'); 

execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_recname=>'AUDIT_JOB',p_exchvald=>TRUE); 

--qwert
execute sysadm.gfc_archive.main(p_max_parts=>-1,p_recname=>'AUDIT_NAMES'); 
execute sysadm.gfc_archive.main(p_max_parts=>-1,p_recname=>'AUDIT_GPACMUSER',p_exchvald=>TRUE); 
execute sysadm.gfc_archive.main(p_max_parts=>-1,p_recname=>'AUDIT_TLRPTTIME',p_exchvald=>TRUE); 
execute sysadm.gfc_archive.main(p_max_parts=>-1,p_recname=>'AUDIT_JOB',p_exchvald=>TRUE); 


execute /*AUDW6M*/ sysadm.gfc_archive.purge_arch_range_part(p_max_parts=>1,p_testmode=>TRUE,p_recname=>'AUDIT_SCH_TBL');



BEGIN 
 FOR i IN(
select part_id, recname 
from sysadm.gfc_part_tables
where part_id like 'AUD%'
and recname = 'AUDIT_SCH_TBL'
) LOOP
sysadm.gfc_archive.purge_range_part(p_recname=>i.recname); 
sysadm.gfc_archive.purge_arch_range_part(p_recname=>i.recname); 
sysadm.gfc_archive.arch_range_part(p_recname=>i.recname);
END LOOP;
END;
/



set wrap on long 5000 lines 500 serveroutput on timi on
execute sysadm.gfc_archive.purge_range_part(p_max_parts=>100); 
execute sysadm.gfc_archive.purge_arch_range_part(p_max_parts=>20); 

execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_testmode=>TRUE); 
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1); 
execute sysadm.gfc_archive.arch_range_part; 





execute sysadm.gfc_archive.main(p_max_parts=>1); 
execute sysadm.gfc_archive.main; 


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



with t as (
select tablespace_name, sum(blocks) blocks
, max(block_id+blocks) max_block_id
from dba_free_space
where tablespace_name like 'AUD%TAB'
group by tablespace_name
having sum(blocks) >=100000
order by blocks)
, s as (
select * from ( 
 select tablespace_name, owner, segment_type, segment_name, partition_name, block_id
 , row_number() over (partition by tablespace_name order by block_id desc) as ranking
 from dba_extents
 ) 
 where ranking = 1
)
select /*+LEADING(T)*/ s.*
from s, t
where s.tablespace_name = t.tablespace_name
and s.block_id > t.max_block_id
/


--coalesce list?
select 'ALTER TABLESPACE '||tablespace_name||' COALESCE;'
from (
select DISTINCT a.tablespace_name 
from dba_free_space a, dba_Free_space b
where a.tablespace_name = b.tablespace_name
and a.file_id = b.file_id
and b.block_id = a.block_id+a.blocks
and a.tablespace_name != 'UNDOTBS1'
)
/



with x as (
select r.part_no, r.part_name, r.arch_Flag
,	DECODE(d.sqltablename,' ','PS_'||d.recname,d.sqltablename) table_name
,     p.recname||'_'||r.part_name partition_name
from	sysadm.gfc_part_tables p
,	sysadm.gfc_part_Ranges r
,	sysadm.psrecdefn d
where	p.part_id like 'AUD%'
and	p.recname = 'AUDIT_NAMES'
and   r.part_id = p.part_id
and	p.part_type = 'R'
and	d.recname = p.recname)
, y as (
select * 
from user_tab_partitions
where table_name = 'PS_AUDIT_NAMES'
) 
select x.part_no, x.arch_flag, x.partition_name, y.partition_name
from x
 full outer join y
 on x.table_name = y.table_name
 and x.partition_name = y.partition_name
order by 1
/


set wrap on long 5000 lines 111 serveroutput on 
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>1,p_testmode=>TRUE); 
execute sysadm.gfc_archive.arch_range_part(p_max_parts=>3); 



set lines 85
column owner format a8
column table_name format a20
column column_name format a20
column data_type format a20
select t1.owner, t1.table_name, t1.column_name, t1.column_id, t1.data_type
,      t2.owner, t2.table_name, t2.column_name, t2.column_id, t2.data_type
,      t3.owner, t3.table_name, t3.column_name, t3.column_id, t3.data_type
from	dba_tab_columns t1
	full outer join dba_tab_columns t2
	on t2.owner = 'PSARCH'
	and t2.table_name = 'XCHG_AUDIT_TL_PAY_TM'
	and t2.column_name = t1.column_name
	full outer join dba_tab_columns t3
	on t3.owner = 'PSARCH'
	and t3.table_name = t1.table_name
	and t3.column_name = t1.column_name
where t1.table_name = 'PS_AUDIT_TL_PAY_TM'
and t1.owner = 'SYSADM'


set serveroutput on 
execute wms_archive.compress_all_parts(p_tabname=>'%AUDIT%',p_testmode=>TRUE);
execute wms_archive.compress_all_parts(p_tabname=>'%AUDIT%TLRPTTIME%');

execute wms_archive.compress_parts(p_ownname=>'PSARCH');


CREATE TABLESPACE aud1ytab DATAFILE '+DATA' SIZE 2M AUTOEXTEND ON NEXT 1M MAXSIZE UNLIMITED;
CREATE TABLESPACE aud1yidx DATAFILE '+DATA' SIZE 2M AUTOEXTEND ON NEXT 1M MAXSIZE UNLIMITED;
CREATE TABLESPACE aud2ytab DATAFILE '+DATA' SIZE 2M AUTOEXTEND ON NEXT 1M MAXSIZE UNLIMITED;
CREATE TABLESPACE aud2yidx DATAFILE '+DATA' SIZE 2M AUTOEXTEND ON NEXT 1M MAXSIZE UNLIMITED;
CREATE TABLESPACE aud3ytab DATAFILE '+DATA' SIZE 2M AUTOEXTEND ON NEXT 1M MAXSIZE UNLIMITED;
CREATE TABLESPACE aud3yidx DATAFILE '+DATA' SIZE 2M AUTOEXTEND ON NEXT 1M MAXSIZE UNLIMITED;



--move indexes on XCHG tables to PSINDEX
select 'ALTER TABLE '||owner||'.'||table_name||' MOVE TABLESPACE USERS;'
from	dba_tables
where	owner = 'PSARCH'
and	partitioned = 'NO'
and	table_name like 'XCHG%'
and	tablespace_name != 'USERS'
/
--move indexes on XCHG tables to PSINDEX
select 'ALTER INDEX '||owner||'.'||index_name||' REBUILD TABLESPACE PSINDEX;'
from	dba_indexes
where	owner = 'PSARCH'
and	partitioned = 'NO'
and	index_type like '%NORMAL'
and	table_name like 'XCHG%'
and	tablespace_name != 'PSINDEX'
/



select table_owner, table_name, partition_name, tablespace_name, compression, blocks, num_rows
from dba_tab_partitions
where tablespace_name like 'AUD2%'
and tablespace_name < 'AUD2012M05Z'
--and compression = 'DISABLED'
--and table_name = 'PS_AUDIT_SCH_TBL'
order by 1,2,3
/

select *
from dba_segments
where tablespace_name like 'AUD2%'
and tablespace_name < 'AUD2012M05Z'
--and compression = 'DISABLED'
order by 1,2,3
/


select *
from DBA_LOBS
where tablespace_name like 'AUD2%'
and tablespace_name < 'AUD2012M05Z'

select *
from DBA_LOB_PARTITIONS
where tablespace_name like 'AUD2%'
and tablespace_name < 'AUD2012M05Z'

DBA_APPLY_INSTANTIATED_GLOBAL
DBA_LOB_SUBPARTITIONS
DBA_PART_LOBS
DBA_STREAMS_GLOBAL_RULES
DBA_SCHEDULER_GLOBAL_ATTRIBUTE
DBA_LOB_TEMPLATES
DBA_LOB_PARTITIONS
DBA_GLOBAL_CONTEXT