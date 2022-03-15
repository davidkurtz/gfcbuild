REM part_wrong_tablespace.sql
column recname format a15
column table_name format a18
column index_name format a18
column partition_name format a30
column subpartition_name format a35
column tab_tablespace format a15 heading 'PS Table|Tablespace|Name'
column idx_tablespace format a15 heading 'PS Index|Tablespace|Name'
column tablespace_name format a15 heading 'Tablespace|Name'
column table_tablespace_name format a15 heading 'ORA Table|Tablespace|Name'
column index_tablespace_name format a15 heading 'ORA Index|Tablespace|Name'
break on table_name skip 1 on partition_name on subpartition_name on tab_tablespace on table_tablespace_name
set lines 180 pages 99
ttitle 'Partition Segments in the wrong tablespace, where the correct tablespace exists'
spool part_wrong_tablespace
WITH x as (
SELECT DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
,      r.recname||'_'||COALESCE(rp.part_name,lp.part_name) partition_name
,      COALESCE(lp.tab_tablespace,rp.tab_tablespace,t.tab_tablespace) tab_tablespace
,      COALESCE(lp.idx_tablespace,rp.idx_tablespace,t.idx_tablespace) idx_tablespace
FROM   psrecdefn r
,      gfc_part_tables t
  LEFT OUTER JOIN gfc_part_ranges rp
  ON   t.part_type = 'R'
  AND  rp.part_id = t.part_id
  LEFT OUTER JOIN gfc_part_lists lp
  ON   t.part_type = 'L'
  AND  lp.part_id = t.part_id
WHERE  r.recname = t.recname
)
select x.table_name, x.partition_name, x.tab_tablespace
, sp.tablespace_name table_tablespace_name
, ip.index_name
, x.idx_tablespace
, ip.tablespace_name index_tablespace_name
from   x
  LEFT OUTER JOIN user_tab_partitions sp
  on   x.table_name = sp.table_name
  and  x.partition_name = sp.partition_name
  LEFT OUTER JOIN dba_tablespaces ts
  ON   x.tab_tablespace = ts.tablespace_name
  LEFT OUTER JOIN user_ind_partitions ip
  on   x.partition_name = ip.partition_name
  LEFT OUTER JOIN dba_tablespaces js
  ON   x.idx_tablespace = js.tablespace_name
where  (x.tab_tablespace != sp.tablespace_name
or      x.idx_tablespace != js.tablespace_name)
order by x.table_name, x.partition_name, ip.index_name
/



ttitle 'Sub-Partition Segments in the wrong tablespace, where the correct tablespace exists'
WITH x as (
SELECT DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
,      r.recname||'_'||rp.part_name partition_name
,      r.recname||'_'||rp.part_name||'_'||lp.part_name subpartition_name
,      COALESCE(lp.tab_tablespace,rp.tab_tablespace,t.tab_tablespace) tab_tablespace
,      COALESCE(lp.idx_tablespace,rp.idx_tablespace,t.idx_tablespace) idx_tablespace
FROM   psrecdefn r
,      gfc_part_tables t
,      gfc_part_ranges rp
,      gfc_part_lists lp
,      gfc_part_subparts s
WHERE  r.recname = t.recname
AND    t.part_type = 'R'
and    t.part_id = rp.part_id
and    t.subpart_type = 'L'
and    t.part_id = lp.part_id
and    s.part_id = t.part_id
and    s.part_name = rp.part_name
and    s.subpart_name = lp.part_name
and    s.build = 'Y'
)
select x.table_name, x.partition_name, x.subpartition_name, x.tab_tablespace
, sp.tablespace_name table_tablespace_name
, ip.index_name
, x.idx_tablespace
, ip.tablespace_name index_tablespace_name
from   x
  LEFT OUTER JOIN user_tab_subpartitions sp
  on   x.table_name = sp.table_name
  and  x.partition_name = sp.partition_name
  and  x.subpartition_name = sp.subpartition_name
  LEFT OUTER JOIN dba_tablespaces ts
  ON   x.tab_tablespace = ts.tablespace_name
  LEFT OUTER JOIN user_ind_subpartitions ip
  on   x.partition_name = ip.partition_name
  and  x.subpartition_name = ip.subpartition_name
  LEFT OUTER JOIN dba_tablespaces js
  ON   x.idx_tablespace = js.tablespace_name
where  (x.tab_tablespace != sp.tablespace_name
or      x.idx_tablespace != js.tablespace_name)
order by x.table_name, x.partition_name, x.subpartition_name, ip.index_name
/
spool off
clear breaks
ttitle off
