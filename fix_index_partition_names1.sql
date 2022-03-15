REM fix_index_partition_names1.sql
set lines 220 pages 99 trimspool on timi on serveroutput on
column recname format a15
column table_name format a18
column indeX_name format a18

column partition_name format a28
column table_partition_name format a28
column index_partition_name format a28
column expected_partition_name format a28

column subpartition_name format a33
column table_subpartition_name format a33
column index_subpartition_name format a33
column expected_subpartition_name format a33

spool fix_index_partition_names1 append
BEGIN psft_ddl_lock.set_ddl_permitted(TRUE); END;
/

set lines 200 serveroutput on
DECLARE
  l_sql CLOB;
  l_sqlerrm CLOB := '';

  e_preexisting_partition_name EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_preexisting_partition_name, -14082);
  e_no_such_partition_name EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_no_such_partition_name, -2149);
BEGIN
  FOR i IN (
WITH tp as (
select /*MATERIALIZE*/ table_name, partition_position, partition_name
from   user_tab_partitions
), ip as (
select /*MATERIALIZE*/ index_name, partition_position, partition_name
from   user_ind_partitions
), X AS (
select /*+MATERIALIZE*/
       pt.recname, pi.table_name, psi.indexid, pi.index_name
--,      i.index_type
from   gfc_part_tables pt
,      psrecdefn r
,      psindexdefn psi
,      user_indexes i
,      user_part_indexes pi
WHERE  i.index_type LIKE '%NORMAL'
and    pi.locality = 'LOCAL'
and    pt.recname = r.recname
and    psi.recname = r.recname
and    pi.index_name = 'PS'||psi.indexid||psi.recname
and    pi.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
and    i.table_name = pi.table_name
and    i.index_name = pi.index_name
), y as (
SELECT /*+LEADING(x tp)*/ x.*
,      tp.partition_name table_partition_name
,      ip.partition_name index_partition_name
--,      SUBSTR(ip.partition_name,1+LENGTH(x.recname),1) x
,      x.recname||x.indexid||SUBSTR(tp.partition_name,2+LENGTH(x.recname)) expected_partition_name
FROM   x
,      tp
,      ip
WHERE  tp.table_name = x.table_name
AND    ip.index_name = x.index_name
AND    tp.partition_position = ip.partition_position
--AND    (NOT ip.partition_name LIKE tp.partition_name
--OR     (   ip.partition_name LIKE tp.partition_name
--       AND SUBSTR(ip.partition_name,1+LENGTH(x.recname),1) != x.indexid))
)
SELECT *
FROM y
WHERE  index_partition_name != expected_partition_name
--AND NOT index_partition_name LIKE table_partition_name
ORDER BY table_name, index_partition_name desc
  ) LOOP
    l_sql := 'alter index '||i.index_name||' rename partition '||i.index_partition_name||' to '||i.expected_partition_name;
    dbms_output.put_line(l_sql);
    BEGIN
      EXECUTE IMMEDIATE l_sql;
    EXCEPTION 
      WHEN e_preexisting_partition_name THEN 
        l_sqlerrm := sqlerrm||' '||i.expected_partition_name;
        dbms_output.put_line(l_sqlerrm);
        IF SUBSTR(i.index_partition_name,-1) != '#' THEN
          l_sql := l_sql || '#';
          dbms_output.put_line(l_sql);
          EXECUTE IMMEDIATE l_sql;
        END IF;
      WHEN e_no_such_partition_name THEN 
        l_sqlerrm := sqlerrm||' '||i.expected_partition_name;
        dbms_output.put_line(l_sqlerrm);
    END;
  END LOOP;
  IF l_sqlerrm IS NOT NULL THEN
    dbms_output.put_line('Error encountered - please run this script again.');
  END IF;
END;
/

REM subpartitions

DROP TABLE gfc_ind_subpartitions PURGE;
CREATE TABLE gfc_ind_subpartitions AS
select index_name, partition_position, partition_name, subpartition_position, subpartition_name from user_ind_subpartitions;
create unique index gfc_ind_subpartitions on gfc_ind_subpartitions(index_name, partition_position, subpartition_position);


DECLARE
  l_sql CLOB;
  l_sqlerrm CLOB := '';

  e_preexisting_subpartition_name EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_preexisting_subpartition_name, -14263);
  e_no_such_subpartition_name EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_no_such_subpartition_name, -14251);
BEGIN
  FOR i IN (
WITH tp as (
select /*+MATERIALIZE*/ table_name, partition_position, partition_name, subpartition_position, subpartition_name
from   user_tab_subpartitions
), ip as (
select /*+MATERIALIZE*/ index_name, partition_position, partition_name, subpartition_position, subpartition_name
from   user_ind_subpartitions
), X AS (
select /*+MATERIALIZE*/ pt.recname, pi.table_name, psi.indexid, pi.index_name
--,      i.index_type
from   gfc_part_tables pt
,      psrecdefn r
,      psindexdefn psi
,      user_indexes i
,      user_part_indexes pi
WHERE  i.index_type != 'LOB'
and    pi.locality = 'LOCAL'
and    pt.recname = r.recname
and    psi.recname = r.recname
and    pi.index_name = 'PS'||psi.indexid||psi.recname
and    pi.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
and    i.table_name = pi.table_name
and    i.index_name = pi.index_name
), y as (
SELECT /*+LEADING(x tp)*/ x.*
,      tp.partition_name table_partition_name
,      ip.partition_name index_partition_name
,      tp.subpartition_name table_subpartition_name
,      ip.subpartition_name index_subpartition_name
--,      SUBSTR(ip.partition_name,1+LENGTH(x.recname),1) x
,      x.recname||x.indexid||SUBSTR(tp.subpartition_name,2+LENGTH(x.recname)) expected_subpartition_name
FROM   x
,      /*gfc_tab_subpartitions*/ tp
,      gfc_ind_subpartitionS ip
WHERE  tp.table_name = x.table_name
AND    ip.index_name = x.index_name
and    ip.partition_name = x.recname||x.indexid||SUBSTR(tp.partition_name,2+LENGTH(x.recname))
AND    tp.partition_position = ip.partition_position
AND    tp.subpartition_position = ip.subpartition_position
--AND    (NOT ip.partition_name LIKE tp.partition_name
--OR     (   ip.partition_name LIKE tp.partition_name
--       AND SUBSTR(ip.partition_name,1+LENGTH(x.recname),1) != x.indexid))
)
SELECT *
FROM   y
WHERE  index_subpartition_name != expected_subpartition_name
--AND NOT index_subpartition_name LIKE table_subpartition_name
ORDER BY table_name, index_partition_name, index_subpartition_name desc
--FETCH FIRST 1000 ROWS ONLY
  ) LOOP
    l_sql := 'alter index '||i.index_name||' rename subpartition '||i.index_subpartition_name||' to '||i.expected_subpartition_name;
    dbms_output.put_line(l_sql);
    BEGIN
      EXECUTE IMMEDIATE l_sql;
    EXCEPTION 
      WHEN e_preexisting_subpartition_name THEN 
        l_sqlerrm := sqlerrm||' '||i.expected_subpartition_name;
        dbms_output.put_line(l_sqlerrm);
        IF SUBSTR(i.index_subpartition_name,-1) != '#' THEN
          l_sql := l_sql || '#';
          dbms_output.put_line(l_sql);
          EXECUTE IMMEDIATE l_sql;
        END IF;
      WHEN e_no_such_subpartition_name THEN 
        l_sqlerrm := sqlerrm||' '||i.expected_subpartition_name;
        dbms_output.put_line(l_sqlerrm);
    END;
  END LOOP;
  IF l_sqlerrm IS NOT NULL THEN
    dbms_output.put_line('Error encountered - please run this script again.');
  END IF;
END;
/

BEGIN psft_ddl_lock.set_ddl_permitted(TRUE); END;
/

DROP TABLE gfc_ind_subpartitions PURGE;

spool off