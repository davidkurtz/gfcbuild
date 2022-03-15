REM fix_index_uniqueness.sql
set pages 99 lines 100 serveroutput on
spool fix_index_uniqueness
column index_name format a18
column uniqueflag heading 'PeopleSoft|Index|UNIQUEFLAG'
column orauniqueflag heading 'Oracle|Index|UNIQUEFLAG'
ttitle 'Mismatching Uniqueness'
WITH x AS (
SELECT r.recname, i.indeXid, ui.index_name
,      ui.uniqueness
,      i.uniqueflag
,      DECODE(ui.uniqueness,'UNIQUE',1,0) orauniqueflag
FROM   psrecdefn r
,      psindexdefn i
,      gfC_part_tables p
,      user_indexes ui
WHERE  r.recname = p.recname
and    i.recname = r.recname
and    ui.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
and    ui.index_name = 'PS'||i.indexid||i.recname
)
select *
from   x
where  uniqueflag != orauniqueflag
/

BEGIN
  UPDATE psversion
  SET    version = version+1
  WHERE  objecttypename IN('SYS','RDM');

  UPDATE pslock
  SET    version = version+1
  WHERE  objecttypename IN('SYS','RDM');

  FOR i IN (
WITH x AS (
SELECT r.recname, i.indeXid, ui.index_name
,      ui.uniqueness
,      i.uniqueflag
,      DECODE(ui.uniqueness,'UNIQUE',1,0) orauniqueflag
FROM   psrecdefn r
,      psindexdefn i
,      gfC_part_tables p
,      user_indexes ui
WHERE  r.recname = p.recname
and    i.recname = r.recname
and    ui.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
and    ui.index_name = 'PS'||i.indexid||i.recname
)
select *
from   x
where  uniqueflag != orauniqueflag
  ) LOOP
    dbms_output.put_line('Recname: '||i.recname||', Index ID: '||i.indexid||' - Set UNIQUEFLAG = '||i.uniqueflag||' => '||i.orauniqueflag);
  
    UPDATE psrecdefn
    SET    version = (SELECT version FROM psversion WHERE objecttypename = 'RDM')
    ,      lastupddttm = SYSTIMESTAMP
    ,      lastupdoprid = 'David Kurtz - SQL'
    WHERE  recname = i.recname;

    UPDATE psindexdefn
    SET    uniqueflag = i.orauniqueflag
    WHERE  recname = i.recname
    AND    indexid = i.indexid;
  END LOOP;
END;
/
commit;
spool off
ttitle off
