rem gfcbuildone.incarc.sql
rem (c) Go-Faster Consultancy Ltd.
clear screen

spool gfcbuild
set echo on termout on serveroutput on

--only necessary if meta data has changed

execute gfc_pspart.truncate_tables(p_all=>FALSE);
/**/
execute gfc_pspart.truncate_tables(p_all=>TRUE);
set termout off 
@@partdata
set termout on 
/**/

spool gfcbuildone.incarc.lst
execute gfc_pspart.truncate_tables;
execute gfc_pspart.set_defaults(p_debug_level => 0);
execute gfc_pspart.display_defaults;

begin
 for i in (
  select *
  from gfc_part_Tables
  where criteria is not null
  and src_table_name is not null
 ) LOOP
  gfc_pspart.main(p_recname => i.recname);
 END LOOP;
end;
/

--pause
--extract script to file
@@gfcbuildspool.sql

set head on feedback on termout on pages 50

