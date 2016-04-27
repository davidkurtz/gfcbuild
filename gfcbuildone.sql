rem gfcbuildone.sql
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


spool gfcbuildone
execute gfc_pspart.truncate_tables;
execute gfc_pspart.set_defaults(p_debug_level => 0);
execute gfc_pspart.display_defaults;

begin
  gfc_pspart.main(p_rectype=>'P', p_recname=>'PSPMTRANSARCH');
  gfc_pspart.main(p_rectype=>'P', p_recname=>'PSPMEVENTARCH');
end;
/


--pause
--extract script to file
@@gfcbuildspool.sql

set head on feedback on termout on pages 50

