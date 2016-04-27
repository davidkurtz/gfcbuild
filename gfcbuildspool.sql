rem gfcbuildspool.sql
rem (c) Go-Faster Consultancy Ltd.
rem 17.9.2008 - moved spool commands from gfcbuild.sql to this script 

column line format a254
set timi off head off feedback off echo off verify off pages 0 lines 1024 trimspool on
set termout off

column SPOOL_FILENAME   new_value SPOOL_FILENAME
select LOWER('gfcbuild_'||MAX(dbname)||'.sql') SPOOL_FILENAME from ps.psdbowner where UPPER(ownerid) = user;
spool &&SPOOL_FILENAME
undefine SPOOL_FILENAME
select * from table(gfc_pspart.spooler(0));
spool off

column SPOOL_FILENAME   new_value SPOOL_FILENAME
select LOWER('gfcindex_'||MAX(dbname)||'.sql') SPOOL_FILENAME from ps.psdbowner where UPPER(ownerid) = user;
spool &&SPOOL_FILENAME
undefine SPOOL_FILENAME
select * from table(gfc_pspart.spooler(1));
spool off

column SPOOL_FILENAME   new_value SPOOL_FILENAME
select LOWER('gfcstats_'||MAX(dbname)||'.sql') SPOOL_FILENAME from ps.psdbowner where UPPER(ownerid) = user;
spool &&SPOOL_FILENAME
undefine SPOOL_FILENAME
select * from table(gfc_pspart.spooler(2));
spool off

column SPOOL_FILENAME   new_value SPOOL_FILENAME
select LOWER('gfcalter_'||MAX(dbname)||'.sql') SPOOL_FILENAME from ps.psdbowner where UPPER(ownerid) = user;
spool &&SPOOL_FILENAME
undefine SPOOL_FILENAME
select * from table(gfc_pspart.spooler(3));
spool off

column SPOOL_FILENAME   new_value SPOOL_FILENAME
select LOWER('gfcarch1_'||MAX(dbname)||'.sql') SPOOL_FILENAME from ps.psdbowner where UPPER(ownerid) = user;
spool &&SPOOL_FILENAME
undefine SPOOL_FILENAME
select * from table(gfc_pspart.spooler(4));
spool off

column SPOOL_FILENAME   new_value SPOOL_FILENAME
select LOWER('gfcarch2_'||MAX(dbname)||'.sql') SPOOL_FILENAME from ps.psdbowner where UPPER(ownerid) = user;
spool &&SPOOL_FILENAME
undefine SPOOL_FILENAME
select * from table(gfc_pspart.spooler(5));
spool off

set head on feedback on termout on pages 50
