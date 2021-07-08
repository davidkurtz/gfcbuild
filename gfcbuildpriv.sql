--------------------------------------------------------------------------------------
-- rem (c) Go-Faster Consultancy
--------------------------------------------------------------------------------------
-- script        : gfcbuildpriv.sql
--
-- created by    : David Kurtz
-- creation date : ??.?.????
--
-- description   : SQl to explicitly grant the relevant privileges to SYSADM for the GFC_PSPART package rather than via a role 
--
-- dependencies  : None
--
-- development & maintenance history
--
-- date           author            version   reference           description
-- ------------   ----------------- -------   ------------        --------------------
-- 10.01.2013     David Kurtz       1.01                          Added privileges required by metadata package
-- 25.06.2021                       1.02                          Extract ownerid from ps.psdbowner
--------------------------------------------------------------------------------------
spool gfcbuildpriv
set echo on
@@ownerid
--------------------------------------------------------------------------------------
GRANT CREATE ANY CONTEXT TO &&ownerid;

GRANT SELECT ON ps.psdbowner TO &&ownerid;

GRANT SELECT ON sys.v_$session TO &&ownerid;
GRANT SELECT ON sys.v_$parameter TO &&ownerid;
GRANT SELECT ON sys.v_$version TO &&ownerid;
GRANT SELECT ON sys.dba_tables TO &&ownerid;
GRANT SELECT ON sys.dba_tab_partitions TO &&ownerid;
GRANT SELECT ON sys.dba_ind_partitions TO &&ownerid;

--10.1.2013 added privileges requird by metadata package
GRANT SELECT on sys.dba_tablespaces    TO &&ownerid;
GRANT SELECT on sys.dba_objects        TO &&ownerid;
--------------------------------------------------------------------------------------
spool off

