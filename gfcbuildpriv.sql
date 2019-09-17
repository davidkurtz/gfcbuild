--------------------------------------------------------------------------------------
--
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
--------------------------------------------------------------------------------------

rem (c) Go-Faster Consultancy Ltd.

spool gfcbuildpriv

GRANT CREATE ANY CONTEXT TO sysadm;

GRANT SELECT ON ps.psdbowner TO sysadm;

GRANT SELECT ON sys.v_$session TO sysadm;
GRANT SELECT ON sys.v_$parameter TO sysadm;
GRANT SELECT ON sys.v_$version TO sysadm;
GRANT SELECT ON sys.dba_tables TO sysadm;
GRANT SELECT ON sys.dba_tab_partitions TO sysadm;
GRANT SELECT ON sys.dba_ind_partitions TO sysadm;

--10.1.2013 added privileges requird by metadata package
GRANT SELECT on sys.dba_tablespaces    TO sysadm;
GRANT SELECT on sys.dba_objects        TO sysadm;


spool off

