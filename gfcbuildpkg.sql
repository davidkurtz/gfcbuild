rem gfcbuildpkg.sql
rem (c) Go-Faster Consultancy Ltd.

spool gfcbuildpkg

set serveroutput on buffer 1000000000 verify on feedback on lines 120 timing off autotrace off pause off echo off termout on
--set echo on

-----------------------------------------------------------------------------------------------------------
--SYSADM requires the following privileges
-----------------------------------------------------------------------------------------------------------
GRANT SELECT ON sys.v_$parameter TO sysadm;
GRANT SELECT ON sys.v_$version TO sysadm;
GRANT CREATE ANY CONTEXT TO sysadm;
-----------------------------------------------------------------------------------------------------------


@@gfcbuildtab.sql


-----------------------------------------------------------------------------------------------------------
--now build the package
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE gfc_pspart AS

-----------------------------------------------------------------------------------------------------------
--Version and Copyright banner
-----------------------------------------------------------------------------------------------------------
PROCEDURE banner;

-----------------------------------------------------------------------------------------------------------
--Display current defaults
-----------------------------------------------------------------------------------------------------------
PROCEDURE display_defaults;

-----------------------------------------------------------------------------------------------------------
--Version History
-----------------------------------------------------------------------------------------------------------
PROCEDURE history;

-----------------------------------------------------------------------------------------------------------
--Reset defaults to values hard coded in program
-----------------------------------------------------------------------------------------------------------
PROCEDURE reset_defaults;

-----------------------------------------------------------------------------------------------------------
--Set one or some or any parameters to user specified values
-----------------------------------------------------------------------------------------------------------
PROCEDURE set_defaults
(p_chardef         VARCHAR2 DEFAULT ''
,p_logging         VARCHAR2 DEFAULT ''
,p_parallel        VARCHAR2 DEFAULT ''
,p_roles           VARCHAR2 DEFAULT ''
,p_scriptid        VARCHAR2 DEFAULT ''
,p_update_all      VARCHAR2 DEFAULT ''
,p_read_all        VARCHAR2 DEFAULT ''
,p_drop_index      VARCHAR2 DEFAULT ''
,p_pause           VARCHAR2 DEFAULT ''
,p_explicit_schema VARCHAR2 DEFAULT ''
,p_block_sample    VARCHAR2 DEFAULT ''
,p_build_stats     VARCHAR2 DEFAULT ''
,p_deletetempstats VARCHAR2 DEFAULT ''
,p_longtoclob      VARCHAR2 DEFAULT ''
,p_ddltrigger      VARCHAR2 DEFAULT '*'
,p_drop_purge      VARCHAR2 DEFAULT ''
--,p_noalterprefix   VARCHAR2 DEFAULT '*'
,p_forcebuild      VARCHAR2 DEFAULT ''
,p_desc_index      VARCHAR2 DEFAULT ''
);

-----------------------------------------------------------------------------------------------------------
--Truncate Working Storage Tables
-----------------------------------------------------------------------------------------------------------
PROCEDURE truncate_tables
(p_all BOOLEAN DEFAULT FALSE
);

-----------------------------------------------------------------------------------------------------------
--Main DDL Generation Procedure
-----------------------------------------------------------------------------------------------------------
PROCEDURE main
(p_recname     VARCHAR2 DEFAULT ''  --name of table(s) to be built - pattern matching possible - default null implies all
,p_rectype     VARCHAR2 DEFAULT 'A' --Build (P)artitioned tables, Global (T)emp tables, or (A)ll tables - default ALL
,p_projectname VARCHAR2 DEFAULT ''  --Build records in named Application Designer Project
);

END gfc_pspart;
/



-----------------------------------------------------------------------------------------------------------
@@gfcbuildpkgbody.plb
-----------------------------------------------------------------------------------------------------------
spool off

execute gfc_pspart.history;
execute gfc_pspart.set_defaults;
execute gfc_pspart.display_defaults;
