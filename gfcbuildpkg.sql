rem gfcbuildpkg.sql
rem (c) Go-Faster Consultancy Ltd.

spool gfcbuildpkg

set serveroutput on buffer 1000000000 verify on feedback on lines 120 timing off autotrace off pause off echo off termout on
ALTER SESSION SET recyclebin = off;
ALTER SESSION SET current_schema=SYSADM;
--set echo on

--@@gfcbuildtab.sql--removed 1.11.2012 because it could cause accidental loss of additional privileges on metadata tables

-----------------------------------------------------------------------------------------------------------
--now build the package
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE sysadm.gfc_pspart AS

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
,p_parallel_table  VARCHAR2 DEFAULT ''
,p_parallel_index  VARCHAR2 DEFAULT ''
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
--,p_ddltrigger    VARCHAR2 DEFAULT '*'
,p_ddlenable       VARCHAR2 DEFAULT ''
,p_ddldisable      VARCHAR2 DEFAULT ''
,p_drop_purge      VARCHAR2 DEFAULT ''
--,p_noalterprefix VARCHAR2 DEFAULT '*'
,p_forcebuild      VARCHAR2 DEFAULT ''
,p_desc_index      VARCHAR2 DEFAULT ''
,p_repopdfltsub    VARCHAR2 DEFAULT ''
,p_repopnewmax     VARCHAR2 DEFAULT ''
,p_rename_parts    VARCHAR2 DEFAULT ''
,p_debug_level     INTEGER DEFAULT NULL
);

-----------------------------------------------------------------------------------------------------------
--Truncate Working Storage Tables
-----------------------------------------------------------------------------------------------------------
PROCEDURE truncate_tables
(p_all BOOLEAN DEFAULT FALSE
);

-----------------------------------------------------------------------------------------------------------
--Spool script
-----------------------------------------------------------------------------------------------------------
TYPE outrecset IS TABLE OF VARCHAR2(200);
FUNCTION spooler
(p_type NUMBER DEFAULT 0) 
RETURN outrecset PIPELINED;

-----------------------------------------------------------------------------------------------------------
--Main DDL Generation Procedure   
-----------------------------------------------------------------------------------------------------------
PROCEDURE main
(p_part_id     VARCHAR2 DEFAULT ''  --Build matching list of PART_IDs
,p_recname     VARCHAR2 DEFAULT ''  --name of table(s) to be built - pattern matching possible - default null implies all
,p_rectype     VARCHAR2 DEFAULT 'A' --Build (P)artitioned tables, Global (T)emp tables, or (A)ll tables - default ALL
,p_projectname VARCHAR2 DEFAULT ''  --Build records in named Application Designer Project
);

END gfc_pspart;
/

-----------------------------------------------------------------------------------------------------------
@@gfcbuildpkgbody.sql
--@@gfcbuildpkgbody.plb
-----------------------------------------------------------------------------------------------------------
spool gfcbuildpkg-check
execute gfc_pspart.history;
execute gfc_pspart.set_defaults;
execute gfc_pspart.display_defaults;
spool off


