clear screen
set serveroutput on echo on timi on
spool xchg_privs.sql

-----------------------------------------------------------------------------------------------------------
--The archiving package requires the following explicit privileges
-----------------------------------------------------------------------------------------------------------
GRANT SELECT on sysadm.psrecdefn TO sysadm;
GRANT SELECT on sysadm.psrecfielddb TO sysadm;
GRANT SELECT on sysadm.gfc_part_ranges TO sysadm;

GRANT SELECT on sys.dba_part_tables TO sysadm;
GRANT SELECT on sys.dba_tab_partitions TO sysadm;
GRANT SELECT on sys.dba_tab_subpartitions TO sysadm;
-----------------------------------------------------------------------------------------------------------
