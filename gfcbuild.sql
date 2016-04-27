rem gfcbuild.sql
rem (c) Go-Faster Consultancy Ltd.
clear screen

spool gfcbuild

execute gfc_pspart.truncate_tables(p_all=>TRUE);
@@partdata
execute gfc_pspart.truncate_tables;

--all tables
execute gfc_pspart.main;

--just generate global temporary tables
--execute gfc_pspart.main(p_rectype => 'T');

--just generate named tables
--execute gfc_pspart.main(p_recname => 'GP_PI_GEN_SOVR', p_rectype => 'P');

--pause
--extract script to file
@@gfcbuildspool.sql

set head on feedback on termout on pages 50
--DROP TABLE gfc_ps_tables;
--DROP TABLE gfc_ps_tab_columns;
--DROP TABLE gfc_ora_tab_columns;
--DROP TABLE gfc_ps_indexdefn;
--DROP TABLE gfc_ps_keydefn;
--DROP TABLE gfc_ps_idxddlparm;
--DROP TABLE gfc_ddl_script;
--DROP VIEW gfc_ps_alt_ind_cols;
--DROP VIEW gfc_ps_keydefn_vw;
--DROP TABLE gfc_part_tables;
--DROP TABLE gfc_part_ranges;
--DROP TABLE gfc_part_range_lists;
