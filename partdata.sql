REM partdata.sql
rem (c) Go-Faster Consultancy 2001-21
-----------------------------------------------------------------------------------------------------------
WHENEVER SQLERROR CONTINUE
spool partdata0
EXECUTE gfc_pspart.truncate_tables(p_all=>TRUE);
-----------------------------------------------------------------------------------------------------------
--@@gfc_partdata_pkg 
EXECUTE gfc_partdata.partdata;
-----------------------------------------------------------------------------------------------------------
--set gfc_pspart defaults
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
set serveroutput on buffer 1000000000 
execute gfc_pspart.reset_defaults;
execute gfc_pspart.set_defaults(p_read_all=>'SYSADM_READ');
execute gfc_pspart.set_defaults(p_update_all=>'SYSADM_READWRITE');
execute gfc_pspart.set_defaults(p_roles => 'Y');
execute gfc_pspart.set_defaults(p_ddlenable  => 'BEGIN psft_ddl_lock.set_ddl_permitted(TRUE); END;'||CHR(10)||'/');
execute gfc_pspart.set_defaults(p_ddldisable => 'BEGIN psft_ddl_lock.set_ddl_permitted(FALSE); END;'||CHR(10)||'/');
execute gfc_pspart.set_defaults(p_drop_purge => 'Y');
execute gfc_pspart.set_defaults(p_desc_index => 'N');
execute gfc_pspart.set_defaults(p_drop_index => 'N');
execute gfc_pspart.set_defaults(p_parallel_table => 'Y');
execute gfc_pspart.set_defaults(p_parallel_index => 'Y');
execute gfc_pspart.set_defaults(p_force_para_dop => '48'); 
execute gfc_pspart.set_defaults(p_block_sample => 'N');
execute gfc_pspart.set_defaults(p_longtoclob => 'Y');
execute gfc_pspart.display_defaults;
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
set lines 180 pages 50 pause off 
column recname          format a15     heading 'PeopleSoft|Record Name'
column table_name       format a18     heading 'Table Name'
column part_id          format a8      heading 'Part ID'
column part_no          format 9999.90 heading 'Part|No.'
column part_type        format a4      heading 'Part|Type'
column part_name        format a15     heading 'Part|Name'
column part_column      format a30     heading 'Part|Column'
column subpart_type     format a4      heading 'SubP|Type'
column subpart_column   format a14     heading 'Sub-Part|Column'
column hash_partitions  format 999     heading 'Hash|Parts'
column tab_tablespace   format a20     heading 'Table|TblSpc'
column idx_tablespace   format a20     heading 'Index|TblSpc'
column tab_storage      format a35     heading 'Table|Storage Clause'
column idx_storage      format a20     heading 'Index|Storage Clause'
column sample_size      format 999     heading 'Sample|Size %'
column method_opt       format a20     heading 'Optimization|Method'
column override_schema  format a10     heading 'Override|Schema'
column part_value       format a30     heading 'Part|Value'
column list_value       format a85     heading 'List|Value'
column range_name       format a15     heading 'Range|Name'
column stats_type       format a5      heading 'Stats|Opt'
column src_table_name   format a18     heading 'Source Table'
column criteria         format a80     heading 'Criteria'
column arch_schema      format a8      heading 'Archive|Schema'
column arch_recname     format a15     heading 'Archive Record'
column arch_table_name  format a20     heading 'Archive Table'
column arch_flag        format a4      heading 'Arch|Flag'
column noarch_condition format a40     heading 'No Archive Condition'
column name_suffix      format a10     heading 'Name|Suffix'
column partial_index    format a7      heading 'Partial|Index'
set lines 150 pages 50 echo off timi off
spool partdata

ttitle 'Partitioned Tables'
SELECT * FROM gfc_part_tables
ORDER BY 1
/
ttitle 'Range Partitioning'
SELECT * FROM gfc_part_ranges
ORDER BY 1,2,3
/
ttitle 'List Partitioning'
SELECT * FROM gfc_part_lists
ORDER BY 1,2,3
/
ttitle 'List/Range Combinations'
SELECT * FROM gfc_part_range_lists
WHERE build = 'Y'
AND 1=2 --disabled
ORDER BY 1,2,3
/
ttitle 'Non-Locally Partitioned Indexes'
select * from gfc_part_indexes
ORDER BY 1
/
ttitle 'Global Temporary Tables'
SELECT t.*, r.rectype
FROM gfc_temp_tables t
	LEFT OUTER JOIN psrecdefn r
	ON r.recname = t.recname
ORDER BY 1
/
ttitle 'Specified Tablespaces'
SELECT 	' ' partitioning
, 	tab_tablespace, idx_tablespace
FROM	gfc_part_tables
UNION
SELECT 	'R', tab_tablespace, idx_tablespace
FROM	gfc_part_ranges
where arch_flag != 'D'
UNION
SELECT 	'L', tab_tablespace, idx_tablespace
FROM	gfc_part_lists
where arch_flag != 'D'
ORDER BY 1,2,3
/
ttitle 'Tablespaces to be created'
SELECT 	tab_tablespace
FROM	gfc_part_tables
UNION
SELECT 	idx_tablespace
FROM	gfc_part_tables
UNION
SELECT 	tab_tablespace
FROM	gfc_part_ranges
WHERE 	arch_flag != 'D'
UNION
SELECT 	idx_tablespace
FROM	gfc_part_ranges
WHERE 	arch_flag != 'D'
UNION
SELECT 	tab_tablespace
FROM	gfc_part_lists
WHERE 	arch_flag != 'D'
UNION
SELECT 	idx_tablespace
FROM	gfc_part_lists
WHERE 	arch_flag != 'D'
MINUS
SELECT	tablespace_name
from 	dba_tablespaces
minus
select	''
from	dual
/
spool off
ttitle off
set termout on echo off
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

@@psownerid

CREATE OR REPLACE PACKAGE &&ownerid..gfc_long as
	FUNCTION user_tab_partitions 
		(p_table_name 	VARCHAR2
		,p_part_name 	VARCHAR2) 
	RETURN DATE;
END;
/

CREATE OR REPLACE PACKAGE body &&ownerid..gfc_long AS
FUNCTION user_tab_partitions 
	(p_table_name 	VARCHAR2
	,p_part_name 	VARCHAR2) RETURN DATE
IS
	v_sql VARCHAR2(1000);
	v_string VARCHAR2(32767);
        l_retval DATE := NULL;
BEGIN
	v_sql:='SELECT high_value FROM user_tab_partitions WHERE table_name = '''||UPPER(p_table_name)
			||''' AND partition_name = '''||UPPER(p_part_name)||'''';
	EXECUTE IMMEDIATE v_sql INTO v_string;
	IF v_string != 'MAXVALUE' THEN
	        v_sql:='SELECT '||v_string||' FROM DUAL';
		EXECUTE IMMEDIATE v_sql INTO l_retval;
	END IF;
	RETURN l_retval;
END user_tab_partitions;
END;
/
show errors

alter session set nls_date_format = 'YYYYMMDD-HH24MISS';
CREATE OR REPLACE view gfc_date_parts as
SELECT	table_name
, 	partition_name bt_partition_name
,	low_value
,	high_value
FROM	(
	SELECT 	table_name
	, 	partition_name
	, 	partition_position
	,	(lag(high_value,1) over (partition by table_name order by partition_position)) low_value
	, 	high_value-1 high_value
	FROM    (
		SELECT	table_name
		,	partition_name
		,	partition_position
		,	gfc_long.user_tab_partitions(table_name,partition_name) high_value
		FROM user_tab_partitions utp
		) utp
	)
/

