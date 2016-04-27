REM gppin-partdata.sql

--------------------------------------------------------------------------------------------------------------
--Partition GP Reporting Tables by PIN
--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_tables
WHERE part_id IN('GPRPTPIN','GPRPTAC')
/

INSERT INTO gfc_part_tables
(recname, organization, part_id, part_column, part_type)
VALUES
('GFC_GPPIN_RPT1', 'I', 'GPRPTPIN','PIN_NUM', 'L')
/

INSERT INTO gfc_part_tables
(recname, organization, part_id, part_column, part_type)
VALUES
('GFC_GPACUM_RPT1', 'I', 'GPRPTAC','PIN_NUM', 'L')
/

UPDATE gfc_part_tables a
SET (criteria,src_table_name) = (
	SELECT 'WHERE 1=1 '||condition_text 
	,	DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
	FROM 	ps_wms_gp_rpt_rec b 
	,	psrecdefn r
	WHERE 	b.tgt_recname = a.recname
	AND	r.recname = b.src_recname)
WHERE part_id IN('GPRPTPIN','GPRPTAC')
and 1=1
/

UPDATE 	gfc_part_tables
SET	tab_tablespace = 'GPRPTTAB'   /*default PSFT tablespace*/
,	idx_tablespace = 'GPRPTIDX' /*default PSFT tablespace*/
,	tab_storage = 'PCTUSED 95 PCTFREE 1'
,	idx_storage = 'PCTFREE 1'
WHERE part_id IN('GPRPTPIN','GPRPTAC')
/

--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategy for PINs
--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_lists
WHERE part_id IN('GPRPTPIN','GPRPTAC')
/
DELETE FROM gfc_part_ranges
WHERE part_id IN('GPRPTPIN','GPRPTAC')
/

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
SELECT	t.part_id
,	row_number() over (partition by part_id order by p.pin_num)
,	LTRIM(TO_CHAR(p.pin_num))
, 	p.pin_num
FROM	gfc_part_tables t
,	ps_wms_gp_rpt_pin a
,	ps_gp_pin p
WHERE	t.part_id IN('GPRPTPIN','GPRPTAC')
AND	a.tgt_recname = t.recname
AND	a.pin_nm = p.pin_nm
/


UPDATE 	gfc_part_lists
SET 	tab_tablespace = 'GP'||part_name||'TAB'
,   	idx_tablespace = 'GP'||part_name||'IDX'
--,	tab_storage = '/*TAB STORAGE*/'
--,	idx_storage = '/*IDX STORAGE*/'
WHERE 	part_id = 'GPPIN32'
AND	part_no < 9999
AND 1=2
/
-----------------------------------------------------------------------------------------------------------
--set tablespaces for GP range partitions
-----------------------------------------------------------------------------------------------------------
UPDATE	gfc_part_tables
SET 	method_opt = 'FOR ALL COLUMNS SIZE 1'
WHERE 	part_id IN('GPRPTPIN','GPRPTAC')
/
-----------------------------------------------------------------------------------------------------------
--mapping between ranges and lists
-----------------------------------------------------------------------------------------------------------
