REM partvalid.sql
clear 
set head off feedback off echo off verify off pages 9999 termout off pause off
column SPOOL_FILENAME new_value SPOOL_FILENAME
SELECT 'partvalid_'||lower(name)||'_'||TO_CHAR(sysdate,'YYYYmmdd') SPOOL_FILENAME 
FROM v$database;

set pages 99 head on termout on autotrace off pages 40 lines 200 feedback off echo off pause off autotrace off timi on trimspool on

column collist format a50
column part_type format a5
column subpart_type format a5
ttitle 'Table Partition Discrepancies'
break on recname skip 1

spool &&SPOOL_FILENAME

with o as (
select  /*+MATERIALIZE*/ distinct ownerid
from 	ps.psdbowner
), g as (
SELECT  recname,
        rectype, 
        CASE SUBSTR(r.recname,1,5) 
             WHEN 'GPAU_' THEN i.INSTALLED_GP_AUS
             WHEN 'GPAR_' THEN i.INSTALLED_GP_ARG
             WHEN 'GPBR_' THEN i.INSTALLED_GP_BRA
             WHEN 'GPCA_' THEN i.INSTALLED_GP_CAN
             WHEN 'GPCH_' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHA' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHS' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHT' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHU' THEN i.INSTALLED_GP_CHE
             WHEN 'GPDE_' THEN i.INSTALLED_GP_DEU
             WHEN 'GPES_' THEN i.INSTALLED_GP_ESP
             WHEN 'GPFR_' THEN i.INSTALLED_GP_FRA
             WHEN 'GPGB_' THEN i.INSTALLED_GP_UK 
             WHEN 'GPIE_' THEN i.INSTALLED_GP_IRL
             WHEN 'GPIT_' THEN i.INSTALLED_GP_ITA
             WHEN 'GPJP_' THEN i.INSTALLED_GP_JPN
             WHEN 'GPIN_' THEN i.INSTALLED_GP_IND
             WHEN 'GPHK_' THEN i.INSTALLED_GP_HKG
             WHEN 'GPMX_' THEN i.INSTALLED_GP_MEX
             WHEN 'GPMY_' THEN i.INSTALLED_GP_MYS
             WHEN 'GPNL_' THEN i.INSTALLED_GP_NLD
             WHEN 'GPNZ_' THEN i.INSTALLED_GP_NZL
             WHEN 'GPSG_' THEN i.INSTALLED_GP_SGP
             WHEN 'GPCH_' THEN i.INSTALLED_GP_CHN /*added 2.12.2016*/
             WHEN 'GPCN_' THEN i.INSTALLED_GP_CHE /*added 25.1.2017*/
             WHEN 'GPTH_' THEN i.INSTALLED_GP_THA /*added 2.12.2016*/
             WHEN 'GPTW_' THEN i.INSTALLED_GP_TWN
             WHEN 'GPUS_' THEN i.INSTALLED_GP_USA
             ELSE 'Y'
        END AS installed_gp
FROM    psrecdefn r
,       ps_installation i
WHERE   r.rectype IN(0,7) --only SQL tables can be partitioned or rebuilt as GTTs
), x as (
select 	o.ownerid
,	r.recname
,	DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
, 	DECODE(NVL(p.part_type,t.part_type),'H','HASH','R','RANGE','L','LIST','N','NONE') part_type
, 	DECODE(NVL(p.subpart_type,t.subpart_type),'H','HASH','R','RANGE','L','LIST','N','NONE') subpart_type
from 	o
,    	g
,    	gfc_part_tables t
inner join psrecdefn r 
	on r.recname = t.recname
left outer join gfc_part_tables p
	on p.recname = t.recname
where r.rectype = 0
and   t.recname = g.recname
and   installed_gp = 'Y'
--and t.recname like 'GP_GL%'
), y as (
select x.*
, o.created
, NVL(t.partitioning_type,'NONE') partitioning_type
, NVL(t.subpartitioning_type,'NONE') subpartitioning_type
from x
inner join dba_objects o
	on o.owner = x.ownerid
	and o.object_name = x.table_name
	and o.object_type = 'TABLE'
left outer join dba_part_tables t
	on t.owner = x.ownerid
	and t.table_name = x.table_name
) , z as (
select y.* ,
CASE WHEN part_type != 'NONE' AND partitioning_type = 'NONE' THEN 'Should be Partitioned'
     WHEN part_type = 'NONE' AND partitioning_type != 'NONE' THEN 'Should not be Partitioned'
     WHEN part_type != partitioning_type THEN 'Partition Difference'
     WHEN subpart_type != 'NONE' AND subpartitioning_type = 'NONE' THEN 'Should be Subpartitioned'
     WHEN subpart_type = 'NONE' AND subpartitioning_type != 'NONE' THEN 'Should not be Subpartitioned'
     WHEN subpart_type != subpartitioning_type THEN 'Subpartition Type Diff'
     ELSE 'OK' END as comparison
from y
)
select recname, part_Type, subpart_type
, partitioning_type, subpartitioning_type
, comparison, created
from z
where comparison != 'OK'
order by 1,2,3
/


ttitle 'Index Partition Discrepancies'
break on recname skip 1
with o as (
select  /*+MATERIALIZE*/ distinct ownerid
from 	ps.psdbowner
), g as (
SELECT         recname,
        rectype, 
        CASE SUBSTR(r.recname,1,5) 
             WHEN 'GPAU_' THEN i.INSTALLED_GP_AUS
             WHEN 'GPAR_' THEN i.INSTALLED_GP_ARG
             WHEN 'GPBR_' THEN i.INSTALLED_GP_BRA
             WHEN 'GPCA_' THEN i.INSTALLED_GP_CAN
             WHEN 'GPCH_' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHA' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHS' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHT' THEN i.INSTALLED_GP_CHE
             WHEN 'GPCHU' THEN i.INSTALLED_GP_CHE
             WHEN 'GPDE_' THEN i.INSTALLED_GP_DEU
             WHEN 'GPES_' THEN i.INSTALLED_GP_ESP
             WHEN 'GPFR_' THEN i.INSTALLED_GP_FRA
             WHEN 'GPGB_' THEN i.INSTALLED_GP_UK 
             WHEN 'GPIE_' THEN i.INSTALLED_GP_IRL
             WHEN 'GPIT_' THEN i.INSTALLED_GP_ITA
             WHEN 'GPJP_' THEN i.INSTALLED_GP_JPN
             WHEN 'GPIN_' THEN i.INSTALLED_GP_IND
             WHEN 'GPHK_' THEN i.INSTALLED_GP_HKG
             WHEN 'GPMX_' THEN i.INSTALLED_GP_MEX
             WHEN 'GPMY_' THEN i.INSTALLED_GP_MYS
             WHEN 'GPNL_' THEN i.INSTALLED_GP_NLD
             WHEN 'GPNZ_' THEN i.INSTALLED_GP_NZL
             WHEN 'GPSG_' THEN i.INSTALLED_GP_SGP
             WHEN 'GPCH_' THEN i.INSTALLED_GP_CHN /*added 2.12.2016*/
             WHEN 'GPCN_' THEN i.INSTALLED_GP_CHE /*added 25.1.2017*/
             WHEN 'GPTH_' THEN i.INSTALLED_GP_THA /*added 2.12.2016*/
             WHEN 'GPTW_' THEN i.INSTALLED_GP_TWN
             WHEN 'GPUS_' THEN i.INSTALLED_GP_USA
             ELSE 'Y'
        END AS installed_gp
FROM    psrecdefn r
,       ps_installation i
WHERE   r.rectype IN(0,7) --only SQL tables can be partitioned or rebuilt as GTTs
), x as (
select 	o.ownerid
,	r.recname
,	DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
, 	i.indexid
, 	DECODE(NVL(p.part_type,t.part_type),'H','HASH','R','RANGE','L','LIST','N','NONE') part_type
, 	DECODE(NVL(p.subpart_type,t.subpart_type),'H','HASH','R','RANGE','L','LIST','N','NONE') subpart_type
from o
,    g
,    gfc_part_tables t
inner join psrecdefn r 
	on r.recname = t.recname
inner join psindexdefn i
	on i.recname = t.recname
left outer join gfc_part_indexes p
	on p.recname = i.recname
	and p.indexid = i.indexid
where r.rectype = 0
and   t.recname = g.recname
and   installed_gp = 'Y'
--and t.recname like 'GP_GL%'
), y as (
select x.*
, o.created
, i.index_name
, i.locality
, NVL(i.partitioning_type,'NONE') partitioning_type
, NVL(i.subpartitioning_type,'NONE') subpartitioning_type
, ( 
  SELECT LISTAGG(column_name,',') WITHIN GROUP (ORDER BY c.column_position) collist
  FROM dba_ind_columns c
  WHERE c.table_owner = x.ownerid
  AND   c.table_name = x.table_name
  AND   c.index_name = 'PS'||x.indexid||x.recname
  GROUP BY c.table_owner, c.table_name, c.index_owner, c.index_name
  ) collist
from x
inner join dba_objects o
	on o.owner = x.ownerid
	and o.object_name = 'PS'||x.indexid||x.recname
	and o.object_type = 'INDEX'
left outer join dba_part_indexes i
	on i.owner = x.ownerid
	and i.table_name = x.table_name
	and i.index_name = 'PS'||x.indexid||x.recname
), z as (
select y.* ,
CASE WHEN part_type != 'NONE' AND partitioning_type = 'NONE' THEN 'Should be Partitioned'
     WHEN part_type = 'NONE' AND partitioning_type != 'NONE' THEN 'Should not be Partitioned'
     WHEN part_type != partitioning_type THEN 'Partition Difference'
     WHEN subpart_type != 'NONE' AND subpartitioning_type = 'NONE' THEN 'Should be Subpartitioned'
     WHEN subpart_type = 'NONE' AND subpartitioning_type != 'NONE' THEN 'Should not be Subpartitioned'
     WHEN subpart_type != subpartitioning_type THEN 'Subpartition Type Diff'
     ELSE 'OK' END as comparison
from y
)
select recname, indexid, part_Type, subpart_type
, locality, partitioning_type, subpartitioning_type
, comparison, created, collist
from z
where comparison != 'OK'
order by 1,2,3
/
spool off

