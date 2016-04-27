@partdata
TRUNCATE TABLE ps_sch_defn_tbl;
INSERT /*+APPEND NOLOGGING PARALLEL(T)*/ INTO ps_sch_defn_tbl t (
setid,sch_adhoc_ind,schedule_id,effdt,eff_status,end_effdt,descr,descrshort,sch_type,
mult_rotations,schedule_days,taskgroup,time_rptg_tmplt,workgroup,sch_dls_opt
) 
WITH x AS (
SELECT rownum n
FROM dual
CONNECT BY LEVEL <= 2000
)
SELECT /*+PARALLEL(S)*/
'ABC' setid
,' ' sch_adhoc_ind
,ROWNUM schedule_id
,TO_DATE('20081110','YYYYMMDD')+n effdt
,'A' eff_status
,TO_DATE('20081110','YYYYMMDD')+n+42 end_effdt
,x.n descr
,x.n descrshort
,' ' sch_type
,' ' mult_rotations
,x.n schedule_days
,' ' taskgroup
,' ' time_rptg_tmplt
,' ' workgroup
,' ' sch_dls_opt
FROM x;


spool dmk 
set wrap on long 5000 lines 500 serveroutput on 
execute sysadm.gfc_archive.main(p_max_parts=>1,p_recname=>'SCH_DEFN_TBL');
execute sysadm.gfc_archive.main(p_max_parts=>10,p_recname=>'SCH_DEFN_TBL');
execute sysadm.gfc_archive.main(p_max_parts=>1000,p_recname=>'SCH_DEFN_TBL');
