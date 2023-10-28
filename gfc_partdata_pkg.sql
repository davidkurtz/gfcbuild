--------------------------------------------------------------------------------------
--
-- script        : gfc_partdata_pkg.sql
--
-- created by    : DMK
-- creation date : 25.07.2012
--
-- description   : Reload partitioning metadata
--                 package PL/SQL procedure (gfc_partdata) supercedes simple *-partdata.sql Scripts
--
-- dependencies:  package procedure psftapi
--
-- development & maintenance history
--
-- date       author            version reference description
-- ---------- ----------------- ------- --------- --------------------
--------------------------------------------------------------------------------------
set echo on termout on
spool gfc_partdata_pkg
--------------------------------------------------------------------------------------------------------------
--gp_partdata procedure uses this view exists to determine which records (that correspond to tables) to process.  
--For country specific GP records (matched by the beginning of the record name) It returns the GP installation 
--flag for the country
----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gfc_installed_gp AS
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
             WHEN 'GPTW_' THEN i.INSTALLED_GP_TWN
             WHEN 'GPUS_' THEN i.INSTALLED_GP_USA
             ELSE 'Y'
        END AS installed_gp
FROM    psrecdefn r
,       ps_installation i
WHERE   r.rectype IN(0,7) --only SQL tables can be partitioned or rebuilt as GTTs
/

set timi on serveroutput on echo on termout off
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--header of package that populates GFC metadata tables
--------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE gfc_partdata AS
--------------------------------------------------------------------------------------------------------------
--procedure to populate audit data
--------------------------------------------------------------------------------------------------------------
PROCEDURE partdata;        --head procedure that calls others
PROCEDURE gp_partdata;     --GP metadata
PROCEDURE index_comp;  --set index compression in PeopleTools tables
PROCEDURE comp_attrib; --procedure to apply compression attributes to existing tables
--------------------------------------------------------------------------------------------------------------
END gfc_partdata;
/

--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--body of package that populates GFC metdata tables
--------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY gfc_partdata AS
--------------------------------------------------------------------------------------------------------------
--Constants used in the meta data that may need to be changed over time as partitions are added/archived/purged
--Currently, before the end of each UK tax year, the partitions for the next year are built.  This requires that
--------------------------------------------------------------------------------------------------------------
--This is the Latest UK Tax Year for which partitions have been built. 2014=Tax Year 2014/15
k_gp_last_year      CONSTANT INTEGER := 2015;     --build partitions to tax year 2014-15

--This is the Latest UK Tax Year for which Archive partitions have been built. 2011=Tax Year 2011/12
k_gp_last_arch_year CONSTANT INTEGER := 2012;

--First UK Tax Year of for GP
k_gp_first_year   CONSTANT INTEGER := 2009;
--------------------------------------------------------------------------------------------------------------
--Other constants that should not be changed
--------------------------------------------------------------------------------------------------------------
k_datetime_format CONSTANT VARCHAR2(25) := 'hh24:mi:ss dd.mm.yyyy'; --date format picture for message stamps
k_module          CONSTANT VARCHAR2(48) := $$PLSQL_UNIT; --name of package for instrumentation

--------------------------------------------------------------------------------------------------------------
--prints message with leading timestamp
--28.08.2012 DMK changed to call psftapi.message_log
--------------------------------------------------------------------------------------------------------------
PROCEDURE msg(p_msg VARCHAR2) IS
BEGIN
  psftapi.message_log(p_message=>p_msg,p_verbose=>TRUE);
  --dbms_output.put_line(TO_CHAR(SYSDATE,k_datetime_format)||':'||p_msg);
END msg;

--------------------------------------------------------------------------------------------------------------
--execute SQL
--------------------------------------------------------------------------------------------------------------
PROCEDURE exec_sql(p_sql VARCHAR2) IS
BEGIN
  msg('SQL:'||p_sql);
  EXECUTE IMMEDIATE p_sql;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows processed.');
END exec_sql;




--------------------------------------------------------------------------------------------------------------
--Procedure to delete existing metadata by PART_ID
--------------------------------------------------------------------------------------------------------------
PROCEDURE del_partdata 
(p_part_id VARCHAR2)
IS
  k_action CONSTANT VARCHAR2(64) := 'DEL_PARTDATA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  msg('Delete pre-existing list of '||p_part_id||' global indexes');
  DELETE FROM gfc_part_indexes 
  WHERE part_id LIKE p_part_id;
  l_num_rows := SQL%ROWCOUNT;
  DELETE FROM gfc_part_indexes WHERE recname IN (SELECT recname FROM gfc_part_tables WHERE part_id LIKE p_part_id);
  l_num_rows := l_num_rows + SQL%ROWCOUNT;
  DELETE FROM gfc_part_indexes WHERE NOT recname IN (SELECT recname FROM gfc_part_tables);
  l_num_rows := l_num_rows + SQL%ROWCOUNT;
  msg(TO_CHAR(l_num_rows)||' rows deleted.');

  msg('Delete pre-existing list of partitioned '||p_part_id||' tables');
  DELETE FROM gfc_part_tables 
  WHERE part_id LIKE p_part_id;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

  msg('Delete pre-existing '||p_part_id||' range partitioning metadata');
  DELETE FROM gfc_part_ranges
  WHERE part_id LIKE p_part_id;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

  msg('Delete pre-existing '||p_part_id||' list partitioning metadata');
  DELETE FROM gfc_part_lists
  WHERE part_id LIKE p_part_id;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

  msg('Delete pre-existing '||p_part_id||' range-v-list partition mapping metadata');
  DELETE FROM gfc_part_subparts
  WHERE part_id LIKE p_part_id;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');

  commit;

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END del_partdata;
--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE GP METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE gp_partdata IS
  k_action CONSTANT VARCHAR2(64) := 'GP_PARTDATA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('GP');
  msg('Delete pre-existing metadata for global non-partitioned function-based index PSZTL_PAYABLE_TIME');
  DELETE FROM gfc_ps_indexdefn
  WHERE recname = 'TL_PAYABLE_TIME';
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
  DELETE FROM gfc_ps_keydefn
  WHERE recname = 'TL_PAYABLE_TIME';
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted.');
  COMMIT;

--------------------------------------------------------------------------------------------------------------
--insert data to describe temporary tables
--country specific tables for installed country extentions only will be added
--------------------------------------------------------------------------------------------------------------
msg('Insert list of GP temporary tables');
INSERT INTO gfc_temp_tables (recname)                
SELECT  r.recname
FROM    gfc_installed_gp r
WHERE   r.installed_gp != 'N'
AND     r.rectype IN(0,7) --only normal tables can be partitioned
AND     (r.recname IN( /*payroll calculation work tables*/
                'GP_CAL_TS_WRK',
                'GP_CANC_WRK',
                'GP_CANCEL_WRK',   /*new in 8.4*/
                'GP_DB2_SEG_WRK',  /*new in 8.4*/
                'GP_DEL_WRK',
                'GP_DEL2_WRK',     /*new in 8.4*/
                'GP_EXCL_WRK',
                'GP_FREEZE_WRK',   /*new in 8.4*/
                'GP_HST_WRK',
                'GP_JOB2_WRK',     /*13.2.2008 - added at Kelly*/
                'GP_MSG1_WRK'    ,'GP_MSG3_WRK',
                'GP_NEW_RTO_WRK' ,
                'GP_OLD_RTO_WRK' ,
                'GP_PI_HDR_WRK',
                'GP_PYE_HIST_WRK', /*13.2.2008 - added at Kelly*/
                'GP_PYE_HIS2_WRK', /*13.2.2008 - added at Kelly*/
                'GP_PYE_ITER_WRK', /*new in 8.4*/
                'GP_PYE_ITR2_WRK', /*new in 8.4*/
                'GP_PYE_RCLC_WRK', /*new in hcm9.0 - added 21.5.2008*/
                'GP_PYE_STAT_WRK',
                'GP_PYE_STA2_WRK', /*13.2.2008 - added at Kelly*/
                'GP_RTO_CRD_WRK', /*20.2.2008 added at kelly*/
                'GP_RTO_MCU_WRK', /*3.10.2010 added at Morrisons*/
                'GP_RTO_PRC_WRK' ,
                'GP_RTO_TRG_WRK1', /*new in 8.4*/
                'GP_RTO_TRGR_WRK',
                'GP_SEG_WRK',
                'GP_TLPTM_WRK',
                'GP_TLSNT_WRK',
                'GP_TLTRC_WRK',
                'GP_TL_PIGEN_WRK',
                'GP_TL_PIHDR_WRK',
                'GP_TL_PIHD2_WRK', --added 11.6.2010
		'GP_TL_PRSLT_WRK', --added 11.6.2010
                'GP_TL_TRG_WRK',
                /*pin packager*/
                'GP_PKG_ELEM_WRK',
                /*GL*/
                'GP_ACC_LINE_STG', --added 5.3.2009
                'GP_GL_AMT1_TMP' ,'GP_GL_AMT2_TMP',
                'GP_GL_DATA_TMP' ,
                'GP_GL_DNF_TMP'  ,
                'GP_GL_MAPI_TMP' ,
                'GP_GL_OLD_TMP'  ,
                'GP_GL_SEG_TMP'  ,
                'GP_GL_SEGV_TMP' ,
                'GP_GL_STO6_TMP' ,
                'GP_GL_S7N8_TMP' ,
                /*Banking*/
                'GP_NET_PAY1_TMP','GP_NET_PAY2_TMP','GP_NET_PAY3_TMP',
                'GP_NET_DST1_TMP','GP_NET_DST2_TMP',
                'GP_PAYMENT_TMP' ,'GP_PAYMENT2_TMP',
                'GP_REV_DLTA_TMP', /*added 9.6.2004*/
                'GP_SRC_BNK1_TMP','GP_SRC_BNK2_TMP',
                /*Swiss Banking*/
                'GPCH_BK_TMP1','GPCH_BK_TMP2',
                'GPCH_BK_PMTTYPE',
                /*reporting*/
                'GPCH_BL_PRINT_T',
                'GPCH_BL_PRT',
                'GPCH_RP_AL1',
                'GPCH_RP_AL01',
                'GPCH_RP_AL03'  , 'GPCH_RP_AL03_1',
                'GPCH_RP_AL07_1', 'GPCH_RP_AL07_2', 'GPCH_RP_AL07_3',
                'GPCH_RP_AL08',
                'GPCH_RP_AL81'  , 'GPCH_RP_AL82'   , 'GPCH_RP_AL83',
                'GPCH_RP_TX01A' ,
                'GPCH_RP_TX06'  , 'GPCH_RP_TX06_01',
                'GPCH_RP_TX61'  , 'GPCH_RP_TX62'   , 'GPCH_RP_TX63',
                'GPCH_RP_FK1A','GPCH_RP_FK2A',
                'GPCH_RP_0001_01',
                'GPCH_SRC_BNK',
                'GPCHAL021_TMP','GPCHAL022_TMP','GPCHAL023_TMP',
                'GPCHAL024_TMP',
                'GPCHAL031_TMP',
                'GPCHAL051_TMP','GPCHAL052_TMP',
                'GPCHAL071_TMP','GPCHAL072_TMP','GPCHAL073_TMP','GPCHAL074_TMP','GPCHAL075_TMP',
                'GPCHAL101_TMP','GPCHAL102_TMP',
                'GPCHSI061_TMP',
                'GPCHST021_TMP','GPCHST022_TMP','GPCHST023_TMP',
                'GPCHTX011_TMP','GPCHTX012_TMP',
                'GPCHTX021_TMP',
                'GPCHTX061_TMP','GPCHTX062_TMP','GPCHTX063_TMP','GPCHTX064_TMP',
                'GPGB_PSLIP_ED_D','GPGB_PSLIP_BL_D', /*gpgb_pslip can now be run stream 4.2.2004*/
                'GPGB_PSLIP_ED_W','GPGB_PSLIP_BL_W'
                )
OR r.recname IN(
                SELECT t.recname
                FROM   psaeappltemptbl t   
                ,      psaeappldefn a   
                WHERE  a.ae_applid = t.ae_applid   
                AND    a.ae_disable_restart = 'Y' --restart is disabled   
                AND    a.ae_applid IN('GP_PMT_PREP','GP_GL_PREP','GPGB_PSLIP','GPGB_PSLIP_X','GPGB_EDI') /*limited to just GP AE processes*/
                ))
MINUS
SELECT recname 
FROM gfc_temp_tables
;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--set tablespace options on global temporary objects
--------------------------------------------------------------------------------------------------------------
msg('Specify tablespaces for global temporary tables');
UPDATE gfc_temp_tables
SET    tab_tablespace = 'PSGTT01'   /*default PSFT tablespace*/
;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');
--------------------------------------------------------------------------------------------------------------
--insert data to specify the tables to be partitioned
--country specific tables for installed country extentions only will be added
--------------------------------------------------------------------------------------------------------------
msg('Insert record metadata for GP tables range partitioned on EMPLID');
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'GP'
,        'EMPLID', 'R'
FROM    gfc_installed_gp r
WHERE   r.installed_gp != 'N'
AND     r.rectype = 0 /*only normal tables can be partitioned*/
AND     (   r.recname IN(
                        'GP_AUDIT_TBL',           /*added 21.5.2008*/
                        'GP_ABS_EVENT',           /*absence - added 3.10.2003*/
                        'GP_GL_DATA',             /*gl transfer table*/
--                      'GP_GRP_LIST_RUN',        /*new in 8.4*/
                        'GP_ITER_TRGR',
                        'GP_MESSAGES',
                        'GP_PAYMENT',             
                        'GP_PI_GEN_HDR',          /*13.2.2008-added for kelly*/
                        'GP_PI_GEN_DATA',         /*postitive input*/
                        'GP_PI_GEN_REF',          /*13.2.2008-added for kelly-TL postitive input*/
                        'GP_PI_GEN_SOVR',         /*14.11.2008-added for abbey, 6.7.2012 partitioned at Morrisons*/
                        'GP_PI_MNL_DATA',         /*postitive input*/
                        'GP_PI_MNL_SOVR',         /*postitive input*/
                        'GP_PYE_ITER_LST',        /*13.2.2008-added for kelly*/
                        'GP_PYE_OVRD',
                        'GP_PYE_PRC_STAT',
                        'GP_PYE_SEG_STAT',
                        'GP_RCP_PYE_DTL',         /*added 7.6.2004 for gp_pmt_prep*/
                        'GP_RSLT_ABS',            /*payroll calculation results*/
                        'GP_RSLT_ACUM',           /*payroll calculation results*/
                        'GP_RSLT_DELTA',          /*payroll calculation results*/
                        'GP_RSLT_ERN_DED',        /*payroll calculation results*/
                        'GP_RSLT_PI_DATA',        /*payroll calculation results*/
                        'GP_RSLT_PI_SOVR',        /*payroll calculation results*/
                        'GP_RSLT_PIN',            /*payroll calculation results*/
                        'GP_RTO_TRG_CTRY',        /*8.1 + 8.3 only*/
                        'GP_RTO_TRGR',            /*deadlock problem*/
                        'GPCH_BK_XFER_EE',        /*bank transfer*/
                        'GPCH_TX_DATA',           /*tax data table - added 19.6.2003 - to improve scan and reduce latch contention*/
                        'GPGB_ABS_EVT_JR',
                        'JOB',                    /*hr data */
--                      'COMPENSATION',           /*hr compensation data added 18.1.2010*/
                        'X_PYE_OVRD_ET',          /*customer table*/
                        'GPGB_PAYMENT',           /*added 12.3.2004*/
                        'GPGB_PSLIP_P_ED',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_PSLIP_P_BL',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_PSLIP_P_HR',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_PSLIP_P_FT',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_EDI_RSLT',          /*13.1.2010-result table for GPGB_EDI process*/
                        'TL_PAYABLE_TIME'        /*13.2.2008-added for kelly-TL*/
                        ) 
        OR r.recname IN(SELECT  recname
                        FROM    ps_gp_wa_array
                        )
        );
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


--------------------------------------------------------------------------------------------------------------
--specify list subpartitioned tables
--------------------------------------------------------------------------------------------------------------
msg('Specify GP tables to be list subpartitioned on CAL_RUN_ID');
UPDATE gfc_part_tables
SET    subpart_type = 'L' --20.10.2014 try list sub-range partitioning
,      subpart_column = 'CAL_RUN_ID' --20.10.2014 try list sub-range partitioning
--
,      part_type = 'R' --20.10.2014 try list sub-range partitioning
,      part_column = 'EMPLID' --20.10.2014 try list sub-range partitioning
--
,      hash_partitions = 0
WHERE  recname IN('GP_RSLT_ACUM', 'GP_RSLT_PIN'
--,'GP_GL_DATA' --not worth subpartitioning at Kelly
--,'GP_PYE_SEG_STAT' -- subpartitioning does not work well with retro queries at kelly
--,'GP_PYE_PRC_STAT' -- subpartitioning does not work well with retro queries at kelly
--,'GP_RSLT_PI_SOVR', 'GP_RSLT_PI_DATA' --14.2.2008 removed at kelly
);
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');


--------------------------------------------------------------------------------------------------------------
--set storage options on partitioned objects
--------------------------------------------------------------------------------------------------------------
msg('Specify tablespaces for partitioned GP tables');
UPDATE gfc_part_tables
SET    tab_tablespace = 'GPAPP'   /*default PSFT tablespace*/
,      idx_tablespace = 'PSINDEX' /*default PSFT tablespace*/
,      tab_storage = 'PCTUSED 95 PCTFREE 1'
,      idx_storage = 
	CASE recname 
             WHEN 'GP_RSLT_ACUM'    THEN 'PCTFREE 0 COMPRESS 7' --dmk 15.10.2014 specify compression for three result tables
             WHEN 'GP_RSLT_PIN'     THEN 'PCTFREE 0 COMPRESS 8'
             WHEN 'GP_RSLT_ERN_DED' THEN 'PCTFREE 0 COMPRESS 7'
             WHEN 'GP_RSLT_ABS'     THEN 'PCTFREE 0'
             WHEN 'GP_RSLT_DELTA'   THEN 'PCTFREE 0'
             WHEN 'GP_RSLT_PI_DATA' THEN 'PCTFREE 0'
             WHEN 'GP_RSLT_PI_SOVR' THEN 'PCTFREE 0'
             ELSE 'PCTFREE 1'
        END 
WHERE  part_id = 'GP';
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

msg('Specify index storage option for GP_PYE_SEG_STAT and GP_PYE_PRC_STAT');
UPDATE 	gfc_part_tables
SET	idx_storage = 'PCTFREE **PCTFREE**'
WHERE 	recname IN('GP_PYE_PRC_STAT','GP_PYE_SEG_STAT','TL_PAYABLE_TIME');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

msg('Specify storage option for GP_PYE_SEG_STAT');
UPDATE gfc_part_tables
SET    tab_storage = 'PCTUSED 80 PCTFREE 15'
WHERE  recname IN('GP_PYE_SEG_STAT');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');


--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
msg('Insert metadata for global non-partitioned index on partitioned GP tables.');
INSERT INTO gfc_part_indexes
(recname, indexid, part_id, part_type, part_column)
SELECT	t.recname
,	i.indexid
,	' ' part_id
,	'N' part_type
,	' ' part_column
FROM	gfc_part_tables t
,	psindexdefn i
WHERE	t.recname = i.recname
AND	t.part_type IN('L','R','H')
AND NOT EXISTS(
	SELECT 'x'
	FROM pskeydefn k
	WHERE k.recname = i.recname
	AND   k.indexid = i.indexid
	AND   k.fieldname = t.part_column
	AND   k.keyposn <= 2
	);
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
INSERT INTO gfc_part_indexes
(recname, indexid, part_id, part_type, part_column, subpart_type, subpart_column, hash_partitions)
SELECT	t.recname
,	i.indexid
,	t.part_id
,	t.part_type
,	t.part_column
,	'N' subpart_type
,	'' subpart_column
,	t.hash_partitions
FROM	gfc_part_tables t
,	psindexdefn i
WHERE	t.recname = i.recname
AND	t.subpart_type IN( 'L','R')
AND NOT EXISTS(
	SELECT 'x'
	FROM pskeydefn k, psrecfielddb f
	WHERE f.recname = i.recname
	AND   k.recname = f.recname_parent
	AND   k.indexid = i.indexid
	AND   k.fieldname = t.subpart_column
	AND   k.keyposn <= 2
	)
AND NOT EXISTS(
	SELECT 'x'
	FROM gfc_part_indexes g
	WHERE g.recname = t.recname
	AND   g.indexid = i.indexid);
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');




--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategy
--------------------------------------------------------------------------------------------------------------
msg('Create GP range partitioning metadata from GP stream definition');
INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value)
SELECT 'GP', strm_num
,      LTRIM(TO_CHAR(strm_num,'000')) part_name
,      NVL(LEAD(''''||emplid_from||'''',1) OVER (ORDER BY strm_Num),'MAXVALUE') part_value
FROM   ps_gp_strm;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


-----------------------------------------------------------------------------------------------------------
--set tablespaces for GP range partitions
-----------------------------------------------------------------------------------------------------------
msg('Specify tablespaces for GP list subpartitions');
UPDATE  gfc_part_ranges
SET     tab_tablespace = 'GPSTRM'||LTRIM(TO_CHAR(MOD(part_no-1,32)+1,'00'))||'TAB'
,       idx_tablespace = 'GPSTRM'||LTRIM(TO_CHAR(MOD(part_no-1,32)+1,'00'))||'IDX'
WHERE part_id IN('GP');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

-----------------------------------------------------------------------------------------------------------
--insert data to list partitions
-----------------------------------------------------------------------------------------------------------
msg('Insert list partition definition');

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value, tab_tablespace, idx_tablespace, tab_storage, idx_storage)
SELECT	'GP'
,	year+period/100
, 	LTRIM(TO_CHAR(y.year,'0000'))
	||CASE WHEN y.year <= 2010 THEN 'L' ELSE 'W' END
	||LTRIM(TO_CHAR(p.period,'00'))
,	''''||LTRIM(TO_CHAR(y.year,'0000'))||'W'||LTRIM(TO_CHAR(p.period,'00'))||'REG'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'W'||LTRIM(TO_CHAR(p.period,'00'))||'SUP'''
,	'GP'||TO_CHAR(TRUNC(TO_DATE('0604'||LTRIM(TO_CHAR(y.year,'0000')),'DDMMYYYY'),'IW')-5+(7*p.period),'YYYYMM')||'TAB'
,	'GP'||TO_CHAR(TRUNC(TO_DATE('0604'||LTRIM(TO_CHAR(y.year,'0000')),'DDMMYYYY'),'IW')-5+(7*p.period),'YYYYMM')||'IDX'
,	CASE WHEN TRUNC(TO_DATE('0604'||LTRIM(TO_CHAR(y.year,'0000')),'DDMMYYYY'),'IW')-5+(7*p.period) 
                  < ADD_MONTHS(SYSDATE,-1) THEN 'COMPRESS' 
        END tab_storage
,	CASE WHEN TRUNC(TO_DATE('0604'||LTRIM(TO_CHAR(y.year,'0000')),'DDMMYYYY'),'IW')-5+(7*p.period) 
                  < ADD_MONTHS(SYSDATE,-1) THEN 'PCTFREE 0' 
        END idx_storage
FROM 	(
	SELECT 	k_gp_first_year-1+rownum as year
	FROM   	dual
	CONNECT BY level <= k_gp_last_year-k_gp_first_year+1 --years of list partitions - 2009 thru 2012
     	) y
,    	(
	SELECT  rownum as period
	FROM 	dual
	CONNECT BY level <= 53
     	) p
WHERE	period <= DECODE(y.year,2011,53,2023,53,52)
AND 	(y.year > 2009 or p.period >= 30)
ORDER BY 1,2,3
;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
VALUES ('GP',9999,'Z_OTHERS','DEFAULT');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

----------------------------------------------------------------------------------------------------
--tablespaces for list partitions
-----------------------------------------------------------------------------------------------------------
--msg('Remove invalid table tablespace names from list partition definitions');
--UPDATE gfc_part_lists l
--SET	 l.tab_tablespace = ''
--WHERE  l.tab_tablespace IS NOT NULL
--AND NOT EXISTS(
--	SELECT 'x'
--	FROM 	dba_tablespaces t
--	WHERE	t.tablespace_name = l.tab_tablespace
--	);
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

--msg('Remove invalid index tablespace names from list partition definitions');
--UPDATE gfc_part_lists l
--SET	 l.idx_tablespace = ''
--WHERE  l.idx_tablespace IS NOT NULL
--AND NOT EXISTS(
--	SELECT 'x'
--	FROM 	dba_tablespaces t
--	WHERE	t.tablespace_name = l.idx_tablespace
--	);
--msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

-----------------------------------------------------------------------------------------------------------
--statistics options for list subpartitioned tables
-----------------------------------------------------------------------------------------------------------
msg('Specify histogram collection policy for GP list subpartitioned tables');
UPDATE  gfc_part_tables
SET     method_opt = 'FOR ALL COLUMNS SIZE 1, FOR COLUMNS SIZE 254 CAL_RUN_ID, CAL_ID, ORIG_CAL_RUN_ID'
WHERE   part_id IN('GP')
AND     subpart_type = 'L';
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

-----------------------------------------------------------------------------------------------------------
--mapping between ranges and lists
-----------------------------------------------------------------------------------------------------------
msg('Insert GP range-v-list partition mapping metadata');
INSERT INTO gfc_part_range_Lists
(part_id, range_name, list_name)
SELECT r.part_id, r.part_name, l.part_name
FROM   gfc_part_ranges r
,      gfc_part_lists l
WHERE  l.part_id = r.part_id
AND    l.part_id IN('GP');
msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

commit;
dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END gp_partdata;
--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE WL METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE wl_partdata IS
  k_action CONSTANT VARCHAR2(48) := 'WL_PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('WL'); --delete part ID GL

  msg('Populating list of WL Partitioned Tables');
  INSERT INTO gfc_part_tables
  (recname, part_id, part_type, part_column, subpart_type, subpart_column
--, tab_tablespace, idx_tablespace, tab_storage, idx_storage
--, method_opt
  )
  VALUES('PSWORKLIST', 'WL', 'R', 'INSTSTATUS', 'N', '' 
--,'GLLARGE', 'PSINDEX', 'PCTUSED 90 PCTFREE **PCTFREE**', 'PCTFREE **PCTFREE**'
--, 'FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 254 LEDGER FISCAL_YEAR ACCOUNTING_PERIOD BOOK_CODE CURRENCY_CD BUSINESS_UNIT ACCOUNT PROJECT_ID'
  );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
  msg('Insert metadata for global non-partitioned index on partitioned WL tables.');
  INSERT INTO gfc_part_indexes
  (recname, indexid, part_id, part_type, part_column, subpart_type, subpart_column, hash_partitions)
  SELECT   t.recname
  ,        i.indexid
  ,        t.part_id
  ,        'N' part_type
  ,        t.part_column
  ,        'N' subpart_type
  ,        '' subpart_column
  ,        t.hash_partitions
  FROM     gfc_part_tables t
  ,        psindexdefn i
  WHERE    t.recname = i.recname
  AND      t.part_type != 'N'
  AND      t.part_id IN('WL')
  AND NOT EXISTS(
        SELECT 'x'
        FROM pskeydefn k, psrecfielddb f
        WHERE f.recname = i.recname
        AND   k.recname = f.recname_parent
        AND   k.indexid = i.indexid
        AND   k.fieldname IN(t.part_column,t.subpart_column)
        AND   k.keyposn <= 3 --partitioning key in first three columns of index
        );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

  BEGIN
    INSERT INTO gfc_part_indexes
    (recname, indexid, part_id, part_type, part_column, subpart_type, partial_index)
    VALUES
    ('PSWORKLIST', 'B', 'WL', 'L', 'INSTSTATUS', 'N', 'Y');
  EXCEPTION WHEN dup_val_on_index THEN
    UPDATE gfc_part_indexes
	SET    part_type = 'R', partial_index = 'Y'
	WHERE  recname = 'PSWORKLIST'
	AND    indexid = 'B';
  END;

  l_num_rows := 0;
  msg('Populating WL Range Partitions');
  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value, partial_index) VALUES('WL',2,'SELECT_OPEN' ,'''2''', 'Y');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;
  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value, tab_storage, idx_storage, partial_index) 
  VALUES('WL',9,'WORKED_CANC','MAXVALUE','PCTFREE 1 PCTUSED 90','PCTFREE 1','N');
  l_num_rows := l_num_rows +SQL%ROWCOUNT;

  msg(TO_CHAR(l_num_rows)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--mapping between ranges and lists
-----------------------------------------------------------------------------------------------------------
  msg('Insert WL range-v-list partition mapping metadata');
  INSERT INTO gfc_part_subparts
  (part_id, part_name, subpart_name)
  SELECT r.part_id, r.part_name, l.part_name
  FROM   gfc_part_ranges r
  ,      gfc_part_lists l
  WHERE  l.part_id = r.part_id
  AND    l.part_id IN('WL');

  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
-----------------------------------------------------------------------------------------------------------
--End WL Metadata
-----------------------------------------------------------------------------------------------------------
  commit;
  msg('WL metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END wl_partdata;
--------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------------------
--PROCEDURE TO POPULATE PPM METADATA
--------------------------------------------------------------------------------------------------------------
PROCEDURE ppm_partdata IS
  k_action CONSTANT VARCHAR2(48) := 'PPM_PARTDATA';
  l_module VARCHAR2(48);
  l_action VARCHAR2(32);
  k_dfps VARCHAR2(20) := 'YYYYMMDD';
  l_num_rows INTEGER; --variable to hold number of rows processed
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  del_partdata('PPMTA'); --delete part ID GL
  del_partdata('PPMEA'); --delete part ID GL

  msg('Populating list of Interval Partitioned Tables PPM Archive');
  l_num_rows := 0;
  INSERT INTO gfc_part_tables
  (recname, part_id, part_type, part_column
  , interval_Expr
  , subpart_type
--, tab_tablespace, idx_tablespace, tab_storage, idx_storage
--, method_opt
  )
  VALUES('PSPMTRANSARCH', 'PPMTA', 'I', 'PM_AGENT_STRT_DTTM'
  , 'NUMTOYMINTERVAL(1,''MONTH'')'
  , 'N'
--,'GLLARGE', 'PSINDEX', 'PCTUSED 90 PCTFREE **PCTFREE**', 'PCTFREE **PCTFREE**'
--, 'FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 254 LEDGER FISCAL_YEAR ACCOUNTING_PERIOD BOOK_CODE CURRENCY_CD BUSINESS_UNIT ACCOUNT PROJECT_ID'
  );
  l_num_rows := l_num_rows + SQL%ROWCOUNT;
  INSERT INTO gfc_part_tables
  (recname, part_id, part_type, part_column
  , interval_Expr
  , subpart_type
--, tab_tablespace, idx_tablespace, tab_storage, idx_storage
--, method_opt
  )
  VALUES('PSPMEVENTARCH', 'PPMEA', 'I', 'PM_AGENT_DTTM'
  , 'NUMTOYMINTERVAL(1,''MONTH'')'
  , 'N'
--,'GLLARGE', 'PSINDEX', 'PCTUSED 90 PCTFREE **PCTFREE**', 'PCTFREE **PCTFREE**'
--, 'FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 254 LEDGER FISCAL_YEAR ACCOUNTING_PERIOD BOOK_CODE CURRENCY_CD BUSINESS_UNIT ACCOUNT PROJECT_ID'
  );
  l_num_rows := l_num_rows + SQL%ROWCOUNT;
  msg(TO_CHAR(l_num_rows)||' rows inserted.');

--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
  msg('Insert metadata for global non-partitioned index on partitioned PPM Archive tables.');
  INSERT INTO gfc_part_indexes
  (recname, indexid, part_id, part_type, part_column, subpart_type, subpart_column, hash_partitions)
  SELECT   t.recname
  ,        i.indexid
  ,        t.part_id
  ,        'N' part_type
  ,        t.part_column
  ,        'N' subpart_type
  ,        '' subpart_column
  ,        t.hash_partitions
  FROM     gfc_part_tables t
  ,        psindexdefn i
  WHERE    t.recname = i.recname
  AND      t.part_type != 'N'
  AND      t.part_id IN('PPMTA','PPMEA')
  AND NOT EXISTS(
        SELECT 'x'
        FROM pskeydefn k, psrecfielddb f
        WHERE f.recname = i.recname
        AND   k.recname = f.recname_parent
        AND   k.indexid = i.indexid
        AND   k.fieldname IN(t.part_column,t.subpart_column)
        AND   k.keyposn <= 3 --partitioning key in first three columns of index
        );
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');


  l_num_rows := 0;
  msg('Populating Initial PPMTA Range Partitions');
  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) 
  SELECT 'PPMTA',1,'FIRST'
  ,      'TO_DATE('''||TO_CHAR(ADD_MONTHS(TRUNC(NVL(MIN(pm_agent_strt_dttm),SYSDATE),'MM'),1),k_dfps)||''','''||k_dfps||''')'
  FROM   pspmtransarch;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

  msg('Populating Initial PPMEA Range Partitions');
  INSERT INTO gfc_part_ranges (part_id, part_no, part_name, part_value) 
  SELECT 'PPMEA',1,'FIRST'
  ,      'TO_DATE('''||TO_CHAR(ADD_MONTHS(TRUNC(NVL(MIN(pm_agent_dttm),SYSDATE),'MM'),1),k_dfps)||''','''||k_dfps||''')'
  FROM   pspmeventarch;
  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');

-----------------------------------------------------------------------------------------------------------
--mapping between ranges and lists
-----------------------------------------------------------------------------------------------------------
--  msg('Insert PPM range-v-list partition mapping metadata');
--  INSERT INTO gfc_part_range_Lists
--  (part_id, range_name, list_name)
--  SELECT r.part_id, r.part_name, l.part_name
--  FROM   gfc_part_ranges r
--  ,      gfc_part_lists l
--  WHERE  l.part_id = r.part_id
--  AND    l.part_id IN('PPM%');
--
--  msg(TO_CHAR(SQL%ROWCOUNT)||' rows inserted.');
-----------------------------------------------------------------------------------------------------------
--End PPM Metadata
-----------------------------------------------------------------------------------------------------------
  commit;
  msg('PPM metadata load complete');
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END ppm_partdata;
--------------------------------------------------------------------------------------------------------------




--------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
PROCEDURE index_comp IS
  k_action CONSTANT VARCHAR2(64) := 'INDEX_COMP';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  msg('Setting Index Compression in DDL Overrides on Record');

  update pslock    set version = version + 1 where objecttypename IN('SYS','RDM');
  update psversion set version = version + 1 where objecttypename IN('SYS','RDM');
  
  update psrecdefn 
  set    version = (SELECT version FROM psversion WHERE objecttypename = 'RDM')
  ,      lastupddttm = SYSDATE
  ,      lastupdoprid = k_module
  where  recname IN('LEDGER','LEDGER_BUDG','S_FT_ACCTDET','S_FT_CICA_SUML','S_FT_USEXP','S_FT_USGEO','S_FT_USIRP','S_FT_USSTAT','S_LEDGER_ACCTS','S_LRG_ACT_CA');
  
  delete from psidxddlparm p
  where  recname IN('LEDGER','LEDGER_BUDG','S_FT_ACCTDET','S_FT_CICA_SUML','S_FT_USEXP','S_FT_USGEO','S_FT_USIRP','S_FT_USSTAT','S_LEDGER_ACCTS','S_LRG_ACT_CA')
  and    parmname IN('PCTFREE','INIT','NEXT');
  
  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN 'C' THEN ' COMPRESS 5' --erroneously 7
    WHEN 'D' THEN ' COMPRESS 4'
    WHEN 'E' THEN ' COMPRESS 1'
    WHEN 'F' THEN ' COMPRESS 4' --erroneously 7
    WHEN 'G' THEN ' COMPRESS 1'
    WHEN '_' THEN ' COMPRESS 3'
  END
  from  psindexdefn
  where recname = 'LEDGER'
  and   platform_ora = 1;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 4'
  END
  from  psindexdefn
  where recname = 'LEDGER_BUDG'
  and   platform_ora = 1 ;
 
  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 6'
  END
  from  psindexdefn
  where recname = 'S_FT_ACCTDET'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 7'
  END
  from  psindexdefn
  where recname = 'S_FT_CICA_SUML'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 3'
  END
  from  psindexdefn
  where recname = 'S_FT_USEXP'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN 'C' THEN ' COMPRESS 4'
    WHEN '_' THEN ' COMPRESS 3'
  END
  from  psindexdefn
  where recname = 'S_FT_USGEO'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 3'
  END
  from  psindexdefn
  where recname = 'S_FT_USIRP'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN 'C' THEN ' COMPRESS 6'
    WHEN '_' THEN ' COMPRESS 4'
  END
  from  psindexdefn
  where recname = 'S_FT_USSTAT'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN '_' THEN ' COMPRESS 5'
  END
  from  psindexdefn
  where recname = 'S_LEDGER_ACCTS'
  and   platform_ora = 1 ;

  insert into psidxddlparm
  select recname, indexid, 2, 0, 'PCTFREE', '5'||CASE indexid 
    WHEN 'A' THEN ' COMPRESS 4'
    WHEN 'B' THEN ' COMPRESS 6'
    WHEN '_' THEN ' COMPRESS 7'
  END
  from  psindexdefn
  where recname = 'S_LRG_ACT_CA'
  and   platform_ora = 1 ;

  update psindexdefn i
  set    platform_ora = 0
  where  recname IN('S_FT_ACCTDET','S_FT_CICA_SUML','S_FT_USEXP','S_FT_USGEO','S_FT_USIRP','S_FT_USSTAT','S_LEDGER_ACCTS','S_LRG_ACT_CA')
  and    platform_ora = 1
  and    indexid != '_';

  update psindexdefn i
  set    ddlcount = (SELECT COUNT(DISTINCT platformid) 
                     FROM   psidxddlparm d
                     WHERE  d.recname = i.recname
                     AND    d.indexid = i.indexid)
  where recname IN('LEDGER','LEDGER_BUDG','S_FT_ACCTDET','S_FT_CICA_SUML','S_FT_USEXP','S_FT_USGEO','S_FT_USIRP','S_FT_USSTAT','S_LEDGER_ACCTS','S_LRG_ACT_CA');

  commit;

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END index_comp;
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--procedure to truncate metdata tables
--------------------------------------------------------------------------------------------------------------
PROCEDURE truncmeta IS
  k_action CONSTANT VARCHAR2(64) := 'TRUNCMETA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  msg('Truncating metadata tables');
  exec_sql('TRUNCATE TABLE gfc_part_tables');
  exec_sql('TRUNCATE TABLE gfc_part_ranges');
  exec_sql('TRUNCATE TABLE gfc_temp_tables');
  exec_sql('TRUNCATE TABLE gfc_part_lists');
  exec_sql('TRUNCATE TABLE gfc_part_subparts');
  exec_sql('TRUNCATE TABLE gfc_part_indexes');

  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END truncmeta;
--------------------------------------------------------------------------------------------------------------
--procedure to apply compression attributes to tables
--------------------------------------------------------------------------------------------------------------
PROCEDURE comp_attrib IS
  k_action CONSTANT VARCHAR2(64) := 'COMP_ATTRIB';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
  l_sql CLOB;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  truncmeta;
  gp_partdata;

  msg('Checking Compression Attributes -v- Metadta');
  FOR i IN ( --table level compression where all the partitions are the same
with tp as (
SELECT table_name, compression, compress_for
,      count(*) num_parts
FROM   user_tab_partitions
GROUP BY table_name, compression, compress_for
), t as (
select tp.*, count(*) over (partition by table_name) compression_types
from tp
), x as (
select pt.part_id, pt.part_type, t.table_name
,      regexp_substr(pt.tab_storage,'(COMPRESS|NOCOMPRESS)[ [:alnum:]]+',1,1,'i') tab_storage
,      t.compression, t.compress_for
FROM   psrecdefn r
INNER JOIN t
  ON t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
  AND t.compression_types = 1
INNER JOIN gfc_part_tables pt
  ON r.recname = pt.recname
), y as (
SELECT x.*
, CASE WHEN compression = 'ENABLED' AND UPPER(tab_storage) = 'COMPRESS FOR '||x.compress_for THEN NULL --do nothing
       WHEN compression = 'ENABLED' AND compress_for = 'BASIC' AND UPPER(tab_storage) = 'COMPRESS' THEN NULL --do nothing
       WHEN compression IN('DISABLED','NONE') AND (UPPER(tab_storage) = 'NOCOMPRESS' OR tab_storage IS NULL) THEN NULL --do nothing
       WHEN tab_storage IS NULL AND x.compression = 'ENABLED' THEN 'NOCOMPRESS'
       ELSE x.tab_storage
  END cmd
FROM x
)
SELECT * FROM y
WHERE cmd IS NOT NULL
ORDER BY 1,2,3,4
) LOOP
    l_sql := 'ALTER TABLE '||i.table_name||' '||i.cmd;
    dbms_output.put_line(l_sql);
    EXECUTE IMMEDIATE l_sql;
  END LOOP;

  FOR i IN ( --partition level compression settings
with x as (
select pt.part_id, pt.part_type, t.table_name, pr.part_name, tp.partition_name
,      COALESCE(regexp_substr(pr.tab_storage,'(COMPRESS|NOCOMPRESS)[ [:alnum:]]+',1,1,'i')
               ,regexp_substr(pt.tab_storage,'(COMPRESS|NOCOMPRESS)[ [:alnum:]]+',1,1,'i')
               )  tab_storage
,      COALESCE(tp.compression, t.compression) compression
,      COALESCE(tp.compress_for, t.compress_for) compress_for
FROM   psrecdefn r
INNER JOIN user_tables t
  ON t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
INNER JOIN gfc_part_tables pt
  ON r.recname = pt.recname
INNER JOIN gfc_part_ranges pr
  ON  pt.part_type = 'R'
  AND pr.part_id = pt.part_id
INNER JOIN user_tab_partitions tp
 ON tp.table_name = t.table_name
 AND tp.partition_name = pt.recname||'_'||pr.part_name
), y as (
SELECT x.*
, CASE WHEN compression = 'ENABLED' AND UPPER(tab_storage) = 'COMPRESS FOR '||x.compress_for THEN NULL --do nothing
       WHEN compression = 'ENABLED' AND compress_for = 'BASIC' AND UPPER(tab_storage) = 'COMPRESS' THEN NULL --do nothing
       WHEN compression IN('DISABLED','NONE') AND (UPPER(tab_storage) = 'NOCOMPRESS' OR tab_storage IS NULL) THEN NULL --do nothing
       WHEN tab_storage IS NULL AND x.compression = 'ENABLED' THEN 'NOCOMPRESS'
       ELSE x.tab_storage
  END cmd
FROM x
)
SELECT * FROM y
WHERE cmd IS NOT NULL
ORDER BY 1,2,3,4
) LOOP
    l_sql := 'ALTER TABLE '||i.table_name||' MODIFY PARTITION '||i.partition_name||' '||i.cmd;
    dbms_output.put_line(l_sql);
    EXECUTE IMMEDIATE l_sql;
  END LOOP;
  
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END comp_attrib;
--------------------------------------------------------------------------------------------------------------
--procedure to call all other partdata procedures
--------------------------------------------------------------------------------------------------------------
PROCEDURE partdata IS
  k_action CONSTANT VARCHAR2(64) := 'PARTDATA';
  l_module v$session.module%TYPE;
  l_action v$session.action%TYPE;
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>k_action);

  truncmeta;

--gp_partdata;
wl_partdata;
ppm_partdata;

--------------------------------------------------------------------------------------------------------------
--set default tablespaces
--------------------------------------------------------------------------------------------------------------
msg('Set default table tablespace where not already set');
UPDATE gfc_part_tables x
SET    x.tab_tablespace = (SELECT y.ddlspacename 
                           FROM   psrectblspc y 
                           WHERE  y.recname = x.recname
                           AND    y.dbtype = (SELECT MAX(y2.dbtype)
                                              FROM   psrectblspc y2
                                              WHERE  y2.recname = x.recname
                                              AND    y2.dbtype IN(' ','2'))) /*default PSFT tablespace*/
WHERE	 x.tab_tablespace IS NULL;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

msg('Set default index tablespace where not already set');
UPDATE 	gfc_part_tables x
SET	x.idx_tablespace = 'PSINDEX' /*default PSFT tablespace*/
WHERE	x.idx_tablespace IS NULL;
msg(TO_CHAR(SQL%ROWCOUNT)||' rows updated.');

dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
END partdata;
-----------------------------------------------------------------------------------------------------------

END gfc_partdata;
/
set termout on serveroutput on
show errors

set pause on
EXECUTE gfc_partdata.partdata;
--EXECUTE gfc_partdata.comp_attrib;

spool off

