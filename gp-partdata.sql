REM gp-partdata.sql
REM 12. 3.2004 added GPGB_PAYMENT
REM 19. 6.2003 added GPCH_TX_DATA 
REM  9. 6.2004 added added working storage tables
REM 13. 2.2008 added GP working storage tables at Kelly
REM 20  2.2008 added GP_RTO_CRD_WRK
REM 21. 5.2008 added hcm9.0 tables
REM 14.11.2008 added SOVR tables
REM  5. 3.2009 added GP_ACC_LINE_STG
REM 13. 1.2010 added EDI tables
REM  3.10.2010 added tables at Morrisons
REM  4.10.2010 ASG753665 additional indexes for payroll identification
REM 19. 7.2012 added this change history block
--------------------------------------------------------------------------------------------------------------
--view to identify country extention tables
--------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gfc_installed_gp AS
SELECT 	recname, 
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
FROM	psrecdefn r
,	ps_installation i
WHERE   r.rectype IN(0,7) --only SQL tables can be partitioned or rebuilt as GTTs
;

ttitle 'GP Installation Flags'
column arg format a3
column aus format a3
column BRA format a3
column CAN format a3
column CHE format a3
column DEU format a3
column ESP format a3
column FRA format a3
column HKG format a3
column IND format a3
column IRL format a3
column ITA format a3
column JPN format a3
column MEX format a3
column MYS format a3
column NLD format a3
column NZL format a3
column SGP format a3
column TWN format a3
column UK  format a2
column USA format a3


SELECT	
 INSTALLED_GP_ARG arg
,INSTALLED_GP_AUS aus
,INSTALLED_GP_BRA bra
,INSTALLED_GP_CAN can
,INSTALLED_GP_CHE che
,INSTALLED_GP_DEU deu
,INSTALLED_GP_ESP esp
,INSTALLED_GP_FRA fra
,INSTALLED_GP_HKG hkg
,INSTALLED_GP_IND ind
,INSTALLED_GP_IRL irl
,INSTALLED_GP_ITA ita
,INSTALLED_GP_JPN jpn
,INSTALLED_GP_MEX mex
,INSTALLED_GP_MYS mys
,INSTALLED_GP_NLD nld
,INSTALLED_GP_NZL nzl
,INSTALLED_GP_SGP sgp
,INSTALLED_GP_TWN twn
,INSTALLED_GP_UK  uk
,INSTALLED_GP_USA usa
FROM ps_installation
/
ttitle off

--------------------------------------------------------------------------------------------------------------
--insert data to describe temporary tables
--country specific tables for installed country extentions only will be added
--------------------------------------------------------------------------------------------------------------
INSERT INTO gfc_temp_tables                 
SELECT  r.recname
FROM    gfc_installed_gp r
WHERE   r.installed_gp != 'N'
AND	r.rectype IN(0,7) --only normal tables can be partitioned
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
		'GPGB_PSLIP_ED_W','GPGB_PSLIP_BL_W',
        'GPGB_EDI_RSLT', /*added 12.2.2010*/
		--element summary report
		'WMS_GP_ELN_TMP','WMS_GP_RSLT_ED','WMS_GP_RSLT_AC',
		--other customer tables
		'S1H_T_TEMP_JOB',
		'S1H_TAL_ERN_DED','S1H_TAL_PRCSTAT','S1H_TAL_RSLTPIN',
		'S1H_TE_GLB_TEMP',
		'S1H_TFIXAMTTMP1','S1H_TFIXAMTTMP2',
		'S1H_TFLXRP_DATA',
		'S1H_TGL_CAL_TMP','S1H_TGL_DATA',
		'S1H_TINTF_SORT',
		'S1H_TLAL_TMP',
		'S1H_TPMM_REPORX','S1H_TPMM_RPEVAX',
		'S1H_TLAWCHECK'  ,'S1H_TLAWCHECK6',
		'S1H_TRSLT_ERN'  ,'S1H_TRSLT_DELTA','S1H_TRSLT_PIN',
		'S1H_TXLS_RTABCT','S1H_TXLS_RTABLT','S1H_TXLS_RTABST', 
		'S1H_TXLS_RTCORE','S1H_TXLS_RTCREL',
		'S1H_TXLS_RTHLNG','S1H_TXLS_RTHTMP',
		'S1H_VGP_RSLT')
OR r.recname IN(
		SELECT t.recname
		FROM   psaeappltemptbl t   
		,      psaeappldefn a   
		WHERE  a.ae_applid = t.ae_applid   
		AND    a.ae_disable_restart = 'Y' --restart is disabled   
	    AND    a.ae_applid IN('GP_PMT_PREP','GP_GL_PREP','GPGB_PSLIP','GPGB_PSLIP_X','GPGB_EDI') /*limited to just GP AE processes*/
		))
/
--------------------------------------------------------------------------------------------------------------
--insert data to specify the tables to be partitioned
--country specific tables for installed country extentions only will be added
--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_tables  WHERE part_id = 'GP';
DELETE FROM gfc_part_indexes WHERE part_id = 'GP';
--DELETE FROM gfc_ps_indexdefn WHERE part_id = 'GP';
--DELETE FROM gfc_ps_keydefn   WHERE part_id = 'GP';



INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'GP'
,	'EMPLID', 'R'
FROM    gfc_installed_gp r
WHERE   r.installed_gp != 'N'
AND	r.rectype = 0 /*only normal tables can be partitioned*/
AND     (       r.recname IN(
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
                        'COMPENSATION',           /*hr compensation data added 18.1.2010*/
                        'X_PYE_OVRD_ET',          /*customer table*/
                        'GPGB_PAYMENT',           /*added 12.3.2004*/
                        'GPGB_PSLIP_P_ED',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_PSLIP_P_BL',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_PSLIP_P_HR',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_PSLIP_P_FT',        /*uk payslip process gpgb_pslip can now be run streamed*/
                        'GPGB_EDI_RSLT',          /*13.1.2010-result table for GPGB_EDI process*/
                        'TL_PAYABLE_TIME',        /*13.2.2008-added for kelly-TL*/
                        'WMS_GB_EDI_RSLT'         /*13.1.2010-custom edi result table*/
                        ) 
	OR      r.recname IN( 			  /*range partition any writable arrays*/
			SELECT  recname
                        FROM    ps_gp_wa_array
                        )
	)
/



--------------------------------------------------------------------------------------------------------------
--archive tables
--------------------------------------------------------------------------------------------------------------
INSERT INTO gfc_part_tables
(part_id, recname, src_table_name, part_column, part_type, tab_storage, idx_storage) 
SELECT  'GPARCHY'
,	ar.recname
,	DECODE(sr.sqltablename,' ','PS_'||sr.recname,sr.sqltablename)
,	'CAL_RUN_ID', 'R'
,       'PCTFREE 0 PCTUSED 99','PCTFREE 0 COMPRESS'
FROM    psrecdefn ar
,	psarchobjrec ao
,	psrecdefn sr
,	gfc_part_tables p
WHERE   ar.rectype = 0 --only normal tables can be partitioned
and	ar.recname = ao.hist_recname
and	ao.recname = sr.recname
and	ao.psarch_object = 'GP_RSLT_ARCHIVE'
and	p.recname = sr.recname
order by p.recname
/

--these tables to have monthly partitions
UPDATE gfc_part_tables
SET    part_id = 'GPARCHM'
WHERE  part_id = 'GPARCHY'
AND    recname IN('GP_RSLT_ACM_HST','GP_RSLT_PIN_HST','GP_RSLT_E_D_HST'
                 ,'GP_RSL_PI_S_HST','GP_PY_PR_ST_HST','GP_PY_SG_ST_HST')
/

merge into gfc_part_tables u
using (
select	s.recname
, 	a.recname arch_recname
from	gfc_part_tables s
,	gfc_part_tables a
,	psrecdefn rs
where	s.part_id = 'GP'
and	a.part_id IN('GPARCHY','GPARCHM')
and	rs.recname = s.recname
and	a.src_table_name = DECODE(rs.sqltablename,' ','PS_'||rs.recname,rs.sqltablename)
) s
ON 	(u.recname = s.recname)
when 	matched then update
set 	arch_recname = s.arch_recname
/


update gfc_part_tables u
SET criteria = 'WHERE cal_run_id <= '''||(
	SELECT SUBSTR(MAX(cal_run_id),1,7)||'ZZZ'
	FROM   ps_gp_cal_run_dtl
        WHERE  prd_end_dt < ADD_MONTHS(SYSDATE,-14)
	)||''''
where part_id IN('GPARCHY','GPARCHM')
AND criteria IS NULL
AND EXISTS(
	SELECT 'x'
	FROM   psrecfielddb f
	WHERE  f.recname = u.recname
	AND    f.fieldname = 'CAL_RUN_ID')
/
update gfc_part_tables u
SET criteria = 'WHERE cal_run_id > '''||(
	SELECT SUBSTR(MAX(cal_run_id),1,7)||'ZZZ'
	FROM   ps_gp_cal_run_dtl
        WHERE  prd_end_dt < ADD_MONTHS(SYSDATE,-14)
	)||''''
where part_id = 'GP'
AND arch_recname IS NOT NULL
AND criteria IS NULL
AND EXISTS(
	SELECT 'x'
	FROM   psrecfielddb f
	WHERE  f.recname = u.recname
	AND    f.fieldname = 'CAL_RUN_ID')
/


--------------------------------------------------------------------------------------------------------------
--move temp tables to partitioned tables
--------------------------------------------------------------------------------------------------------------
--just want to build partitioned working storage table
--DELETE FROM gfc_part_tables;


--INSERT INTO gfc_part_tables
--(recname, part_id, part_column, part_type, stats_type) 
--SELECT  recname, 'GP', 'EMPLID', 'R', 'D'
--FROM   gfc_temp_tables
--WHERE	1=1
--AND	recname LIKE 'GP%WRK'
--AND NOT recname IN('GP_CAL_TS_WRK','GP_PKG_ELEM_WRK','GP_TLTRC_WRK')
--/

--DELETE FROM gfc_temp_tables
--WHERE recname IN (SELECT recname FROM gfc_part_tables)
--/

--------------------------------------------------------------------------------------------------------------
--delete rows from meta data tables which are not to be built, use this to restrict output
--------------------------------------------------------------------------------------------------------------
--DELETE FROM gfc_part_tables
--WHERE NOT recname IN('GP_RTO_TRGR')
--/
--
--DELETE FROM gfc_temp_tables
--/

--------------------------------------------------------------------------------------------------------------
--specify list subpartitioned tables
--------------------------------------------------------------------------------------------------------------
UPDATE 	gfc_part_tables
SET	subpart_type = 'L'
,	subpart_column = 'CAL_RUN_ID'
,	hash_partitions = 0
WHERE 	recname IN('GP_RSLT_ACUM', 'GP_RSLT_PIN'
--,'GP_GL_DATA' --not worth subpartitioning at Kelly
--,'GP_PYE_SEG_STAT' -- subpartitioning does not work well with retro queries at kelly
--,'GP_PYE_PRC_STAT' -- subpartitioning does not work well with retro queries at kelly
--,'GP_RSLT_PI_SOVR', 'GP_RSLT_PI_DATA' --14.2.2008 removed at kelly
)
/

UPDATE 	gfc_part_tables
SET	subpart_type = 'L'
,	subpart_column = 'SRC_CAL_RUN_ID'
,	hash_partitions = 0
WHERE 	recname IN('GP_PI_GEN_DATA') --no point subpartitioning because rows deleted during processing
AND 1=2 -- and it causes performance problems in GPPDPDM5_S_PIGSOVR
/

--------------------------------------------------------------------------------------------------------------
--set storage options on partitioned objects
--------------------------------------------------------------------------------------------------------------

UPDATE 	gfc_part_tables
SET	tab_tablespace = 'GPAPP'   /*default PSFT tablespace*/
,	idx_tablespace = 'PSINDEX' /*default PSFT tablespace*/
,	tab_storage = 'PCTUSED 95 PCTFREE 1'
,	idx_storage = 'PCTFREE 1'
WHERE	part_id = 'GP'
/


UPDATE 	gfc_part_tables
SET	idx_storage = 'PCTFREE **PCTFREE**'
WHERE 	recname IN('GP_PYE_PRC_STAT','GP_PYE_SEG_STAT')
/


UPDATE 	gfc_part_tables
SET	tab_storage = 'PCTUSED 80 PCTFREE 15'
WHERE 	recname IN('GP_PYE_SEG_STAT')
/


--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitioned
--------------------------------------------------------------------------------------------------------------
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
	)
;

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
	AND   g.indexid = i.indexid)
;


--GP_PYE_SEG_STAT B&D

--------------------------------------------------------------------------------------------------------------

--Index A on SEQ_NBR only
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_PAYABLE_TIME','A','GP','N',' ')
/

--Index C leads on TRC, EMPLID, still worth range partitioning
--INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
--VALUES ('TL_PAYABLE_TIME','C','GP','N',' ')
--/

--Index D on DEPTID only - don't partition
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_PAYABLE_TIME','D','GP','N',' ')
/

--Index Not built - leads on CHARTFIELD1
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('TL_PAYABLE_TIME','E','GP','N',' ')
/
--ASG614499 Index F on EMPLID, EMPL_RCD, APPRV_PRCS_DTTM to be locally range partitioned
--INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
--VALUES ('TL_PAYABLE_TIME','F','GP','R','APPRV_PRCS_DTTM')
--/


INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','0','GP','N',' ', 'COMPRESS 1')
/
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','1','GP','N',' ', 'COMPRESS 1')
/
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','2','GP','N',' ', 'COMPRESS 1')
/
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','3','GP','N',' ', 'COMPRESS 1')
/
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','4','GP','N',' ', 'COMPRESS 1')
/
/*-------------------------------------------------------------------------------------------
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage)
VALUES ('JOB','E','GP','N',' ', 'COMPRESS 1')
/
/*-------------------------------------------------------------------------------------------*/

--------------------------------------------------------------------------------------------------------------
--insert data to generate the function based indexed
--------------------------------------------------------------------------------------------------------------
--(recname,indexid,keyposn,fieldname)
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage, name_suffix)
VALUES ('TL_PAYABLE_TIME','Y','GP','N',' ', 'PCTFREE 1','_SPARSE');
INSERT INTO gfc_ps_indexdefn (recname, indexid, subrecname, subindexid, platform_ora, custkeyorder, uniqueflag)
VALUES ('TL_PAYABLE_TIME','Y','TL_PAYABLE_TIME','Y', 1, 0, 0);

--(recname,indexid,keyposn,fieldname)
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Y',1,'DECODE(PAYABLE_STATUS,''SP'',''SP'',NULL)',1);
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Y',2,'DECODE(PAYABLE_STATUS,''SP'',EMPLID,NULL)',1);
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Y',3,'DECODE(PAYABLE_STATUS,''SP'',DUR,NULL)',1);
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Y',4,'DECODE(PAYABLE_STATUS,''SP'',EMPL_RCD,NULL)',1);

INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column, idx_storage, name_suffix)
VALUES ('TL_PAYABLE_TIME','Z','GP','L',' ', 'PCTFREE 1','_SPARSE');
INSERT INTO gfc_ps_indexdefn (recname, indexid, subrecname, subindexid, platform_ora, custkeyorder, uniqueflag)
VALUES ('TL_PAYABLE_TIME','Z','TL_PAYABLE_TIME','Z', 1, 0, 0);

INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Z',1,'DECODE(PAYABLE_STATUS,''NA'',''NA'',NULL)',1);
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Z',2,'DECODE(PAYABLE_STATUS,''NA'',EMPLID,NULL)',1);
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Z',3,'DECODE(PAYABLE_STATUS,''NA'',DUR,NULL)',1);
INSERT INTO gfc_ps_keydefn 
VALUES ('TL_PAYABLE_TIME','Z',4,'DECODE(PAYABLE_STATUS,''NA'',EMPL_RCD,NULL)',1);

--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategy
--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_ranges
WHERE part_id = 'GP'
/

INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value)
SELECT	'GP', strm_num
, 	LTRIM(TO_CHAR(strm_num,'000')) part_name
,	NVL(LEAD(''''||emplid_from||'''',1) OVER (ORDER BY strm_Num),'MAXVALUE') part_value
FROM	ps_gp_strm
/


UPDATE 	gfc_part_ranges
SET	tab_tablespace = 'GPSTRM'||LTRIM(TO_CHAR(MOD(part_no-1,32)+1,'00'))||'TAB'
,	idx_tablespace = 'GPSTRM'||LTRIM(TO_CHAR(MOD(part_no-1,32)+1,'00'))||'IDX'
--SET	tab_tablespace = 'GPSTRM'||part_name||'TAB'
--,	idx_tablespace = 'GPSTRM'||part_name||'IDX'
--SET	tab_tablespace = 'GPTABPART'||part_name||'' 
--,	idx_tablespace = 'GPIDXPART'||part_name||''
--,	tab_storage = '/*TAB STORAGE*/'
--,	idx_storage = '/*IDX STORAGE*/'
WHERE 1=1
/

--GPMAX is identical to GP but has maxvalue to support globally partitioned indexes
INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value, tab_tablespace, idx_tablespace)
SELECT	'GPMAX', part_no, part_name, part_value, tab_tablespace, idx_tablespace
FROM	gfc_part_ranges
WHERE	part_id = 'GP'
/

INSERT INTO gfc_part_ranges 
(part_id, part_no, part_name, part_value)
VALUES
('GPMAX', 9999, 9999, 'MAXVALUE')
/
-----------------------------------------------------------------------------------------------------------
--insert data to list partitions
--2007 onwards
-----------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_lists
WHERE part_id = 'GP'
/

INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
VALUES ('GP',9999,'Z_OTHERS','DEFAULT')
;

-----------------------------------------------------------------------------------------------------------
--monthly partitions for Pensioners
-----------------------------------------------------------------------------------------------------------
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
SELECT	'GP'
,	year
, 	      LTRIM(TO_CHAR(y.year,'0000'))
, 	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM01'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM02'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM03'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM04'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM05'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM06'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM07'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM08'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM09'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM10'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM11'','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'PM12'''
FROM (
	SELECT 	2007+rownum as year
	FROM   	dba_objects
	WHERE 	rownum <= 3
     	) y
WHERE 1=2
ORDER BY 1,2,3
/


-----------------------------------------------------------------------------------------------------------
--lunar monthly partitions for weekly and lunar monthy payees
-----------------------------------------------------------------------------------------------------------
INSERT INTO gfc_part_lists
(part_id, part_no, part_name, list_value)
SELECT	'GP'
,	year+period/100
, 	LTRIM(TO_CHAR(y.year,'0000'))||'L'||LTRIM(TO_CHAR(p.period,'00'))
,	''''||LTRIM(TO_CHAR(y.year,'0000'))||'UL'||LTRIM(TO_CHAR(  p.period  ,'00'))||''','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'UW'||LTRIM(TO_CHAR(4*p.period-3,'00'))||''','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'UW'||LTRIM(TO_CHAR(4*p.period-2,'00'))||''','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'UW'||LTRIM(TO_CHAR(4*p.period-1,'00'))||''','||
	''''||LTRIM(TO_CHAR(y.year,'0000'))||'UW'||LTRIM(TO_CHAR(4*p.period-0,'00'))||''''
FROM 	(
	SELECT  rownum as period
	FROM 	dba_objects
	WHERE	rownum <= 14
     	) p
,    	(
	SELECT 	2007+rownum as year
	FROM   	dba_objects
	WHERE 	rownum <= 3 --years of list partitions
     	) y
WHERE	period <= DECODE(y.year,2023,14,13)
AND	LTRIM(TO_CHAR(y.year,'0000'))||'L'||LTRIM(TO_CHAR(p.period,'00')) >= '2008L09' --suppress build of unnecessary GP lists
ORDER BY 1,2,3
/

--need to add specific years where W53
UPDATE	gfc_part_lists a
SET	a.list_value = a.list_value ||','''||SUBSTR(a.part_name,1,4)||'UW53'''
WHERE	a.part_id = 'GP'
AND	a.part_name = (
		SELECT 	MAX(b.part_name)
		FROM	gfc_part_lists b
		WHERE	a.part_id = 'GP'
		AND	SUBSTR(a.part_name,1,5) = SUBSTR(b.part_name,1,5))
AND	(a.part_name LIKE '2013_' OR
         a.part_name LIKE '2023_') -- and others
AND	a.list_value like '%UW52%'
;


-----------------------------------------------------------------------------------------------------------
--lunar monthly partitions for weekly and lunar monthy Gibraltar Migration payees (GIB Tax Year starts 1st July)
-----------------------------------------------------------------------------------------------------------
UPDATE	gfc_part_lists a
SET	a.list_value = a.list_value||','||
	''''||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4))-1,'0000'))||'GML'||LTRIM(TO_CHAR(  TO_NUMBER(SUBSTR(part_name,6))+10  ,'00'))||''','||
	''''||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4))-1,'0000'))||'GMW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))+40-3,'00'))||''','||
	''''||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4))-1,'0000'))||'GMW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))+40-2,'00'))||''','||
	''''||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4))-1,'0000'))||'GMW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))+40-1,'00'))||''','||
	''''||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4))-1,'0000'))||'GMW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))+40  ,'00'))||''''
WHERE	a.part_id = 'GP'
and	SUBSTR(part_name,6) <= '03'
and	part_no <= 2009.03
;


UPDATE	gfc_part_lists a
SET	a.list_value = a.list_value||','||
	''''||SUBSTR(part_name,1,4)||'GML'||LTRIM(TO_CHAR(  TO_NUMBER(SUBSTR(part_name,6))-3   ,'00'))||''','||
	''''||SUBSTR(part_name,1,4)||'GMW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))-12-3,'00'))||''','||
	''''||SUBSTR(part_name,1,4)||'GMW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))-12-2,'00'))||''','||
	''''||SUBSTR(part_name,1,4)||'GMW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))-12-1,'00'))||''','||
	''''||SUBSTR(part_name,1,4)||'GMW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))-12  ,'00'))||''''
WHERE	a.part_id = 'GP'
and	SUBSTR(part_name,6) > '03'
and	part_no <= 2009.03
;

-----------------------------------------------------------------------------------------------------------
--lunar monthly partitions for weekly and lunar monthy Gibraltar payees (GIB Tax Year starts 1st July)
-----------------------------------------------------------------------------------------------------------
UPDATE	gfc_part_lists a
SET	a.list_value = a.list_value||','||
	''''||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4))-1,'0000'))||'GL'||LTRIM(TO_CHAR(  TO_NUMBER(SUBSTR(part_name,6))+10  ,'00'))||''','||
	''''||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4))-1,'0000'))||'GW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))+40-3,'00'))||''','||
	''''||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4))-1,'0000'))||'GW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))+40-2,'00'))||''','||
	''''||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4))-1,'0000'))||'GW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))+40-1,'00'))||''','||
	''''||LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(part_name,1,4))-1,'0000'))||'GW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))+40  ,'00'))||''''
WHERE	a.part_id = 'GP'
and	SUBSTR(part_name,6) <= '03'
and	part_no > 2009.03
and	part_no < 9999
;


UPDATE	gfc_part_lists a
SET	a.list_value = a.list_value||','||
	''''||SUBSTR(part_name,1,4)||'GL'||LTRIM(TO_CHAR(  TO_NUMBER(SUBSTR(part_name,6))-3   ,'00'))||''','||
	''''||SUBSTR(part_name,1,4)||'GW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))-12-3,'00'))||''','||
	''''||SUBSTR(part_name,1,4)||'GW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))-12-2,'00'))||''','||
	''''||SUBSTR(part_name,1,4)||'GW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))-12-1,'00'))||''','||
	''''||SUBSTR(part_name,1,4)||'GW'||LTRIM(TO_CHAR(4*TO_NUMBER(SUBSTR(part_name,6))-12  ,'00'))||''''
WHERE	a.part_id = 'GP'
and	SUBSTR(part_name,6) > '03'
and	part_no > 2009.03
and	part_no < 9999
;

-----------------------------------------------------------------------------------------------------------
--lunar monthly partitions for weekly and lunar monthy Gibraltar payees (GIB Tax Year starts 1st July)
-----------------------------------------------------------------------------------------------------------
--need to add specific years where W53
UPDATE	gfc_part_lists a
SET	a.list_value = a.list_value ||','''||SUBSTR(a.part_name,1,4)||'GW53'''
WHERE	a.part_id = 'GP'
AND	a.part_name = (
		SELECT 	MAX(b.part_name)
		FROM	gfc_part_lists b
		WHERE	a.part_id = 'GP'
		AND	SUBSTR(a.part_name,1,5) = SUBSTR(b.part_name,1,5))
AND	(a.part_name LIKE '2013_' OR
         a.part_name LIKE '2023_') -- and others
AND	a.list_value like '%GW52%'
;

UPDATE 	gfc_part_lists
SET 	tab_tablespace = 'GP'||part_name||'TAB'
,   	idx_tablespace = 'GP'||part_name||'IDX'
--,	tab_storage = '/*TAB STORAGE*/'
--,	idx_storage = '/*IDX STORAGE*/'
WHERE 	part_id = 'GP'
AND	part_no < 9999
AND 1=1
/

-----------------------------------------------------------------------------------------------------------
--set compression on historical list partitions
-----------------------------------------------------------------------------------------------------------
UPDATE 	gfc_part_lists
SET	tab_storage = 'PCTFREE 0 COMPRESS'
,	idx_storage = 'PCTFREE 0'
WHERE part_id IN('GP')
AND   part_no < 2010
/

-----------------------------------------------------------------------------------------------------------
--set tablespaces for GP range partitions
-----------------------------------------------------------------------------------------------------------
UPDATE 	gfc_part_ranges
SET	tab_tablespace = 'GPSTRM'||LTRIM(TO_CHAR(MOD(part_no-1,32)+1,'00'))||'TAB'
,	idx_tablespace = 'GPSTRM'||LTRIM(TO_CHAR(MOD(part_no-1,32)+1,'00'))||'IDX'
--SET	tab_tablespace = 'GPSTRM'||part_name||'TAB'
--,	idx_tablespace = 'GPSTRM'||part_name||'IDX'
--SET	tab_tablespace = 'GPTABPART'||part_name||'' 
--,	idx_tablespace = 'GPIDXPART'||part_name||''
--,	tab_storage = '/*TAB STORAGE*/'
--,	idx_storage = '/*IDX STORAGE*/'
WHERE part_id IN('GP')
/
-----------------------------------------------------------------------------------------------------------
--statistics options
-----------------------------------------------------------------------------------------------------------
UPDATE	gfc_part_tables
SET 	method_opt = 'FOR COLUMNS cal_run_id SIZE AUTO, FOR ALL COLUMNS SIZE 1'
WHERE 	part_id IN('GP')
AND	subpart_type = 'L'
/
-----------------------------------------------------------------------------------------------------------
--mapping between ranges and lists
-----------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_range_lists
WHERE part_id IN('GP')
/

INSERT INTO gfc_part_range_Lists
(part_id, range_name, list_name)
SELECT r.part_id, r.part_name, l.part_name
FROM   gfc_part_ranges r
,      gfc_part_lists l
WHERE  l.part_id = r.part_id
AND    l.part_id IN('GP')
/

-----------------------------------------------------------------------------------------------------------
--delete range/list combinations that are not needed
-----------------------------------------------------------------------------------------------------------
--UPDATE gfc_part_range_lists
--SET   build = 'N'

--DELETE FROM gfc_part_range_lists
--WHERE build = 'Y'
--AND   (  (list_name like 'IRL%' AND range_name != '01')
--      OR (list_name like 'UK%'  AND NOT range_name IN('02','03','04','05','06','07','08')))
--AND   build = 'Y'
--AND   part_id = 'GP'
--/

--Uncomment this if you to just rebuild composite partitioned tables
--DELETE FROM gfc_temp_tables --WHERE RECNAME != ' '
--;
--DELETE FROM gfc_part_tables 
--WHERE subpart_type = 'N'
--RECNAME != 'GP_PYE_SEG_STAT'
--;


--Uncomment this if you to just rebuild composite partitioned tables
--DELETE FROM gfc_temp_tables;
--DELETE FROM gfc_part_tables WHERE SUBPART_TYPE IS NULL;


