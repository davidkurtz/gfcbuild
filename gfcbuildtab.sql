rem gfcbuildtab.sql
rem (c) Go-Faster Consultancy
rem tables and views required by gfcbuildpkg.sql
rem 16.09.2009 - extracted from gfcbuildpkg.sql
rem 20.10.2014 - replace gfc_part_range_lists with gfc_part_subparts
rem 10.02.2015 - added tempinstanceonline count
rem 02.03.2015 - increase method_opt to 200, interval range partitioning
rem 25.06.2021 - Extract ownerid from ps.psdbowner

clear screen
set echo on
spool gfcbuildtab

WHENEVER SQLERROR CONTINUE

ROLLBACK;

@@psownerid
ALTER SESSION SET recyclebin = off;
ALTER SESSION SET current_schema=&&ownerid;

DROP TABLE &&ownerid..gfc_ps_tables PURGE;
DROP TABLE &&ownerid..gfc_ps_tab_columns PURGE;
DROP TABLE &&ownerid..gfc_ora_tab_columns PURGE;
DROP TABLE &&ownerid..gfc_ps_indexdefn PURGE;
DROP TABLE &&ownerid..gfc_ps_keydefn PURGE;
DROP TABLE &&ownerid..gfc_ddl_script PURGE;
DROP TABLE &&ownerid..gfc_ps_idxddlparm PURGE;

DROP TABLE &&ownerid..gfc_part_tables PURGE;
DROP TABLE &&ownerid..gfc_part_indexes PURGE;
DROP TABLE &&ownerid..gfc_part_ranges PURGE;
DROP TABLE &&ownerid..gfc_temp_tables PURGE;
DROP TABLE &&ownerid..gfc_part_lists PURGE;
DROP TABLE &&ownerid..gfc_part_subparts PURGE;
DROP TABLE &&ownerid..gfc_part_range_lists PURGE;  
DROP VIEW  &&ownerid..gfc_part_range_lists;

--gfc_ps_tables holds the records for which DDL scripts are to be regeneated by this script
CREATE
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_ps_tables
(recname            VARCHAR2(15)    NOT NULL
,table_name         VARCHAR2(18)    NOT NULL
,table_type         VARCHAR2(1)
,rectype            NUMBER
,temptblinstances   NUMBER
,tempinstanceonline NUMBER --10.2.2015 added
,override_schema    VARCHAR2(30)
,match_db           VARCHAR2(30)
,CONSTRAINT gfc_ps_tables PRIMARY KEY(recname)
);

--gfc_ps_tab_columns holds a list of columns for tables to be recreated.   Any sub-records will be expanded recursively
CREATE
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_ps_tab_columns
(recname         VARCHAR2(15)    NOT NULL
,fieldname       VARCHAR2(18)    NOT NULL
,useedit         number          NOT NULL
,fieldnum        number          NOT NULL
,subrecname      VARCHAR2(15)    NOT NULL
,CONSTRAINT gfc_ps_tab_columns PRIMARY KEY(recname, fieldname)
);

--gfc_ora_tab_columns
CREATE
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_ora_tab_columns
(table_name	     VARCHAR2(30)	 NOT NULL
,column_name     VARCHAR2(30)	 NOT NULL
,column_id	     NUMBER 		 NOT NULL
,CONSTRAINT gfc_ora_tab_columns PRIMARY KEY(table_name, column_name)
,CONSTRAINT gfc_ora_tab_columns_idx2 UNIQUE(table_name, column_id)
);

--gfc_ps_indexdefn - expanded version of psindexdefn
CREATE
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_ps_indexdefn
(recname         VARCHAR2(15)    NOT NULL
,indexid         VARCHAR2(1)     NOT NULL
,subrecname      VARCHAR2(15)    NOT NULL
,subindexid      VARCHAR2(1)     NOT NULL
,platform_ora    NUMBER          NOT NULL
,custkeyorder    NUMBER          NOT NULL --6.9.2007
,uniqueflag      NUMBER          NOT NULL --6.9.2007    
,name_suffix	 VARCHAR2(20)	--16.6.2010
,CONSTRAINT gfc_ps_indexdefn PRIMARY KEY(recname, indexid)
,CONSTRAINT gfc_ps_indexdefn2 UNIQUE(subrecname, subindexid)
);

--gfc_ps_keydefn - expanded version of pskeydefn
CREATE
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_ps_keydefn
(recname         VARCHAR2(15)    NOT NULL
,indexid         VARCHAR2(1)     NOT NULL
,keyposn         NUMBER          NOT NULL
,fieldname       VARCHAR2(100)   NOT NULL --6.9.2007
,ascdesc	     NUMBER		     NOT NULL --1=ascending, 0=descending
,CONSTRAINT gfc_ps_keydefn PRIMARY KEY(recname,indexid,keyposn)
,CONSTRAINT gfc_ps_keydefn2 UNIQUE(recname,indexid,fieldname)
);

--to hold override parameters for function based indexes specified in partdata - 19.10.2007
CREATE
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_ps_idxddlparm
(recname         VARCHAR2(15)    NOT NULL
,indexid	     VARCHAR2(18)    NOT NULL
,parmname	     VARCHAR2(8)     NOT NULL
,parmvalue	     VARCHAR2(128)   NOT NULL
,CONSTRAINT gfc_ps_idxddlparm PRIMARY KEY(recname,indexid,parmname)
);

CREATE 
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_part_ranges
(part_id         VARCHAR2(8)     NOT NULL --ID of partitioning schema
,part_no         NUMBER          NOT NULL --sequence number of range
,part_name       VARCHAR2(30)    NOT NULL --this goes into the partition names
,part_value      VARCHAR2(100)   NOT NULL --range less than value
,tab_tablespace  VARCHAR2(30)
,idx_tablespace  VARCHAR2(30)
,tab_storage     VARCHAR2(100)
,idx_storage     VARCHAR2(100)
,arch_flag       VARCHAR2(1)     DEFAULT 'N' NOT NULL --N=not archived, A=archive D=delete/drop
 CONSTRAINT gfc_part_ranges_arch CHECK (arch_flag IN('A','D','N'))
,partial_index   VARCHAR2(1)     --26.11.2020 add partial index control at partition level
 CONSTRAINT gfc_part_ranges_partial CHECK (partial_index IN('Y','N')) --Y=on, N=off
,CONSTRAINT gfc_part_ranges PRIMARY KEY (part_id, part_no)
,CONSTRAINT gfc_part_ranges2 UNIQUE(part_id, part_name)
);

CREATE 
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_part_tables
(recname          VARCHAR2(30)   NOT NULL --peoplesoft record name
,organization     VARCHAR2(1)    DEFAULT 'T' 
 CONSTRAINT organization_check CHECK(organization IN('T','I')) --(T)able, (I)OT
,part_id          VARCHAR2(8) 	 NOT NULL --ID of partitioning strategy.  Many tables can share one 
,part_column      VARCHAR2(100)  NOT NULL --range partitioning column, or comma separated columns
,part_type        VARCHAR2(1)    NOT NULL --(R)ange, (L)ist, (H)ash, (I)nterval or (N)ot 
 CONSTRAINT tables_part_type CHECK (part_type IN('R','L','H','I','N'))
,interval_expr    VARCHAR2(100)  --Interval partitioning expression -- added 2.3.2015
,subpart_id       VARCHAR2(8)    --added 10.5.2021
,subpart_type     VARCHAR2(1)    DEFAULT 'N' --(L)ist or (H)ash only 
 CONSTRAINT tables_subpart_type CHECK (subpart_type IN('R','L','H','N')) --20.10.2014 added range subpartitioning
,subpart_column   VARCHAR2(100) 		 --sub partitioning column
,hash_partitions  NUMBER DEFAULT 0 NOT NULL --number of hash partitions
 CONSTRAINT tables_hash_partitions_pos CHECK(hash_partitions>=0)
,tab_tablespace   VARCHAR2(30)
,idx_tablespace   VARCHAR2(30)
,tab_storage      VARCHAR2(100)  DEFAULT 'PCTFREE **PCTFREE** PCTUSED **PCTUSED**' NOT NULL 
,idx_storage      VARCHAR2(100)  DEFAULT 'PCTFREE **PCTFREE**'                     NOT NULL 
,stats_type       VARCHAR2(1)    DEFAULT 'Y' 
 CONSTRAINT tables_stats_type CHECK (stats_type IN('Y','N','D'))
,sample_size      NUMBER 	 --analyze sample size : null means auto sample size
,method_opt       VARCHAR2(200)  --override statistics clause in gather_table_stats
,override_schema  VARCHAR2(30)
,src_table_name   VARCHAR2(30)   --8.6.2010 use this table as source for data during build - if null use same record
,criteria         VARCHAR2(1000) --crieria to be applied to partitioned table to filter rows during rebuild
,arch_flag        VARCHAR2(1)   DEFAULT 'N' NOT NULL --N=not archived, A=archive D=delete/drop
 CONSTRAINT gfc_part_tables_arch CHECK (arch_flag IN('A','D','N'))
,arch_schema      VARCHAR2(30)	 --schema of archive table (else db owner schema or override schema)
,arch_table_name  VARCHAR2(30)   --name of archive table (else taken from record definition)
,arch_recname	  VARCHAR2(15)	 --record name to use for definition of archive table (can be same in different schema)
,noarch_condition VARCHAR2(1000) --logical condition to specify rows of data that are not to be archived.
,CONSTRAINT gfc_part_tables PRIMARY KEY(recname)
,CONSTRAINT gfc_part_tables_columns CHECK(part_column IS NOT NULL OR subpart_column IS NOT NULL)
,CONSTRAINT gfc_part_tables_types CHECK(part_type != subpart_type OR subpart_type = 'N' OR (part_type = 'R' AND subpart_type = 'R')) --13.5.2021 add support for different range-range composite partitioning
);

--13.5.2021 --backward compatibility trigger to set subpart_id=part_id
CREATE OR REPLACE TRIGGER &&ownerid...gfc_part_tables_subpart_id 
BEFORE INSERT OR UPDATE 
ON &&ownerid..gfc_part_tables FOR EACH ROW
BEGIN
  IF :new.subpart_id IS NULL THEN
    :new.subpart_id := :new.part_id;
  END IF;
END;
/

CREATE
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_part_indexes
(recname         VARCHAR2(30) 	NOT NULL --peoplesoft record name
,indexid         VARCHAR2(1)	NOT NULL --peoplesoft index id
,part_id         VARCHAR2(8) 	NOT NULL --ID of partitioning strategy.  Many tables can share one 
,part_column     VARCHAR2(100) 	NOT NULL --range partitioning column, or comma separated columns
,part_type       VARCHAR2(1)    NOT NULL --(R)ange or (L)ist, (H)ash or (N)ot Partitioned
 CONSTRAINT index_part_type CHECK (part_type IN('R','L','H','N'))
,subpart_id      VARCHAR2(8) 	NOT NULL --ID of partitioning strategy.  Many tables can share one 
,subpart_type    VARCHAR2(1) DEFAULT 'N' --(L)ist or (H)ash only 
 CONSTRAINT index_subpart_type CHECK (subpart_type IN('R','L','H','N'))
,subpart_column  VARCHAR2(100) 		 --sub partitioning column
,hash_partitions NUMBER --number of hash partitions
 CONSTRAINT indexes_hash_partitions_pos CHECK(hash_partitions>=0)
,idx_tablespace  VARCHAR2(30)
,idx_storage     VARCHAR2(100)  DEFAULT 'PCTFREE **PCTFREE**' NOT NULL 
,override_schema VARCHAR2(30)
,name_suffix     VARCHAR2(20)	--16.6.2010
,partial_index   VARCHAR2(1)    --26.11.2020 add partial index control at index level
 CONSTRAINT gfc_part_indexes_partial CHECK (partial_index IN('Y','N')) --Y=partial else not
,CONSTRAINT gfc_part_indexes_tables PRIMARY KEY(recname, indexid)
);

--13.5.2021 --backward compatibility trigger to set subpart_id=part_id
CREATE OR REPLACE TRIGGER &&ownerid..gfc_part_indexes_subpart_id 
BEFORE INSERT OR UPDATE 
ON &&ownerid..gfc_part_indexes FOR EACH ROW
BEGIN
  IF :new.subpart_id IS NULL THEN
    :new.subpart_id := :new.part_id;
  END IF;
END;
/

CREATE 
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_part_lists 
(part_id         VARCHAR2(8)    NOT NULL --ID of partitioning schema
,part_no         NUMBER         NOT NULL --sequence number of range
,part_name       VARCHAR2(30)   NOT NULL --this goes into the partition names
,list_value      VARCHAR2(1000) NOT NULL --list value
,tab_tablespace  VARCHAR2(30)
,idx_tablespace  VARCHAR2(30)
,tab_storage     VARCHAR2(100)
,idx_storage     VARCHAR2(100)
,arch_flag       VARCHAR2(1)    DEFAULT 'N' NOT NULL --N=not archived, A=archive D=delete/drop
 CONSTRAINT gfc_part_lists_arch CHECK (arch_flag IN('A','D','N'))
,partial_index   VARCHAR2(1)    --26.11.2020 add partial index control at partition level
 CONSTRAINT gfc_part_lists_partial CHECK (partial_index IN('Y','N')) --Y=on, N=off
,CONSTRAINT gfc_part_lists PRIMARY KEY (part_id, part_no)
,CONSTRAINT gfc_part_lists2 UNIQUE(part_id, part_name)
);

--20.10.2014 replaces gfc_part_range_lists
CREATE 
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_part_subparts
(part_id         VARCHAR2(8)    NOT NULL --ID of partitioning schema
,part_name       VARCHAR2(30)   NOT NULL --this goes into the partition names
,subpart_id      VARCHAR2(8)    NOT NULL --ID of subpartitioning schema --added 13.5.2021
,subpart_name    VARCHAR2(30)   NOT NULL --this goes into the subpartition names
,build           VARCHAR2(1)    DEFAULT 'Y' NOT NULL 
 CONSTRAINT gfc_part_range_lists_build CHECK (build IN('Y','N'))
,CONSTRAINT gfc_part_range_Lists PRIMARY KEY(part_id, part_name, subpart_name)
);

--13.5.2021 --backward compatibility trigger to set subpart_id
CREATE OR REPLACE TRIGGER &&ownerid..gfc_part_subparts_subpart_id 
BEFORE INSERT OR UPDATE 
ON &&ownerid..gfc_part_subparts FOR EACH ROW
BEGIN
  IF :new.subpart_id IS NULL THEN
    :new.subpart_id := :new.part_id;
  END IF;
END;
/

--20.10.2014 create for backward compatibility
CREATE OR REPLACE VIEW &&ownerid..gfc_part_range_lists AS
SELECT part_id
,      part_name range_name
,      subpart_name list_name
,      build
FROM   gfc_part_subparts;

CREATE 
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_temp_tables
(recname         VARCHAR2(30) 	NOT NULL --peoplesoft record name
,tab_tablespace  VARCHAR2(30)
,CONSTRAINT gfc_temp_tables PRIMARY KEY(recname)
);

--gfc_ddl_script is a table used to store the lines of the script that is generated
CREATE
--GLOBAL TEMPORARY
TABLE &&ownerid..gfc_ddl_script
(type           NUMBER          NOT NULL 
,lineno         NUMBER          NOT NULL     
,line           VARCHAR2(4000)
);

CREATE OR REPLACE VIEW gfc_ps_alt_ind_cols AS
SELECT   c.recname
,        LTRIM(TO_CHAR(RANK() over (PARTITION BY c.recname 
                                        ORDER BY c.fieldnum)-1,'9')) indexid
,        c.subrecname
,        LTRIM(TO_CHAR(RANK() over (PARTITION BY c.recname, c.subrecname 
                                        ORDER BY c.fieldnum)-1,'9')) subindexid
,        c.fieldname
,        1-MOD(FLOOR(useedit/64),2) ascdesc
FROM     gfc_ps_tab_columns c
WHERE    MOD(FLOOR(useedit/16),2) = 1
--AND    RECNAME= 'JOB'
;

rem 11.2.2003 - view corrected to handled user indexes
CREATE OR REPLACE VIEW gfc_ps_keydefn_vw AS
SELECT  j.recname, j.indexid
,       RANK() OVER (PARTITION BY j.recname, j.indexid
                        ORDER BY DECODE(j.custkeyorder,1,k.keyposn,c.fieldnum)) as keyposn --6.9.2007
,       k.fieldname
,       c.fieldnum
,       RANK() OVER (PARTITION BY j.recname, j.indexid ORDER BY c.fieldnum) as fieldposn
,       k.ascdesc
FROM    gfc_ps_indexdefn j
,       gfc_ps_tab_columns c --6.9.2007 removed psindexdefn
,       pskeydefn k
WHERE   j.indexid = '_'
AND     c.recname = j.recname
AND     k.recname = c.subrecname
AND     k.indexid = j.subindexid
AND     k.fieldname = c.fieldname
UNION ALL
SELECT  j.recname, j.indexid
,       RANK() OVER (PARTITION BY j.recname, j.indexid ORDER BY k.keyposn) as keyposn
,       k.fieldname
,       c.fieldnum
,       RANK() OVER (PARTITION BY j.recname, j.indexid ORDER BY c.fieldnum) as fieldposn
,       k.ascdesc
FROM    gfc_ps_indexdefn j
,       gfc_ps_tab_columns c --6.9.2007 removed psindexdefn
,       pskeydefn k
WHERE   j.indexid BETWEEN 'A' AND 'Z'
AND     c.recname = j.recname
AND     k.recname = c.recname
--AND     k.recname = c.subrecname???qwert
AND     k.indexid = j.subindexid
AND     k.fieldname = c.fieldname
;

-----------------------------------------------------------------------------------------------------------
GRANT SELECT ON gfc_part_tables TO public;
GRANT SELECT ON gfc_part_ranges TO public;
-----------------------------------------------------------------------------------------------------------
--now build the context
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE CONTEXT gfc_pspart
USING gfc_pspart ACCESSED GLOBALLY
/
spool off
set echo off
