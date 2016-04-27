REM gp-partdata.sql

--------------------------------------------------------------------------------------------------------------
--insert data to describe temporary tables
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--insert data to specify the tables to be partitioned
--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_tables
WHERE part_id IN('PPM','PPMTRANS','PPMEVENT')
/

INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'PPMTRANS'
,	'PM_TRANS_DEFN_ID', 'L'
FROM    psrecdefn r
WHERE   r.recname IN(	'PSPMTRANSHIST'
                        ) 
/
INSERT INTO gfc_part_tables
(recname, part_id, part_column, part_type) 
SELECT  r.recname, 'PPMEVENT'
,	'PM_EVENT_DEFN_ID', 'L'
FROM    psrecdefn r
WHERE   r.recname IN(	'PSPMEVENTHIST'
                        ) 
/

UPDATE 	gfc_part_tables x
SET	x.tab_tablespace = (SELECT y.ddlspacename FROM psrectblspc y WHERE y.recname = x.recname) /*default PSFT tablespace*/
,	x.idx_tablespace = 'PSINDEX' /*default PSFT tablespace*/
,	x.tab_storage = 'PCTUSED 90 PCTFREE 0'
,	x.idx_storage = 'PCTFREE 0'
WHERE	x.part_id = 'PPM'
/

--------------------------------------------------------------------------------------------------------------
--describe indexes that are not to be locally partitions
--------------------------------------------------------------------------------------------------------------
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('PSPMTRANSHIST','_','PPM','N',' ')
/
INSERT INTO gfc_part_indexes (recname, indexid, part_id, part_type, part_column)
VALUES ('PSPMEVENTHIST','_','PPM','N',' ')
/
--------------------------------------------------------------------------------------------------------------
--insert data to specify range partitioning strategy
--------------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_ranges
WHERE part_id = 'PPM'
/
-----------------------------------------------------------------------------------------------------------
--insert data to list partitions
-----------------------------------------------------------------------------------------------------------
DELETE FROM gfc_part_lists
WHERE part_id = 'PPM'
/

INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMTRANS', 101, 'TRANS101', '101');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMTRANS', 108, 'TRANS108', '108');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMTRANS', 109, 'TRANS109', '109');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMTRANS', 113, 'TRANS113', '113');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMTRANS', 115, 'TRANS412', '115');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMTRANS', 116, 'TRANS116', '116');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMTRANS', 400, 'TRANS400', '400');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMTRANS', 401, 'TRANS401', '401');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMTRANS', 412, 'TRANS412', '412');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMTRANS', 999, 'OTHERS'  , 'DEFAULT');

INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMEVENT', 150, 'EVENT150', '150');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMEVENT', 151, 'EVENT151', '151');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMEVENT', 152, 'EVENT152', '152');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMEVENT', 153, 'EVENT153', '153');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMEVENT', 200, 'EVENT200', '200');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMEVENT', 300, 'EVENT300', '300');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMEVENT', 301, 'EVENT301', '301');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMEVENT', 302, 'EVENT302', '302');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMEVENT', 351, 'EVENT351', '351');
INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPMEVENT', 999, 'OTHERS', 'DEFAULT');

INSERT INTO gfc_part_lists (part_id, part_no, part_name, list_value) VALUES ('PPM', 999, 'OTHERS', 'DEFAULT');

DECLARE 
 l_last_dbname VARCHAR2(8) := '';
 l_list_value VARCHAR2(32767) := '';
 l_part_no INTEGER := 0;

 CURSOR c_agents IS 
 SELECT s.dbname, a.pm_agentid
 FROM pspmagent a, pspmsysdefn s
 WHERE a.pm_systemid = s.pm_systemid
 ORDER BY s.dbname, a.pm_agentid
 ;
 p_agents c_agents%ROWTYPE;

BEGIN
 OPEN c_agents;
 LOOP

  FETCH c_agents INTO p_agents;

  IF l_last_dbname IS NOT NULL AND (l_last_dbname != p_agents.dbname OR c_agents%NOTFOUND) THEN
   l_part_no := l_part_no + 1;
   INSERT INTO gfc_part_lists
   (part_id, part_no, part_name, list_value)
   VALUES('PPM', l_part_no, l_last_dbname, l_list_value);
   l_list_value := '';
  END IF;

  EXIT WHEN c_agents%NOTFOUND;

  IF l_list_value IS NULL THEN 
    l_list_value := p_agents.pm_agentid;
  ELSE
    l_list_value := l_list_value||','||p_agents.pm_agentid;
  END IF;

  l_last_dbname := p_agents.dbname;

 END LOOP;
 CLOSE c_agents;

END;
/


UPDATE 	gfc_part_lists
SET 	tab_tablespace = 'PPM'||part_name||'TAB'
,   	idx_tablespace = 'PPM'||part_name||'IDX'
--,	tab_storage = '/*TAB STORAGE*/'
--,	idx_storage = '/*IDX STORAGE*/'
WHERE 	part_id = 'PPM'
AND 1=2
/

-----------------------------------------------------------------------------------------------------------
--mapping between ranges and lists
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
--delete range/list combinations that are not needed
-----------------------------------------------------------------------------------------------------------
