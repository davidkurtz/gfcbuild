rem gfcbuildpkgbody.sql
rem (c) Go-Faster Consultancy Ltd. www.go-faster.co.uk
rem ***Version History moved to procedure history

set echo on
spool gfcbuildpkgbody

-----------------------------------------------------------------------------------------------------------
--SOurce Code
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY gfc_pspart AS

        l_lineno INTEGER := 0;
        l_dbname VARCHAR2(8 CHAR);                       /*PeopleSoft database name*/
        l_oraver NUMBER;                                 /*oracle rdbms version*/
--      l_parallel_max_servers INTEGER;                  /*value of oracle initialisation parameter*/
        l_ptver  VARCHAR2(20 CHAR);                      /*peopletools version*/
        l_schema1 VARCHAR2(30 CHAR);                     /*schema name*/
        l_schema2 VARCHAR2(31 CHAR);                     /*schema name with separator*/
        l_debug BOOLEAN := FALSE;                        /*enable debug messages*/
        l_unicode_enabled INTEGER := 0;                  /*unicode database*/
        l_database_options INTEGER := 0;                 /*database options--6.9.2007*/
	l_lf VARCHAR2(1 CHAR) := CHR(10);                /*line feed character*/
        l_drop_purge_suffix VARCHAR2(10 CHAR);           /*use explicit purge clause on drop table - 14.2.2008*/
	l_sys_context VARCHAR2(10 CHAR) := 'GFC_PSPART'; /*name of system context*/

        /*the following variables may need to be set by the user*/

        l_chardef VARCHAR2(1 CHAR);             /*permit VARCHAR2 character definition*/
        l_logging VARCHAR2(1 CHAR);             /*set to Y to generate build script that logs*/
        l_parallel VARCHAR2(1 CHAR);            /*set to true to enable parallel index build*/
        l_roles VARCHAR2(1 CHAR);               /*should roles be granted*/
        l_scriptid VARCHAR2(8 CHAR);            /*id string in script and project names*/
        l_update_all VARCHAR2(30 CHAR);         /*name of role than can update PS tables*/
        l_read_all VARCHAR2(30 CHAR);           /*name of role than can read PS tables*/
        l_drop_index VARCHAR2(1 CHAR);          /*if true drops index on exiting table, else alters name of index to old*/
        l_pause VARCHAR2(1 CHAR);               /*if true add pause commands to build script*/
        l_explicit_schema VARCHAR2(1 CHAR);     /*all objects schema explicitly named*/
        l_block_sample VARCHAR2(1 CHAR);        /*use block sampling for statistics*/
        l_build_stats VARCHAR2(1 CHAR);         /*if true analyzes tables as it builds them*/
        l_deletetempstats VARCHAR2(1 CHAR);     /*if true delete and in ORacle 10 also lock stats on temp tables*/
        l_longtoclob VARCHAR2(1 CHAR);          /*if Y then partition and convert longs to clobs if override schema*/
        l_ddltrigger VARCHAR2(30 CHAR);         /*name of trigger (T_LOCK) that locks DDL on partitioned objects*/
        l_drop_purge VARCHAR2(1 CHAR);          /*use explicit purge clause on drop table - 14.2.2008*/
        l_forcebuild VARCHAR2(1 CHAR);          /*if true then force build even if structure matches DB*/
        l_desc_index VARCHAR2(1 CHAR);          /*Y to force desc index, N to disable, null to follow PS default*/

        l_noalterprefix VARCHAR2(8 CHAR) := ''; /*if true then don't generate alters, just for debug--6.9.2007*/

--version history --need an array
        PROCEDURE history IS
	BEGIN
		banner;
		sys.dbms_output.put_line('03.12.2002 - improved subrecord handling');
		sys.dbms_output.put_line('11.02.2003 - correction to column sequencing on user'); --some table names change to bring script in line with colaudit
-- 19.06.2003 - correction to MOD() that processes useedit flags
		sys.dbms_output.put_line('09.07.2003 - nologging facility added');
		sys.dbms_output.put_line('10.07.2003 - oracle version detection added to control fix for 8.1.7.2 bug');
		sys.dbms_output.put_line('05.09.2003 - corrected handling of peoplesoft long character columns');
-- 18.09.2003 - changed statistics script to only build histograms on indexes columns, and default number of buckets
		sys.dbms_output.put_line('18.09.2003 - added trigger to prevent updates on tables whilst being rebuilt');
		sys.dbms_output.put_line('09.10.2003 - tables and indexes set to logging enabled and parallel disabled, parallel control variable added');
--  9.10.2003 - grants to PS_READ_ALL and PS_UPDATE_ALL roles added to vanilla version
		sys.dbms_output.put_line('28.10.2003 - supress partitioning for tables with long columns'); --additional partitioned table
		sys.dbms_output.put_line('29.10.2003 - pt version detection to enable new PT8.4 features');
		sys.dbms_output.put_line('04.11.2003 - oracle 9i features, role name control');
		sys.dbms_output.put_line('17.11.2003 - rename/drop index');
		sys.dbms_output.put_line('07.01.2004 - explicit schema name, script control');
		sys.dbms_output.put_line('22.03.2004 - oracle 9 varchars in characters');
		sys.dbms_output.put_line('27.09.2004 - suppress disabled index build, but force disabled index to drop');
		sys.dbms_output.put_line('18.04.2005 - support for PeopleSoft temporary tables'); --backported from gtbuild.sql
-- 22.06.2005 - cloned from GPBUILD.sql
-- 25.07.2006 - applied GPBUILD corrections, and remove chartfield fudge
		sys.dbms_output.put_line('05.12.2006 - added handling for hash-partitioned only tables');
		sys.dbms_output.put_line('13.12.2006 - remove partitioning column check');
		sys.dbms_output.put_line('06.09.2007 - enhancement for PeopleTools 8.48, support partitioned function based indexes');
		sys.dbms_output.put_line('08.11.2007 - support for range-list composite partitioning');
		sys.dbms_output.put_line('14.02.2008 - selective list partition -v- range build, drop purge, all keys on subrecords');
		sys.dbms_output.put_line('28.08.2008 - conversion to package procedure');
		sys.dbms_output.put_line('16.09.2008 - DDL moved to gfcbuildtab');
		sys.dbms_output.put_line('16.12.2008 - Single table build options can be combined');
		sys.dbms_output.put_line('23.01.2009 - Partitioning columns in unique indexes must not be descending');
		sys.dbms_output.put_line('01.04.2009 - Corrections to add partition scripts');
		sys.dbms_output.put_line('18.04.2009 - Override Default Application Designer Project Name');
		sys.dbms_output.put_line('03.06.2009 - Extended check descending partitioning columns in unique indexes to subrecords');

	END;

--read defaults from contexts
        PROCEDURE reset_variables IS
        BEGIN
	        l_chardef := 'N';                /*permit VARCHAR2 character definition*/
        	l_logging := 'N';                /*set to Y to generate build script that logs*/
	        l_parallel := 'Y';               /*set to true to enable parallel index build*/
        	l_roles := 'N';                  /*should roles be granted*/
	        l_scriptid := 'GFCBUILD';        /*id string in script and project names*/
        	l_update_all := 'PS_UPDATE_ALL'; /*name of role than can update PS tables*/
	        l_read_all := 'PS_READ_ALL';     /*name of role than can read PS tables*/
        	l_drop_index := 'Y';             /*if true drops index on exiting table, else alters name of index to old*/
	        l_pause := 'N';                  /*if true add pause commands to build script*/
        	l_explicit_schema := 'Y';        /*all objects schema explicitly named*/
	        l_block_sample := 'Y';           /*use block sampling for statistics*/
        	l_build_stats := 'N';            /*if true analyzes tables as it builds them*/
	        l_deletetempstats := 'Y';        /*if true delete and in ORacle 10 also lock stats on temp tables*/
        	l_longtoclob := 'N';             /*if Y then partition and convert longs to clobs if override schema*/
	        l_ddltrigger := '';              /*name of trigger (T_LOCK) that locks DDL on partitioned objects*/
        	l_drop_purge := 'Y';             /*use explicit purge clause on drop table - 14.2.2008*/
	        l_noalterprefix := '';           /*if true then don't generate alters, just for debug--6.9.2007*/
        	l_forcebuild := 'Y';             /*if true then force build even if structure matches DB*/
	        l_desc_index := 'Y';             /*Y to force desc index, N to disable, null to follow PS default*/
	END reset_variables;

--read defaults from contexts
        PROCEDURE read_context IS
        BEGIN
		reset_variables;
		l_chardef          := NVL(SYS_CONTEXT(l_sys_context,'chardef'        ),l_chardef);
		l_logging          := NVL(SYS_CONTEXT(l_sys_context,'logging'        ),l_logging);
		l_parallel         := NVL(SYS_CONTEXT(l_sys_context,'parallel'       ),l_parallel);
		l_roles            := NVL(SYS_CONTEXT(l_sys_context,'roles'          ),l_roles);
		l_scriptid         := NVL(SYS_CONTEXT(l_sys_context,'scriptid'       ),l_scriptid);
		l_update_all       := NVL(SYS_CONTEXT(l_sys_context,'update_all'     ),l_update_all);
		l_read_all         := NVL(SYS_CONTEXT(l_sys_context,'read_all'       ),l_read_all);
		l_drop_index       := NVL(SYS_CONTEXT(l_sys_context,'drop_index'     ),l_drop_index);
		l_pause            := NVL(SYS_CONTEXT(l_sys_context,'pause'          ),l_pause);
		l_explicit_schema  := NVL(SYS_CONTEXT(l_sys_context,'explicit_schema'),l_explicit_schema);
		l_block_sample     := NVL(SYS_CONTEXT(l_sys_context,'block_sample'   ),l_block_sample);
		l_build_stats      := NVL(SYS_CONTEXT(l_sys_context,'build_stats'    ),l_build_stats);
		l_deletetempstats  := NVL(SYS_CONTEXT(l_sys_context,'deletetempstats'),l_deletetempstats);
		l_longtoclob       := NVL(SYS_CONTEXT(l_sys_context,'longtoclob'     ),l_longtoclob);
		l_ddltrigger       := NVL(SYS_CONTEXT(l_sys_context,'ddltrigger'     ),l_ddltrigger);
		l_drop_purge       := NVL(SYS_CONTEXT(l_sys_context,'drop_purge'     ),l_drop_purge);
--		l_noalterprefix    := NVL(SYS_CONTEXT(l_sys_context,'noalterprefix'  ),l_noalterprefix);
		l_forcebuild       := NVL(SYS_CONTEXT(l_sys_context,'forcebuild'     ),l_forcebuild);
		l_desc_index       := NVL(SYS_CONTEXT(l_sys_context,'desc_index'     ),l_desc_index);		
	END;

--write defaults to context
        PROCEDURE write_context IS
        BEGIN
		dbms_session.set_context(l_sys_context,'chardef'          ,l_chardef);
		dbms_session.set_context(l_sys_context,'logging'          ,l_logging);
		dbms_session.set_context(l_sys_context,'parallel'         ,l_parallel);
		dbms_session.set_context(l_sys_context,'roles'            ,l_roles);
		dbms_session.set_context(l_sys_context,'scriptid'         ,l_scriptid);
		dbms_session.set_context(l_sys_context,'update_all'       ,l_update_all);
		dbms_session.set_context(l_sys_context,'read_all'         ,l_read_all);
		dbms_session.set_context(l_sys_context,'drop_index'       ,l_drop_index);
		dbms_session.set_context(l_sys_context,'pause'            ,l_pause);
		dbms_session.set_context(l_sys_context,'explicit_schema'  ,l_explicit_schema);
		dbms_session.set_context(l_sys_context,'block_sample'     ,l_block_sample);
		dbms_session.set_context(l_sys_context,'build_stats'      ,l_build_stats);
		dbms_session.set_context(l_sys_context,'deletetempstats'  ,l_deletetempstats);
		dbms_session.set_context(l_sys_context,'longtoclob'       ,l_longtoclob);
		dbms_session.set_context(l_sys_context,'ddltrigger'       ,l_ddltrigger);
		dbms_session.set_context(l_sys_context,'drop_purge'       ,l_drop_purge);
--		dbms_session.set_context(l_sys_context,'noalterprefix'    ,l_noalterprefix);
		dbms_session.set_context(l_sys_context,'forcebuild'       ,l_forcebuild);
		dbms_session.set_context(l_sys_context,'desc_index'       ,l_desc_index);
	END;

--get name of PeopleSoft database
        PROCEDURE dbname IS
        BEGIN
                SELECT  UPPER(ownerid), MAX(dbname)
                INTO    l_schema1,l_dbname
                FROM    ps.psdbowner
                WHERE   UPPER(ownerid) = user
                GROUP BY UPPER(ownerid)
                ;

                IF l_explicit_schema = 'Y' THEN
                        l_schema2 := LOWER(l_schema1)||'.';
                ELSE
                        l_schema2 := '';
                END IF;
        END;

--get version of Oracle
        PROCEDURE oraver IS
        BEGIN
                SELECT  TO_NUMBER(SUBSTR(banner,1,INSTR(banner,'.',1,2)-1)) version
                INTO    l_oraver
                FROM    (
                        SELECT  SUBSTR(banner,6) banner
                        FROM    v$version
                        WHERE   banner LIKE 'CORE%'
                        )
                ;

		IF l_oraver < 10 THEN
			l_drop_purge := 'N';
			l_drop_purge_suffix := '';
		ELSE	
			l_drop_purge_suffix := ' PURGE';
		END IF;

--              SELECT  TO_NUMBER(value)
--              INTO    l_parallel_max_servers
--              FROM    v$parameter
--              WHERE   name = 'parallel_max_servers'
--              ;

        END;

--get version of PeopleTools
        PROCEDURE ptver IS
        BEGIN
                --6.9.2007 added PT8.48 unicode/long logic
                SELECT  toolsrel, unicode_enabled
                INTO    l_ptver, l_unicode_enabled
                FROM    psstatus
                ;

		IF l_ptver < '8.15' AND l_desc_index IS NULL THEN
			l_desc_index := 'N';
		END IF;

		IF l_ptver >= '8.48' THEN
			l_logging := 'N';
  			EXECUTE IMMEDIATE 'SELECT database_options FROM psstatus' 
					INTO l_database_options;

			IF l_database_options = 2 THEN
				l_unicode_enabled := 0; /*6.9.2007:character semantics*/
				l_longtoclob := 'Y'; /*use clobs*/              
			END IF;

			IF l_desc_index IS NULL THEN
				l_desc_index := 'Y';
			END IF;
		END IF;
        END;

--display debug code
        PROCEDURE debug(p_message VARCHAR2) IS
        BEGIN
                IF l_debug THEN
                        sys.dbms_output.put_line(p_message);
                END IF;
        END;

--make a project of the interested tables
        PROCEDURE gfc_project(p_projectname VARCHAR2 DEFAULT '') IS
                l_version INTEGER;
                l_version2 INTEGER;
                l_projectname VARCHAR2(20 CHAR) := UPPER(l_scriptid);
                l_sql VARCHAR2(32767);
        BEGIN
		IF p_projectname IS NOT NULL THEN
			l_projectname := p_projectname;
		END IF;

                UPDATE  PSLOCK
                SET     version = version + 1
                WHERE   objecttypename IN ('PJM','SYS');

                UPDATE  PSVERSION
                SET     version = version + 1
                WHERE   objecttypename IN ('PJM','SYS');

                SELECT  version
                INTO    l_version
                FROM    PSLOCK
                WHERE   objecttypename IN ('PJM') FOR UPDATE OF version;

                SELECT  version
                INTO    l_version2
                FROM    psversion
                WHERE   objecttypename IN ('PJM') FOR UPDATE OF version;

		l_version := GREATEST(l_version,l_version2);
		l_version2 := l_version;

                DELETE FROM PSPROJECTDEL        WHERE PROJECTNAME = l_projectname;
                DELETE FROM PSPROJECTITEM       WHERE PROJECTNAME = l_projectname;
                DELETE FROM PSPROJDEFNLANG      WHERE PROJECTNAME = l_projectname;
                DELETE FROM PSPROJECTSEC        WHERE PROJECTNAME = l_projectname;
                DELETE FROM PSPROJECTINC        WHERE PROJECTNAME = l_projectname;
                DELETE FROM PSPROJECTDEP        WHERE PROJECTNAME = l_projectname;
                DELETE FROM PSPROJECTDEFN       WHERE PROJECTNAME = l_projectname;

                INSERT INTO PSPROJECTITEM
                (       PROJECTNAME, OBJECTTYPE, OBJECTID1, OBJECTVALUE1,
                        OBJECTID2, OBJECTVALUE2, OBJECTID3, OBJECTVALUE3,
                        OBJECTID4, OBJECTVALUE4, NODETYPE, SOURCESTATUS,
                        TARGETSTATUS, UPGRADEACTION, TAKEACTION, COPYDONE)
                SELECT  l_projectname,0,1,r.recname,
                        0,' ',0,' ',
                        0,' ',0,0,
                        0,2,1,0
                FROM    psrecdefn r,
                        gfc_ps_tables e
                WHERE   e.recname = r.recname
                ;

		INSERT INTO psprojectitem
		(	PROJECTNAME, OBJECTTYPE, OBJECTID1, OBJECTVALUE1, 
			OBJECTID2, OBJECTVALUE2, OBJECTID3, OBJECTVALUE3, 
			OBJECTID4, OBJECTVALUE4, NODETYPE, SOURCESTATUS, 
			TARGETSTATUS, UPGRADEACTION, TAKEACTION, COPYDONE)
		SELECT	DISTINCT l_projectname,1,1,k.recname,
			24,k.indexid,0,' ',
			0,' ',0,0,
			0,0,1,0
                FROM    gfc_ps_tables e,
			psrecfielddb f,
			pskeydefn k
                WHERE   f.recname = e.recname
		AND	k.recname = f.recname_parent
		AND	k.fieldname = f.fieldname
		;

                l_sql := 'INSERT INTO '||l_schema2||'PSPROJECTDEFN ('
                                ||'VERSION, PROJECTNAME, TGTSERVERNAME, TGTDBNAME, TGTOPRID, '
                                ||'TGTOPRACCT, REPORTFILTER, TGTORIENTATION, COMPARETYPE, KEEPTGT, '
                                ||'COMMITLIMIT, MAINTPROJ, COMPRELEASE, COMPRELDTTM,';
                IF l_ptver >= '8.4' THEN /*new column in pt8.4*/
                        l_sql := l_sql || 'OBJECTOWNERID, ';
                END IF;
                l_sql := l_sql  ||'LASTUPDDTTM, LASTUPDOPRID, PROJECTDESCR, '
		                ||'RELEASELABEL, RELEASEDTTM, DESCRLONG) '
                                ||'VALUES ('||l_version||','''||l_projectname||''','' '','' '','' '','
                                ||''' '',16232832,0,1,3,'
                                ||'50,0,'' '', null,' ;
                IF l_ptver >= '8.4' THEN /*new column in pt8.4*/
                        l_sql := l_sql || ''' '',' ;
                END IF;
                l_sql := l_sql  ||'sysdate,''PS'',''Partitioned + Global Temp Tabs'', '' '', NULL, '
		                ||'''Partitioned and Global Temporary Tables generated by gfcbuild script on '
		                ||TO_CHAR(SYSDATE,'dd.mm.yyyy')||''' )';

                EXECUTE IMMEDIATE l_sql;
        END;

--populate list of tables
--19.6.2003 list of tables reorganised
        PROCEDURE gfc_ps_tables
	(p_recname VARCHAR2 DEFAULT ''
	,p_rectype VARCHAR2 DEFAULT 'A'
	) IS
        BEGIN

                INSERT INTO gfc_ps_tables
                (recname, rectype, table_name, table_type, temptblinstances, override_schema)
                SELECT  r.recname, r.rectype,
                        DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name,
                        'P' table_type
		,	0 temptblinstances
		,	p.override_schema
                FROM    psrecdefn r
                ,       gfc_part_tables p
                WHERE   r.rectype = 0
                AND     r.recname = p.recname
		AND	(p.recname LIKE p_recname OR p_recname IS NULL)
		AND	(p_rectype IN('A','P') OR p_rectype IS NULL)
                UNION ALL
                SELECT  r.recname, r.rectype
                ,       DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
                ,       'T' table_type
		,	0 temptblinstances
		,	'' override_schema
                FROM    psrecdefn r
                ,	gfc_temp_tables t
                WHERE   r.recname = t.recname
                AND	(	r.rectype = 0
                        OR	(	r.rectype = 7
		                AND     r.recname IN(
						SELECT 	a.recname
						FROM	psaeappldefn b
						, 	psaeappltemptbl a
						WHERE 	a.ae_applid = b.ae_applid
						AND	b.ae_disable_restart = 'Y'
						MINUS
						SELECT 	a.recname
						FROM	psaeappldefn b
						, 	psaeappltemptbl a
						WHERE 	a.ae_applid = b.ae_applid
						AND	b.ae_disable_restart = 'N'
					)
                               )
		        )
		AND	(t.recname LIKE p_recname OR p_recname IS NULL)
		AND	(p_rectype IN('A','T') OR p_rectype IS NULL)
                ;

                UPDATE gfc_ps_tables g
                SET    temptblinstances = (
                       SELECT NVL(o.temptblinstances+t.temptblinstances,0) temptblinstances
                       FROM   psoptions o, pstemptblcntvw t
                       WHERE  t.recname(+) = g.recname)
                WHERE  g.rectype = 7
                ;

--18.12.2008 -- added criteria on p_recname to prevent duplicate inserts
                INSERT INTO gfc_ora_tab_columns
                        (table_name, column_name, column_id)
                SELECT c.table_name, c.column_name, c.column_id
                FROM   user_tab_columns c
                ,      gfc_ps_tables g
                WHERE  g.table_name = c.table_name
		AND    (g.recname LIKE p_recname OR p_recname IS NULL) 
                ;

        END;

--populate table of columns
        PROCEDURE gfc_ps_tab_columns 
	(p_recname VARCHAR2 DEFAULT ''
	) IS
        BEGIN
                INSERT INTO gfc_ps_tab_columns
                (       recname, fieldname, useedit, fieldnum, subrecname)
                SELECT  r.recname, f.fieldname, f.useedit, f.fieldnum, r.recname
                FROM    gfc_ps_tables r
                ,       psrecfield f
                WHERE   r.recname = f.recname
		AND     (r.recname LIKE p_recname OR p_recname IS NULL) 
                ;
        END;

--populate table of indexes - dmk
        PROCEDURE gfc_ps_indexdefn 
	(p_recname VARCHAR2 DEFAULT ''
	) IS
        BEGIN
                INSERT INTO gfc_ps_indexdefn
                (       recname, indexid, subrecname, subindexid, platform_ora, custkeyorder, uniqueflag)
                SELECT  DISTINCT x.recname, x.indexid
                ,       x.subrecname
		, 	x.subindexid
		, 	x.platform_ora
		, 	x.custkeyorder
		, 	NVL(j.uniqueflag,x.uniqueflag) uniqueflag --6.9.2007 added psindexdefn psindexdefn
                FROM    (
			SELECT	c.recname, i.indexid
			,	c.recname subrecname
			, 	i.indexid subindexid
			, 	i.platform_ora
			, 	i.custkeyorder
			,	i.uniqueflag
			FROM	gfc_ps_tab_columns c
                	,       psindexdefn i
	                WHERE   i.recname = c.subrecname
			AND	(c.recname LIKE p_recname OR p_recname IS NULL) 
			) x
                	LEFT OUTER JOIN psindexdefn j
			ON 	j.recname = x.recname
			AND 	j.indexid = x.indexid
		UNION
                SELECT  c.recname, i.indexid --indexes defined on record -- added in case all keys on subrecords
                ,       c.recname subrecname
		, 	i.indexid subindexid
		, 	i.platform_ora
		, 	i.custkeyorder
		,	i.uniqueflag
                FROM    gfc_ps_tab_columns c
                ,       psindexdefn i
	        WHERE   i.recname = c.recname
		AND	(c.recname LIKE p_recname OR p_recname IS NULL) 
                UNION
                SELECT  DISTINCT x.recname, x.indexid
                ,       x.subrecname, x.subindexid, i.platform_ora
		, 	i.custkeyorder, i.uniqueflag --6.9.2007 added psindexdefn fields
                FROM    gfc_ps_alt_ind_cols x
                ,       psindexdefn i
                WHERE   x.indexid BETWEEN '0' AND '9'
                AND     x.subindexid = i.indexid
                AND     x.subrecname = i.recname
		AND	(x.recname LIKE p_recname OR p_recname IS NULL) 
--		ORDER BY 1,2,3,4,5
                ;
        END;

--populate table of indexes - dmk
        PROCEDURE gfc_ps_keydefn  
	(p_recname VARCHAR2 DEFAULT ''
	) IS
        BEGIN
--insert unique and user indexes
                INSERT INTO gfc_ps_keydefn
                (       recname, indexid, keyposn, fieldname, ascdesc)
                SELECT  recname, indexid, keyposn, fieldname, ascdesc
                FROM    gfc_ps_keydefn_vw
		WHERE	(recname LIKE p_recname OR p_recname IS NULL) 
                ;

--insert first columns of alternate search key indexes
                INSERT INTO gfc_ps_keydefn
                (       recname, indexid, keyposn, fieldname, ascdesc)
                SELECT  c.recname, c.indexid, 1, c.fieldname, ascdesc
                FROM    gfc_ps_alt_ind_cols c
		WHERE	(c.recname LIKE p_recname OR p_recname IS NULL) 
                ;

--insert other coulmns of alternate search key indexes
                INSERT INTO gfc_ps_keydefn
                (       recname, indexid, keyposn, fieldname, ascdesc)
                SELECT  c.recname, c.indexid, k.fieldposn+1, k.fieldname, k.ascdesc
                FROM    gfc_ps_alt_ind_cols c
                ,       gfc_ps_keydefn_vw k
                where   k.recname = c.recname
                AND     k.indexid = '_'
		AND	(c.recname LIKE p_recname OR p_recname IS NULL) 
                ;
--30.6.2005-remove special processing columns
--                DELETE FROM gfc_ps_keydefn
--                WHERE fieldname IN('AFFILIATE_INTRA2','CHARTFIELD1','CHARTFIELD2','CHARTFIELD3')
--                ;

--correct column sequencing
                UPDATE  gfc_ps_keydefn k
                SET     k.keyposn =
                        (SELECT k1.keyposn
                        FROM    pskeydefn k1
                        WHERE   k1.recname = k.recname
                        AND     k1.indexid = k.indexid
                        AND     k1.fieldname = k.fieldname)
                WHERE   (k.recname,k.indexid) IN (
                        SELECT  j.recname, j.indexid
                        FROM    gfc_ps_indexdefn j  --6.9.2007 removed psindexdefn
                        WHERE   j.custkeyorder = 1
			AND	(j.recname LIKE p_recname OR p_recname IS NULL) 
			)
                ;

--remove descending key option on partition and subpartition columns 23.1.2009
--and corrected again for subrecords 3.6.2009
		UPDATE	gfc_ps_keydefn k
		SET 	k.ascdesc = 1 /*is now an ascending column*/
		WHERE 	k.ascdesc = 0 /*is a descending column*/
		AND EXISTS(
			SELECT 	'x'
			FROM 	psindexdefn i
			,	psrecfielddb f
			, 	gfc_part_tables p
			WHERE	k.recname = f.recname
			AND	f.recname_parent = i.recname
			AND 	i.indexid = k.indexid
			AND 	i.uniqueflag = 1 /*it's a unique index*/
			AND 	p.recname = k.recname
			AND	f.fieldname = k.fieldname
			AND 	k.fieldname IN (p.part_column,p.subpart_column)
			AND	ROWNUM <= 1
			)
		;

        END;

--recursively expand and subrecords in the list of columns
        PROCEDURE expand_sbr IS
                CURSOR c_sbr_col IS
	                SELECT *
        	        FROM   gfc_ps_tab_columns
                	WHERE  fieldname IN
                        	(SELECT recname
	                        FROM    psrecdefn
        	                WHERE   rectype = 3)
                ORDER BY recname, fieldnum
                ;

                p_sbr_col c_sbr_col%ROWTYPE;

                l_found_sbr INTEGER :=0; /*number of subrecords found in loop*/
                l_sbr_cols  INTEGER;     /*number of columns in the subrecord*/
                l_last_recname VARCHAR2(18 CHAR) := ''; /*name oflast record processed*/
                l_fieldnum_adj INTEGER := 0; /*field number offset when expanding subrecords*/
        BEGIN
                LOOP
                        l_found_sbr := 0;
                        OPEN c_sbr_col;
                        LOOP
                                FETCH c_sbr_col INTO p_sbr_col;
                                EXIT WHEN c_sbr_col%NOTFOUND;

                                debug(l_last_recname||'/'||p_sbr_col.recname||'/'||l_fieldnum_adj);
                                IF (l_last_recname != p_sbr_col.recname OR l_last_recname IS NULL) THEN
                                        l_fieldnum_adj := 0;
                                        l_last_recname := p_sbr_col.recname;
                                END IF;

                                debug(p_sbr_col.recname||'/'|| p_sbr_col.fieldnum||'/'|| 
                                      p_sbr_col.fieldname||'/'|| l_fieldnum_adj);

                                l_found_sbr := l_found_sbr +1;

                                SELECT COUNT(*)
                                INTO   l_sbr_cols
                                FROM   psrecfield f
                                WHERE  f.recname = p_sbr_col.fieldname;

                                DELETE FROM gfc_ps_tab_columns
                                WHERE  recname = p_sbr_col.recname
                                AND    fieldname = p_sbr_col.fieldname;

                                UPDATE gfc_ps_tab_columns
                                SET    fieldnum = fieldnum + l_sbr_cols - 1
                                WHERE  recname = p_sbr_col.recname
                                AND    fieldnum > p_sbr_col.fieldnum + l_fieldnum_adj;

                                INSERT INTO gfc_ps_tab_columns
                                (recname, fieldname, useedit, fieldnum, subrecname)
                                SELECT  p_sbr_col.recname, f.fieldname, f.useedit
				, 	f.fieldnum + p_sbr_col.fieldnum + l_fieldnum_adj - 1, f.recname
                                FROM    psrecfield f
                                WHERE   f.recname = p_sbr_col.fieldname;

                                l_fieldnum_adj := l_fieldnum_adj + l_sbr_cols -1;

                        END LOOP;
                        CLOSE c_sbr_col;
                        debug('Found: '||l_found_sbr||' sub-records');
                        EXIT WHEN l_found_sbr = 0;
                END LOOP;
        END;

--shuffle long columns to bottom of record
        PROCEDURE shuffle_long IS
                CURSOR c_cols IS
                SELECT  c.recname, c.fieldname, c.fieldnum
                FROM    gfc_ps_tab_columns c
                ,       psdbfield d
                ,       psrecdefn r
                WHERE   c.fieldname = d.fieldname
                AND     (       (       d.fieldtype = 1
                                AND     NOT d.length BETWEEN 1 AND 2000)
                        OR      d.fieldtype = 8)
                AND     r.recname = c.recname
                AND     r.rectype IN(0,7)
                AND EXISTS(
                        SELECT 'x'
                        FROM    gfc_ps_tab_columns c1
                        ,       psdbfield d1
                        WHERE   c1.recname = c.recname
                        AND     c1.fieldname = d1.fieldname
                        AND     c1.fieldnum > c.fieldnum
                        AND NOT (       (       d1.fieldtype = 1
                                        AND     NOT d1.length BETWEEN 1 AND 2000)
                                OR      d1.fieldtype = 8)
                        )
                ;
                p_cols c_cols%ROWTYPE;
                l_fieldcount INTEGER;
        BEGIN
                OPEN c_cols;
                LOOP
                        FETCH c_cols INTO p_cols;
                        EXIT WHEN c_cols%NOTFOUND;

                        SELECT  MAX(fieldnum)
                        INTO    l_fieldcount
                        FROM    gfc_ps_tab_columns
                        WHERE   recname = p_cols.recname;

                        UPDATE  gfc_ps_tab_columns
                        SET     fieldnum = DECODE(fieldnum,p_cols.fieldnum,l_fieldcount,fieldnum-1)
                        WHERE   recname = p_cols.recname
                        AND     fieldnum >= p_cols.fieldnum
                        ;

                END LOOP;
                CLOSE c_cols;
        END;

--generate the column definition clause for create table DDL
        FUNCTION col_def(p_recname VARCHAR2, p_fieldname VARCHAR2, p_longtoclob VARCHAR) RETURN VARCHAR2 IS
                l_col_def VARCHAR2(80 CHAR);

                CURSOR c_dbfield (p_fieldname VARCHAR2) IS
                SELECT  *
                FROM    psdbfield
                WHERE   fieldname = p_fieldname;
                p_dbfield c_dbfield%ROWTYPE;

                CURSOR c_gp_columns (p_recname VARCHAR2, p_fieldname VARCHAR2) IS
                SELECT  *
                FROM    gfc_ps_tab_columns
                WHERE   fieldname = p_fieldname
                AND     recname = p_recname;
                p_gp_columns c_gp_columns%ROWTYPE;

                l_nullable VARCHAR2(10 CHAR);
                l_datatype VARCHAR2(12 CHAR);
        BEGIN
                OPEN c_dbfield(p_fieldname);
                OPEN c_gp_columns(p_recname, p_fieldname);
                FETCH c_dbfield INTO p_dbfield;
                FETCH c_gp_columns INTO p_gp_columns;

                IF p_dbfield.fieldtype = 0 THEN
                        l_datatype := 'VARCHAR2';
                ELSIF p_dbfield.fieldtype IN(1,8,9) THEN
                        IF p_dbfield.length BETWEEN 1 AND 2000 THEN
                                l_datatype := 'VARCHAR2';
                        ELSE
                                IF p_longtoclob = 'Y' THEN 
                                        l_datatype := 'CLOB';
                                ELSIF l_ptver >= '8.4' THEN
                                        l_datatype := 'LONG VARCHAR';
                                ELSE
                                        l_datatype := 'LONG';
                                END IF;
                        END IF;
                ELSIF p_dbfield.fieldtype IN(4,5,6) THEN
                        l_datatype := 'DATE';
                ELSIF p_dbfield.fieldtype = 2 THEN
                        IF p_dbfield.decimalpos > 0 THEN
                                l_datatype := 'DECIMAL';
                        ELSE
                                IF p_dbfield.length <= 4 THEN
                                        l_datatype := 'SMALLINT';
				ELSIF p_dbfield.length > 8 THEN --20.2.2008 pt8.47??
					l_datatype := 'DECIMAL';
					p_dbfield.length := p_dbfield.length + 1;
                                ELSE
                                        l_datatype := 'INTEGER';
                                END IF;
                        END IF;
                ELSIF p_dbfield.fieldtype = 3 THEN
                        l_datatype := 'DECIMAL';
                        IF p_dbfield.decimalpos = 0 THEN
                                p_dbfield.length := p_dbfield.length + 1;
                        END IF;
                END IF;

                l_col_def := l_datatype;
                IF l_datatype = 'VARCHAR2' THEN 
                        IF l_oraver >= 9.0 AND l_chardef = 'Y' THEN /*in Oracle 9 to provide for unicode VARCHAR2s defined in CHRs*/
                                l_col_def := l_col_def||'('||p_dbfield.length||' CHAR)';
                        ELSIF l_unicode_enabled = 1 THEN
                                l_col_def := l_col_def||'('||LEAST(4000,p_dbfield.length*3)||') CHECK(LENGTH('||p_fieldname||')<='||p_dbfield.length||')';
                        ELSE /*normal*/
                                l_col_def := l_col_def||'('||p_dbfield.length||')';
                        END IF;
                ELSIF l_datatype = 'DECIMAL' THEN
                        IF p_dbfield.fieldtype = 2 THEN
                                l_col_def := l_col_def||'('||(p_dbfield.length-1);
                        ELSE /*type 3*/
                                l_col_def := l_col_def||'('||(p_dbfield.length-2);
                        END IF;
                        IF p_dbfield.decimalpos > 0 THEN
                                l_col_def := l_col_def||','||p_dbfield.decimalpos;
                        END IF;
                        l_col_def := l_col_def||')';
                END IF;

                IF p_dbfield.fieldtype IN(0,2,3) OR mod(FLOOR(p_gp_columns.useedit/256),2) = 1 THEN
                        l_col_def := l_col_def||' NOT NULL';
                END IF;

                CLOSE c_dbfield;
                CLOSE c_gp_columns;

                RETURN l_col_def;
        END;

--insert line of script into table
        PROCEDURE ins_line(p_type NUMBER, p_line VARCHAR2) IS
--              PRAGMA AUTONOMOUS_TRANSACTION;
        BEGIN
                l_lineno := l_lineno + 1;

                EXECUTE IMMEDIATE 'INSERT INTO gfc_ddl_script VALUES(:p_type, :l_lineno, :p_line)'
		USING p_type, l_lineno, p_line;

--              COMMIT;
        END;

--insert line of script into table
        PROCEDURE debug_line(p_type NUMBER, p_line VARCHAR2) IS
        BEGIN
                IF l_debug THEN
                        ins_line(p_type, p_line);
                END IF;
        END;

--insert pause into script
        PROCEDURE pause_sql(p_type NUMBER) IS
        BEGIN
                IF l_pause = 'Y' THEN
                        ins_line(p_type,'pause');
                        ins_line(p_type,'');
                END IF;
        END;

--sql whenever error control
        PROCEDURE whenever_sqlerror(p_type NUMBER, p_error BOOLEAN) IS
        BEGIN
                IF p_error THEN
                        ins_line(p_type,'WHENEVER SQLERROR EXIT FAILURE');
                ELSE
                        ins_line(p_type,'WHENEVER SQLERROR CONTINUE');
                END IF;
                ins_line(p_type,'');
        END;

--print generation date into build script
        PROCEDURE signature(p_type NUMBER, p_error BOOLEAN) IS
        BEGIN
		ins_line(p_type,'REM Generated by GFC_PSPART - (c)Go-Faster Consultancy Ltd. www.go-faster.co.uk 2001-2009');
                ins_line(p_type,'REM '||l_dbname||' @ '||TO_CHAR(sysdate,'HH24:MI:SS DD.MM.YYYY'));
                whenever_sqlerror(p_type,p_error);
                ins_line(p_type,'');
        END;

--create database roles
        PROCEDURE create_roles IS
        BEGIN
                IF l_roles = 'Y' THEN
                        ins_line(0,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
                        ins_line(0,LOWER('spool '||LOWER(l_scriptid)||'_'||l_dbname||'.lst'));
                        signature(0,FALSE);
                        ins_line(0,'');
                        ins_line(0,'CREATE ROLE '||LOWER(l_update_all));
                        ins_line(0,'/');
                        ins_line(0,'CREATE ROLE '||LOWER(l_read_all));
                        ins_line(0,'/');
                        ins_line(0,'');
                        ins_line(0,'spool off');
                END IF;
        END;
--generate commands to rename table partititons
        PROCEDURE rename_parts(p_table_name VARCHAR2, p_drop_index VARCHAR2) IS
                CURSOR c_tab_parts(p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'ALTER TABLE '||LOWER(l_schema2||utp.table_name)||' RENAME PARTITION '
                                 ||LOWER(utp.partition_name)||' TO old_'||LOWER(utp.partition_name)||';' rename_cmd
                FROM    user_tab_partitions utp
                WHERE   utp.table_name = p_table_name
		ORDER BY 1
                ;

                CURSOR c_idx_parts (p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'ALTER INDEX '||LOWER(l_schema2||uip.index_name)||' RENAME PARTITION '
                                 ||LOWER(uip.partition_name)||' TO old_'||LOWER(uip.partition_name)||';' rename_cmd
                FROM    user_ind_partitions uip
                ,       user_part_indexes upi
                WHERE   uip.index_name = upi.index_name
                AND     upi.table_name = p_table_name
		ORDER BY 1
                ;

--drop index rather than rebuilding and renaming it somewhere else
                CURSOR c_drop_idx (p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'DROP INDEX '||LOWER(uip.index_name)||';'  drop_cmd
                FROM    user_ind_partitions uip
                ,       user_part_indexes upi
                WHERE   uip.index_name = upi.index_name
                AND     upi.table_name = p_table_name
		ORDER BY 1
                ;

                p_tab_parts c_tab_parts%ROWTYPE;
                p_idx_parts c_idx_parts%ROWTYPE;
                p_drop_idx  c_drop_idx%ROWTYPE;
        BEGIN 
		whenever_sqlerror(0,FALSE);
                OPEN c_tab_parts(p_table_name);
                LOOP
                        FETCH c_tab_parts INTO p_tab_parts;
                        EXIT WHEN c_tab_parts%NOTFOUND;

                        ins_line(0,p_tab_parts.rename_cmd);
                END LOOP;
                CLOSE c_tab_parts;
                ins_line(0,'');

                IF p_drop_index = 'Y' THEN
                        OPEN c_drop_idx(p_table_name);
                        LOOP
                                FETCH c_drop_idx INTO p_drop_idx;
                                EXIT WHEN c_drop_idx%NOTFOUND;

                                ins_line(0,p_drop_idx.drop_cmd);
                        END LOOP;
                        CLOSE c_drop_idx;
                ELSE
                        OPEN c_idx_parts(p_table_name);
                        LOOP
                                FETCH c_idx_parts INTO p_idx_parts;
                                EXIT WHEN c_idx_parts%NOTFOUND;

                                ins_line(0,p_idx_parts.rename_cmd);
                        END LOOP;
                        CLOSE c_idx_parts;
                END IF;
		whenever_sqlerror(0,TRUE);
                ins_line(0,'');
        END;

--generate commands to rename table partititons
        PROCEDURE rename_subparts (p_table_name VARCHAR2, p_drop_index VARCHAR2) IS
                CURSOR c_tab_subparts (p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'ALTER TABLE '||LOWER(l_schema2||utp.table_name)||' RENAME PARTITION '
                                 ||LOWER(utp.subpartition_name)||' TO old_'||LOWER(utp.subpartition_name)||';' rename_cmd
                FROM    user_tab_subpartitions utp
                WHERE   utp.table_name = p_table_name
                ORDER BY 1
                ;

                CURSOR c_idx_subparts (p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'ALTER INDEX '||LOWER(l_schema2||uip.index_name)||' RENAME PARTITION '
                        ||LOWER(uip.subpartition_name)||' TO old_'||LOWER(uip.subpartition_name)||';' rename_cmd
                FROM    user_ind_subpartitions uip
                ,       user_part_indexes upi
                WHERE   uip.index_name = upi.index_name
                AND     upi.table_name = p_table_name
                ORDER BY 1
                ;

--drop index rather than rebuilding and renaming it somewhere else
                CURSOR c_drop_idx (p_table_name VARCHAR2) IS
                SELECT  DISTINCT 'DROP INDEX '||LOWER(uip.index_name)||';'  drop_cmd
                FROM    user_ind_subpartitions uip
                ,       user_part_indexes upi
                WHERE   uip.index_name = upi.index_name
                AND     upi.table_name = p_table_name
                ORDER BY 1
                ;

                p_tab_subparts c_tab_subparts%ROWTYPE;
                p_idx_subparts c_idx_subparts%ROWTYPE;
                p_drop_idx     c_drop_idx%ROWTYPE;
        BEGIN
		whenever_sqlerror(0,FALSE);
                OPEN c_tab_subparts(p_table_name);
                LOOP
                        FETCH c_tab_subparts INTO p_tab_subparts;
                        EXIT WHEN c_tab_subparts%NOTFOUND;

                        ins_line(0,p_tab_subparts.rename_cmd);
                END LOOP;
                CLOSE c_tab_subparts;
                ins_line(0,'');

                IF p_drop_index = 'Y' THEN
                        OPEN c_drop_idx(p_table_name);
                        LOOP
                                FETCH c_drop_idx INTO p_drop_idx;
                                EXIT WHEN c_drop_idx%NOTFOUND;

                                ins_line(0,p_drop_idx.drop_cmd);
                        END LOOP;
                        CLOSE c_drop_idx;
                ELSE
                        OPEN c_idx_subparts(p_table_name);
                        LOOP
                                FETCH c_idx_subparts INTO p_idx_subparts;
                                EXIT WHEN c_idx_subparts%NOTFOUND;

                                ins_line(0,p_idx_subparts.rename_cmd);
                        END LOOP;
                        CLOSE c_idx_subparts;
                END IF;
		whenever_sqlerror(0,TRUE);
                ins_line(0,'');
        END;

--list columns in table for column list in create table DDL
        PROCEDURE tab_cols(p_type INTEGER, p_recname VARCHAR2, p_longtoclob VARCHAR2) IS
                CURSOR c_tab_cols(p_recname VARCHAR2) IS
                SELECT   *
                FROM     gfc_ps_tab_columns
                WHERE    recname = p_recname
                ORDER BY recname, fieldnum
                ;
                p_tab_cols c_tab_cols%ROWTYPE;
                l_col_def VARCHAR2(100 CHAR);
        BEGIN
                OPEN c_tab_cols(p_recname);
                LOOP
                        FETCH c_tab_cols INTO p_tab_cols;
                        EXIT WHEN c_tab_cols%NOTFOUND;

                        IF p_tab_cols.fieldnum = 1 THEN
                                l_col_def := '(';
                        ELSE
                                l_col_def := ',';
                        END IF;
                        l_col_def := l_col_def||LOWER(p_tab_cols.fieldname)||' '||col_def(p_recname,p_tab_cols.fieldname,p_longtoclob);
                        ins_line(p_type,l_col_def);
                END LOOP;
                ins_line(p_type,')');
                CLOSE c_tab_cols;
        END;

--list columns in table for column list in create table DDL
        PROCEDURE tab_col_list(p_recname VARCHAR2, p_column_name BOOLEAN DEFAULT TRUE) IS
                CURSOR c_tab_cols(p_recname VARCHAR2) IS
                SELECT  g.*, c.column_name
                FROM    (
                        SELECT g.*, r.sqltablename, d.fieldtype
                        FROM   gfc_ps_tab_columns g
                        ,      psrecdefn r
                        ,      psdbfield d
			WHERE  r.recname = g.recname
			AND    d.fieldname = g.fieldname
			) g
			LEFT OUTER JOIN gfc_ora_tab_columns c
				ON c.column_name = g.fieldname
				AND c.table_name = DECODE(g.sqltablename,' ','PS_'||g.recname,g.sqltablename)
                        WHERE   g.recname = p_recname
                        ORDER BY g.recname, g.fieldnum
                        ;
                p_tab_cols c_tab_cols%ROWTYPE;
                l_col_def VARCHAR2(100 CHAR);
        BEGIN
                OPEN c_tab_cols(p_recname);
                LOOP
                        FETCH c_tab_cols INTO p_tab_cols;
                        EXIT WHEN c_tab_cols%NOTFOUND;

                        IF p_tab_cols.fieldnum > 1 THEN
                                l_col_def := l_col_def||',';
                        END IF;
			
			IF p_column_name OR p_tab_cols.column_name IS NOT NULL THEN --22.1.2007 - to handle new columns not in table
	                        l_col_def := l_col_def||LOWER(p_tab_cols.fieldname);
			ELSIF p_tab_cols.fieldtype = 0 THEN -- character
				l_col_def := l_col_def||''' ''';
			ELSIF p_tab_cols.fieldtype IN(2,3) THEN -- numeric
				l_col_def := l_col_def||'0';
			ELSIF p_tab_cols.fieldtype IN(4,5,6) THEN --date
				l_col_def := l_col_def||'NULL';
			ELSIF p_tab_cols.fieldtype IN(1,8,9) THEN -- long
				IF mod(FLOOR(p_tab_cols.useedit/256),2) = 1 THEN --required
					l_col_def := l_col_def||''' ''';
				ELSE
					l_col_def := l_col_def||'NULL';
				END IF;
			END IF; --22.1.2007 - end of new column handling 

                        IF LENGTH(l_col_def) >= 80 THEN
                                ins_line(0,l_col_def);
                                l_col_def := '';
                        END IF;
                END LOOP;
                IF LENGTH(l_col_def) > 0 THEN
                        ins_line(0,l_col_def);
                END IF;
                CLOSE c_tab_cols;
        END;

--6.9.2007 substituate table storage variables in same way as Peoplesoft
	FUNCTION tab_storage(p_recname VARCHAR2, p_storage VARCHAR2) RETURN VARCHAR2 IS
		l_storage VARCHAR2(1000 CHAR);
	BEGIN
		l_storage := p_storage;

		FOR c_tab_storage IN
		(SELECT	'**'||d.parmname||'**' parmname
		, 	DECODE(NVL(r.parmvalue,' '),' ',d.parmvalue,r.parmvalue) parmvalue
		FROM	psddldefparms d
		,	psrecddlparm r
		WHERE	d.statement_type = 1 /*create table*/
		AND	d.platformid = 2 /*oracle*/
		AND	d.sizing_set = 0 /*just because*/
		AND	r.recname(+) = p_recname
		AND	r.platformid(+) = d.platformid
		AND	r.sizingset(+) = d.sizing_set /*yes, sizingset without an underscore - psft got it wrong*/
		AND	r.parmname(+) = d.parmname
                AND     l_storage LIKE '%**'||d.parmname||'**%'
                ) LOOP
			l_storage := replace(l_storage,c_tab_storage.parmname,c_tab_storage.parmvalue);
		END LOOP;

		RETURN l_storage;
	END;

--6.9.2007 new:substituate index storage variables in same way as Peoplesoft
	FUNCTION idx_storage(p_recname VARCHAR2, p_indexid VARCHAR2, p_storage VARCHAR2, 
                             p_subpartitions NUMBER DEFAULT 0) RETURN VARCHAR2 IS
		l_storage VARCHAR2(1000 CHAR);
	BEGIN
		l_storage := p_storage;

		FOR c_idx_storage IN
		(SELECT	'**'||d.parmname||'**' parmname
		,	CASE WHEN i.parmvalue IS NOT NULL AND i.parmvalue != ' ' THEN i.parmvalue
			     WHEN g.parmvalue IS NOT NULL THEN g.parmvalue
			     ELSE d.parmvalue
                        END as parmvalue
		FROM	psddldefparms d
			LEFT OUTER JOIN	psidxddlparm i
				ON	i.recname = p_recname
				AND	i.indexid = p_indexid
				AND	i.platformid = d.platformid
				AND	i.sizingset = d.sizing_set /*yes, sizingset without an underscore - psft got it wrong*/
				AND	i.parmname = d.parmname
			LEFT OUTER JOIN	gfc_ps_idxddlparm g
				ON	g.recname = p_recname
				AND	g.indexid = p_indexid
				AND	g.parmname = d.parmname
		WHERE	d.statement_type = 2 /*create index*/
		AND	d.platformid = 2 /*oracle*/
		AND	d.sizing_set = 0 /*just because*/
                AND     l_storage LIKE '%**'||d.parmname||'**%'
                ) LOOP
			l_storage := replace(l_storage,c_idx_storage.parmname,c_idx_storage.parmvalue);
			if p_subpartitions > 1 AND l_oraver < 10 THEN /*can't compress composite partitions*/
				l_storage := replace(l_storage,' COMPRESS',' --COMPRESS');
			END IF;
		END LOOP;

		RETURN l_storage;
	END;

--generate partition clause on the basis of part ranges table
        PROCEDURE tab_listparts(p_type NUMBER, p_recname VARCHAR2
                               ,p_part_id VARCHAR2, p_part_name VARCHAR2) IS --19.3.2010 added subpart
                l_part_def VARCHAR2(300 CHAR);
                l_counter INTEGER := 0;
        BEGIN
		debug('TAB_LISTPARTS:'||p_recname||'/'||p_part_id);

                --12.2.2008 restrict combinations of partitions
		FOR t IN(
			SELECT  a.*
			FROM	gfc_part_lists a, gfc_part_range_lists b
			WHERE	a.part_id = p_part_id
			AND     b.part_id = p_part_id
			AND	b.range_name = p_part_name
			AND     b.list_name = a.part_name
			AND     b.build = 'Y' --if subpartition to be built in range partition
			ORDER BY a.part_name
		) LOOP
			IF l_counter = 0 THEN
				l_part_def := '(';
			ELSE
				l_part_def := ',';
			END IF;
			l_part_def := l_part_def||'SUBPARTITION '||LOWER(p_recname||'_'||p_part_name||'_'||t.part_name);
			ins_line(p_type,l_part_def);

			l_part_def := ' VALUES ('||t.list_value||')';
                        IF t.tab_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def||' TABLESPACE '||t.tab_tablespace;
                        END IF;
                        IF t.tab_storage IS NOT NULL THEN
                                l_part_def := l_part_def||' '||tab_storage(p_recname, t.tab_storage); --6.9.2007
                        END IF;
			debug(l_part_def);
                        ins_line(p_type,l_part_def);
			l_counter := l_counter + 1;
		END LOOP;
		IF l_counter > 0 THEN
			ins_line(p_type,')');
		END IF;
        END;

--generate partition clause on the basis of part ranges table
        PROCEDURE idx_listparts(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2, 
	                        p_part_id VARCHAR2, p_part_name VARCHAR2) IS
                l_part_def VARCHAR2(300 CHAR);
                l_counter INTEGER := 0;
        BEGIN
		debug('idx_listparts:'||p_recname||'/'||p_indexid||'/'||p_part_id);

                --12.2.2008 restrict combinations of partitions
		FOR t IN(
			SELECT  a.*
			FROM	gfc_part_lists a, gfc_part_range_lists b
			WHERE	a.part_id = p_part_id
			AND     b.part_id = p_part_id
			AND	b.range_name = p_part_name
			AND     b.list_name = a.part_name
			AND     b.build = 'Y' --if subpartition to be built in range partition
			ORDER BY a.part_name
		) LOOP
			IF l_counter = 0 THEN
				l_part_def := '(';
			ELSE
				l_part_def := ',';
			END IF;
			l_part_def := l_part_def||'SUBPARTITION '
			                        ||LOWER(p_recname||p_indexid||p_part_name||'_'||t.part_name);
			ins_line(p_type,l_part_def);

			l_part_def := ' VALUES ('||t.list_value||')';
                        IF t.tab_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def||' TABLESPACE '||t.idx_tablespace;
                        END IF;
                        IF t.tab_storage IS NOT NULL THEN
                                l_part_def := l_part_def||' '||
				              idx_storage(p_recname, p_indexid, t.idx_storage);
                        END IF;
			debug(l_part_def);
                        ins_line(p_type,l_part_def);
			l_counter := l_counter + 1;
		END LOOP;
		IF l_counter > 0 THEN
			ins_line(p_type,')');
		END IF;
        END;

--generate partition clause on the basis of part ranges table
        PROCEDURE tab_part_ranges(p_recname VARCHAR2, p_part_id VARCHAR2, 
		            p_subpart_type VARCHAR2, p_subpartitions INTEGER) IS
                CURSOR c_tab_part_ranges(p_recname VARCHAR2) IS
                SELECT *
                FROM gfc_part_ranges r
                WHERE r.part_id = p_part_id
                ORDER BY part_no, part_name;

                p_tab_part_ranges c_tab_part_ranges%ROWTYPE;
                l_part_def VARCHAR2(200 CHAR);
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
        BEGIN
		debug('tab_part_ranges:'||p_recname||'/'||p_part_id||'/'||p_subpart_type);
                OPEN c_tab_part_ranges(p_recname);
                LOOP
                        FETCH c_tab_part_ranges INTO p_tab_part_ranges;
			debug('Part Range:'||p_tab_part_ranges.part_No||'/'||p_tab_part_ranges.part_name);
                        EXIT WHEN c_tab_part_ranges%NOTFOUND;

                        l_counter := l_counter + 1;
                        IF l_counter = 1 THEN
                                l_part_def := '(';
                        ELSE
                                l_part_def := ',';
                        END IF;
                        l_part_def := l_part_def||'PARTITION '||LOWER(p_recname||'_'||p_tab_part_ranges.part_name);
			l_part_def := l_part_def||' VALUES LESS THAN ('||p_tab_part_ranges.part_value||')';
			IF LENGTH(l_part_def) > 70 THEN
	                        ins_line(0,l_part_def);
				l_part_def := '';
			END IF;
                        IF p_tab_part_ranges.tab_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def||' TABLESPACE '||p_tab_part_ranges.tab_tablespace;
                        END IF;
                        IF p_tab_part_ranges.tab_storage IS NOT NULL THEN
                                l_part_def := l_part_def||' '||tab_storage(p_recname, p_tab_part_ranges.tab_storage); --6.9.2007
                        END IF;
			IF LENGTH(l_part_def) > 0 THEN
	                        ins_line(0,l_part_def);
				l_part_def := '';
			END IF;
                        IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                                FOR l_subpartition IN 1..p_subpartitions LOOP
                                        IF l_subpartition = 1 THEN
                                                l_part_def := '(';
                                        ELSE
                                                l_part_def := ',';
                                        END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '
						||LOWER(p_recname||'_'||p_tab_part_ranges.part_name
						||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                        ins_line(0,l_part_def);
                                END LOOP;
                                ins_line(0,')');
			ELSIF p_subpart_type = 'L' THEN
				tab_listparts(0,p_recname,p_part_id,p_tab_part_ranges.part_name); 
                        END IF;
                END LOOP;
                IF l_counter > 0 THEN
                        ins_line(0,')');
                END IF;
                CLOSE c_tab_part_ranges;
        END;

--generate partition clause on the basis of part ranges table
        PROCEDURE tab_part_lists(p_recname VARCHAR2, p_part_id VARCHAR2, 
		            p_subpart_type VARCHAR2, p_subpartitions INTEGER) IS
                CURSOR c_tab_part_lists(p_recname VARCHAR2) IS
                SELECT *
                FROM gfc_part_lists l
                WHERE l.part_id = p_part_id
                ORDER BY part_no, part_name;

                p_tab_part_lists c_tab_part_lists%ROWTYPE;
                l_part_def VARCHAR2(1000 CHAR);
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
        BEGIN
		debug('tab_part_lists:'||p_recname||'/'||p_part_id||'/'||p_subpart_type);
                OPEN c_tab_part_lists(p_recname);
                LOOP
                        FETCH c_tab_part_lists INTO p_tab_part_lists;
			debug('Part Range:'||p_tab_part_lists.part_No||'/'||p_tab_part_lists.part_name);
                        EXIT WHEN c_tab_part_lists%NOTFOUND;

                        l_counter := l_counter + 1;
                        IF l_counter = 1 THEN
                                        l_part_def := '(';
                                ELSE
                                        l_part_def := ',';
                                END IF;
                        l_part_def := l_part_def||'PARTITION '||LOWER(p_recname||'_'||p_tab_part_lists.part_name);
                        ins_line(0,l_part_def);
			l_part_def := ' VALUES ('||p_tab_part_lists.list_value||')';
                        ins_line(0,l_part_def);
			l_part_def := '';
                        IF p_tab_part_lists.tab_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def||' TABLESPACE '||p_tab_part_lists.tab_tablespace;
                        END IF;
                        IF p_tab_part_lists.tab_storage IS NOT NULL THEN
                                l_part_def := l_part_def||' '||tab_storage(p_recname, p_tab_part_lists.tab_storage); --6.9.2007
                        END IF;
			IF LENGTH(l_part_def) > 0 THEN
	                        ins_line(0,l_part_def);
			END IF;
                        IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                                FOR l_subpartition IN 1..p_subpartitions LOOP
                                        IF l_subpartition = 1 THEN
                                                l_part_def := '(';
                                        ELSE
                                                l_part_def := ',';
                                        END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '
						||LOWER(p_recname||'_'||p_tab_part_lists.part_name
						||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                        ins_line(0,l_part_def);
                                END LOOP;
                                ins_line(0,')');
                        END IF;
                END LOOP;
                IF l_counter > 0 THEN
                        ins_line(0,')');
                END IF;
                CLOSE c_tab_part_lists;
        END;

--generate partition clause on the basis of part ranges table
        PROCEDURE ind_hashparts(p_type      NUMBER
	                       ,p_recname   VARCHAR2
	                       ,p_indexid   VARCHAR2 DEFAULT '_'
	                       ,p_num_parts INTEGER) IS
                l_part_def VARCHAR2(200 CHAR);
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
        BEGIN
                IF p_num_parts > 1 THEN
                        FOR l_subpartition IN 1..p_num_parts LOOP
                                IF l_subpartition = 1 THEN
                                        l_part_def := '(';
                                ELSE
                                        l_part_def := ',';
                                END IF;
                                l_part_def := l_part_def||'PARTITION '||LOWER(p_recname||p_indexid||LTRIM(TO_CHAR(l_subpartition,'00')));
	                        ins_line(p_type,l_part_def);
                        END LOOP;
			ins_line(p_type,')');
                END IF;
        END ind_hashparts;

        PROCEDURE tab_hashparts(p_type      NUMBER
	                       ,p_recname   VARCHAR2
	                       ,p_num_parts INTEGER) IS
                l_part_def VARCHAR2(200 CHAR);
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
        BEGIN
		ind_hashparts(p_type, p_recname, '_', p_num_parts);
        END tab_hashparts;

--list indexed columns
        PROCEDURE ind_cols(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2) IS
                CURSOR c_ind_cols(p_recname VARCHAR2, p_indexid VARCHAR2) IS
                SELECT  NVL(LOWER(c.fieldname),k.fieldname) fieldname --6.9.2007 only lower case of columns
--indexes not built descending in PT8.15
--              ||      DECODE(k.ascdesc,0,' DESC')
                ,       k.ascdesc
		,	t.part_column, t.subpart_column
                FROM    gfc_ps_keydefn k
			LEFT OUTER JOIN gfc_ps_tab_columns c --6.9.2007 to determine fields not expressions
			ON c.recname = k.recname
			AND c.fieldname = k.fieldname
			LEFT OUTER JOIN gfc_part_tables t
			ON t.recname = k.recname
                WHERE   k.recname = p_recname
                AND     k.indexid = p_indexid
                ORDER BY k.keyposn
                ;

                p_ind_cols c_ind_cols%ROWTYPE;
                l_col_def VARCHAR2(50 CHAR);
                l_counter INTEGER := 0;
        BEGIN
                OPEN c_ind_cols(p_recname, p_indexid);
                LOOP
                        FETCH c_ind_cols INTO p_ind_cols;
                        EXIT WHEN c_ind_cols%NOTFOUND;
                        l_counter := l_counter + 1;
                        IF l_counter > 1 THEN
                                l_col_def := ',';
                        ELSE
                                l_col_def := '(';
                        END IF;
                        l_col_def := l_col_def||p_ind_cols.fieldname;

                        IF (l_desc_index = 'Y' OR l_desc_index IS NULL) AND p_ind_cols.ascdesc = 0 THEN
                                l_col_def := l_col_def || ' DESC';
                        END IF;
                        ins_line(p_type,l_col_def);
                END LOOP;
                ins_line(p_type,')');
                CLOSE c_ind_cols;
        END;

--generate partition clause on the basis of part ranges table
        PROCEDURE ind_listparts(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2, 
                                p_part_id VARCHAR2, p_part_name VARCHAR2) IS
                l_part_def VARCHAR2(300 CHAR);
                l_counter INTEGER := 0;
        BEGIN
		FOR t IN(
			SELECT  a.*
			FROM	gfc_part_lists a, gfc_part_range_lists b
			WHERE	a.part_id = p_part_id
			AND     b.part_id = p_part_id
			AND	b.range_name = p_part_name
			AND     b.list_name = a.part_name
			AND     b.build = 'Y' --if subpartition to be built in range partition
			ORDER BY a.part_name
		) LOOP
			IF l_counter = 0 THEN
				l_part_def := '(';
			ELSE
				l_part_def := ',';
			END IF;
			l_part_def := l_part_def||'SUBPARTITION '
			                        ||LOWER(p_recname||p_indexid||p_part_name||'_'||t.part_name);
                        IF t.tab_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def||' TABLESPACE '||t.idx_tablespace;
                        END IF;
                        IF t.tab_storage IS NOT NULL THEN
                                l_part_def := l_part_def||' '||idx_storage(p_recname, p_indexid, t.idx_storage);
                        END IF;
                        ins_line(p_type,l_part_def);
			l_counter := l_counter + 1;
		END LOOP;
		IF l_counter > 0 THEN
			ins_line(p_type,')');
		END IF;
        END;

--generate partition clause for create index DDL
        PROCEDURE ind_part_ranges(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2, 
                            p_part_id VARCHAR2, p_subpart_type VARCHAR2, p_subpartitions NUMBER) IS
                CURSOR c_ind_part_ranges IS
                SELECT *
                FROM gfc_part_ranges r
                WHERE r.part_id = p_part_id
                ORDER BY part_no, part_name
                ;
                p_ind_part_ranges c_ind_part_ranges%ROWTYPE;
                l_part_def VARCHAR2(100 CHAR);
                l_subpartition INTEGER;
        BEGIN
                OPEN c_ind_part_ranges;
                l_part_def := '(';
                LOOP
                        FETCH c_ind_part_ranges INTO p_ind_part_ranges;
                        EXIT WHEN c_ind_part_ranges%NOTFOUND;

                        l_part_def := l_part_def||'PARTITION '||LOWER(p_recname||p_indexid||p_ind_part_ranges.part_name);
                        IF p_ind_part_ranges.idx_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def ||' TABLESPACE '||p_ind_part_ranges.idx_tablespace;
                        END IF;
                        IF p_ind_part_ranges.idx_storage IS NOT NULL THEN
                                l_part_def := l_part_def ||' '||
                                      idx_storage(p_recname, p_indexid, 
                                                  p_ind_part_ranges.idx_storage,
                                                  p_subpartitions); --6.9.2007
                        END IF;
                        ins_line(p_type,l_part_def);
                        --explicity define subpartitions
                        IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                                FOR l_subpartition IN 1..p_subpartitions LOOP
                                        IF l_subpartition = 1 THEN
                                                l_part_def := '(';
                                        ELSE
                                                l_part_def := ',';
                                        END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '
                                                      ||LOWER(p_recname||p_indexid||p_ind_part_ranges.part_name
                                                      ||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                        ins_line(p_type,l_part_def);
                                END LOOP;
                                ins_line(p_type,')');
			ELSIF p_subpart_type = 'L' THEN
				ind_listparts(p_type, p_recname, p_indexid, p_part_id, p_ind_part_ranges.part_name);
                        END IF;
                        l_part_def := ',';
                END LOOP;
                ins_line(p_type,')');
                CLOSE c_ind_part_ranges;
        END;

--generate partition clause for create index DDL
        PROCEDURE ind_part_lists(p_type NUMBER, p_recname VARCHAR2, p_indexid VARCHAR2, 
                            p_part_id VARCHAR2, p_subpart_type VARCHAR2, p_subpartitions NUMBER) IS
                CURSOR c_ind_part_lists IS
                SELECT *
                FROM gfc_part_lists r
                WHERE r.part_id = p_part_id
                ORDER BY part_no, part_name
                ;
                p_ind_part_lists c_ind_part_lists%ROWTYPE;
                l_part_def VARCHAR2(100 CHAR);
                l_subpartition INTEGER;
        BEGIN
                OPEN c_ind_part_lists;
                l_part_def := '(';
                LOOP
                        FETCH c_ind_part_lists INTO p_ind_part_lists;
                        EXIT WHEN c_ind_part_lists%NOTFOUND;

                        l_part_def := l_part_def||'PARTITION '||LOWER(p_recname||p_indexid||p_ind_part_lists.part_name);
                        IF p_ind_part_lists.idx_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def ||' TABLESPACE '||p_ind_part_lists.idx_tablespace;
                        END IF;
                        IF p_ind_part_lists.idx_storage IS NOT NULL THEN
                                l_part_def := l_part_def ||' '||
                                      idx_storage(p_recname, p_indexid, p_ind_part_lists.idx_storage,
                                                  p_subpartitions); --6.9.2007
                        END IF;
                        ins_line(p_type,l_part_def);
                        --explicity define subpartitions
                        IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                        FOR l_subpartition IN 1..p_subpartitions LOOP
                                IF l_subpartition = 1 THEN
                                        l_part_def := '(';
                                ELSE
                                        l_part_def := ',';
                                END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '
                                                      ||LOWER(p_recname||p_indexid||p_ind_part_lists.part_name
                                                      ||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                ins_line(p_type,l_part_def);
                        END LOOP;
                        ins_line(p_type,')');
			ELSIF p_subpart_type = 'L' THEN
				ind_listparts(p_type, p_recname, p_indexid, p_part_id, p_ind_part_lists.part_name);
                        END IF;
                        l_part_def := ',';
                END LOOP;
                ins_line(p_type,')');
                CLOSE c_ind_part_lists;
        END;

--enable/disable DDL trigger, added 10.10.2007
	PROCEDURE ddltrigger(p_type NUMBER, p_trgstatus BOOLEAN) IS
	BEGIN
		IF l_ddltrigger IS NOT NULL THEN
			IF p_trgstatus THEN
				ins_line(p_type,'ALTER TRIGGER '||l_ddltrigger||' ENABLE');
			ELSE
				ins_line(p_type,'ALTER TRIGGER '||l_ddltrigger||' DISABLE');
			END IF;
			ins_line(p_type,'/');
			ins_line(p_type,'');
		END IF;
	END;

--generate partition clause for global index
        PROCEDURE glob_ind_parts(p_type INTEGER, p_recname VARCHAR2, p_indexid VARCHAR2, p_part_id VARCHAR2, 
		            p_part_type VARCHAR2, p_subpart_type VARCHAR2, p_subpartitions INTEGER) IS
                CURSOR c_idx_parts (p_recname VARCHAR2) IS
                SELECT *
                FROM gfc_part_ranges r
                WHERE r.part_id = p_part_id
                ORDER BY part_no, part_name;

                p_idx_parts c_idx_parts%ROWTYPE;
                l_part_def VARCHAR2(200 CHAR);
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
        BEGIN
		debug('glob_ind_PARTS:'||p_recname||'/'||p_part_id||'/'||p_part_type||'/'||p_subpart_type);
                OPEN c_idx_parts(p_recname);
                LOOP
                        FETCH c_idx_parts INTO p_idx_parts;
			debug('Part Range:'||p_idx_parts.part_No||'/'||p_idx_parts.part_name);
                        EXIT WHEN c_idx_parts%NOTFOUND;

                        l_counter := l_counter + 1;
                        IF l_counter = 1 THEN
                                l_part_def := '(';
                        ELSE
                                l_part_def := ',';
                        END IF;
                        l_part_def := l_part_def||'PARTITION '||LOWER(p_recname||p_indexid||p_idx_parts.part_name);
			IF p_part_type = 'R' THEN
				l_part_def := l_part_def||' VALUES LESS THAN ('||p_idx_parts.part_value||')';
			ELSIF p_part_type = 'L' THEN /*List*/
				l_part_def := l_part_def||' VALUES ('||p_idx_parts.part_value||')';
			END IF;
                        IF p_idx_parts.idx_tablespace IS NOT NULL THEN
                                l_part_def := l_part_def||' TABLESPACE '||p_idx_parts.idx_tablespace;
                        END IF;
                        IF p_idx_parts.idx_storage IS NOT NULL THEN
                                l_part_def := l_part_def||' '||idx_storage(p_recname, p_indexid, 
				                                           p_idx_parts.idx_storage,
				                                           p_subpartitions); --6.9.2007
                        END IF;
                        ins_line(p_type,l_part_def);
                        IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                                FOR l_subpartition IN 1..p_subpartitions LOOP
                                        IF l_subpartition = 1 THEN
                                                l_part_def := '(';
                                        ELSE
                                                l_part_def := ',';
                                        END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '
					                        ||LOWER(p_recname||p_indexid||p_idx_parts.part_name
					                        ||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                        ins_line(p_type,l_part_def);
                                END LOOP;
                                ins_line(p_type,')');
			ELSIF p_subpart_type = 'L' THEN
				idx_listparts(p_type,p_recname,p_indexid,p_part_id,p_idx_parts.part_name);
                        END IF;
                END LOOP;
                IF l_counter > 0 THEN
                        ins_line(p_type,')');
                END IF;
                CLOSE c_idx_parts;
        END;

--generate all partitioned indexes defined on record
        PROCEDURE mk_part_indexes(p_recname    VARCHAR2
                                 ,p_table_name VARCHAR2
                                 ,p_schema     VARCHAR2
                                 ) IS
                l_ind_def VARCHAR2(100 CHAR);
		l_type INTEGER;
		l_schema VARCHAR2(31 CHAR);
		l_ind_prefix VARCHAR2(3 CHAR);
        BEGIN
                ins_line(1,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
                ins_line(1,LOWER('spool gfcindex_'||l_dbname||'_'||p_recname||'.lst'));
                signature(1,FALSE);
                whenever_sqlerror(1,TRUE);
		ddltrigger(1,FALSE); --added 10.10.2007
                FOR p_indexes IN(
                	SELECT   g.indexid, g.uniqueflag, g.platform_ora
	                ,        p.RECNAME
	                ,        NVL(i.part_id,p.part_id) part_id
	                ,        NVL(i.part_type,p.part_type) part_type
	                ,        NVL(i.part_column,p.part_column) part_column
	                ,        NVL(i.subpart_type,p.subpart_type) subpart_type
	                ,        NVL(i.subpart_column,p.subpart_column) subpart_column 
			,        NVL(i.hash_partitions,p.hash_partitions) hash_partitions
			, 	 NVL(i.idx_tablespace,p.idx_tablespace) idx_tablespace
	                ,        NVL(i.idx_storage,p.idx_storage) idx_storage
	                ,        NVL(i.override_schema,p.override_schema) override_schema
			,        DECODE(i.indexid,NULL,'L','G') ind_part_type
        	        FROM     gfc_ps_indexdefn g --6.9.2007 removed psindexdefn
			         LEFT OUTER JOIN gfc_part_indexes i
			         ON i.recname = g.recname
			         AND i.indexid = g.indexid
                	,        gfc_part_tables p
	                WHERE    p.recname = g.recname
			AND      g.recname = p_recname
                	AND      p.recname = p_recname
			AND      g.platform_ora = 1
			ORDER BY g.indexid)
                LOOP
	                IF l_explicit_schema = 'Y' THEN
				l_schema := NVL(p_indexes.override_schema,LOWER(l_schema1))||'.';
			ELSE 
				l_schema := '';
			END IF;

                        --first do indexes for standard build script --
                        whenever_sqlerror(1,FALSE);
			IF l_drop_index = 'Y' THEN 
	       	                ins_line(1,'DROP INDEX '||LOWER(l_schema||'ps'||p_indexes.indexid||p_recname));
       		                ins_line(1,'/');
       	        	        ins_line(1,'');
			END IF;

               	        whenever_sqlerror(1,TRUE);
                        FOR l_type IN 0..1 LOOP --do the index create twice
				IF l_type = 0 THEN
					l_ind_prefix := 'gfc';
				ELSE
					l_ind_prefix := 'ps';
				END IF;
                                l_ind_def := 'CREATE';
       	                        IF p_indexes.uniqueflag = 1 THEN
               	                        l_ind_def := l_ind_def||' UNIQUE';
                       	        END IF;
                               	l_ind_def := l_ind_def||' INDEX '||
                                                    LOWER(l_schema||l_ind_prefix||p_indexes.indexid||p_recname)||
                                                    ' ON '||LOWER(l_schema||l_ind_prefix||'_'||p_recname);
                                ins_line(l_type,l_ind_def);
       	                        ind_cols(l_type,p_recname,p_indexes.indexid);
				IF p_indexes.ind_part_type = 'L' THEN --local partitioning
                        	        ins_line(l_type,'LOCAL');
					IF p_indexes.part_type = 'L' THEN
        	                        	ind_part_lists(p_type=>l_type,
						          p_recname => p_recname,
						          p_indexid => p_indexes.indexid,
						          p_part_id => p_indexes.part_id,
						          p_subpart_type => p_indexes.subpart_type,
					        	  p_subpartitions => p_indexes.hash_partitions);
					ELSIF p_indexes.part_type = 'R' THEN
        	                        	ind_part_ranges(p_type=>l_type,
					          p_recname => p_recname,
					          p_indexid => p_indexes.indexid,
					          p_part_id => p_indexes.part_id,
					          p_subpart_type => p_indexes.subpart_type,
					          p_subpartitions => p_indexes.hash_partitions);
					ELSIF p_indexes.part_type = 'H' AND 
					      p_indexes.hash_partitions > 1 THEN
						ind_hashparts(l_type,p_recname
					                    ,p_indexes.indexid,p_indexes.hash_partitions);
					END IF;

				ELSIF p_indexes.part_type IN('R') THEN -- add global range index clause here
					IF p_indexes.part_type = 'R' THEN
       				                ins_line(l_type,'GLOBAL PARTITION BY RANGE ('
   						        ||p_indexes.part_column||')');
					ELSE /*List-although oracle doesn't support this yet!*/
       				                ins_line(l_type,'GLOBAL PARTITION BY LIST('
						        ||p_indexes.part_column||')');
					END IF;
		       	                IF p_indexes.subpart_type = 'H' AND 
					   p_indexes.hash_partitions > 1 AND 
					   p_indexes.subpart_column IS NOT NULL THEN
						ins_line(l_type,'SUBPARTITION BY HASH ('||p_indexes.subpart_column
						          ||') SUBPARTITIONS '||p_indexes.hash_partitions);
					ELSIF p_indexes.subpart_type = 'L' AND 
					      p_indexes.subpart_column IS NOT NULL THEN
						ins_line(l_type,'SUBPARTITION BY LIST ('||p_indexes.subpart_column||')');
					END IF;
		                        glob_ind_parts(l_type,p_indexes.recname, p_indexes.indexid, p_indexes.part_id,
					          p_indexes.part_type, p_indexes.subpart_type,
					          p_indexes.hash_partitions);

				ELSIF p_indexes.part_type = 'H' AND p_indexes.hash_partitions > 1 THEN
					ins_line(l_type,'GLOBAL PARTITION BY HASH ('||p_indexes.part_column||')');
					ind_hashparts(p_type     =>l_type
					             ,p_recname  =>p_indexes.recname
					             ,p_indexid  =>p_indexes.indexid
					             ,p_num_parts=>p_indexes.hash_partitions);

				END IF;
				--index level storage clause
                                IF p_indexes.idx_tablespace IS NOT NULL THEN
	                                ins_line(l_type,'TABLESPACE '||p_indexes.idx_tablespace);
       		                END IF;
                      	        IF p_indexes.idx_storage IS NOT NULL THEN
                        	        ins_line(l_type,idx_storage(p_recname, p_indexes.indexid, 
					           p_indexes.idx_storage, p_indexes.hash_partitions)); --6.9.2007
                                END IF;

                                --9.10.2003 - create index parallel
       	                        IF l_parallel = 'Y' THEN
               	                        ins_line(l_type,'PARALLEL');
				ELSE
					ins_line(l_type,'NOPARALLEL');
                                END IF;

       	                        --9.10.2003 - create index nologging, then change it to logged noparallel
               	                IF l_logging = 'N' THEN
                       	                ins_line(l_type,'NOLOGGING');
                               	END IF;
                                ins_line(l_type,'/');
       	                        ins_line(l_type,'');

                                IF l_logging = 'N' THEN
       	                                ins_line(l_type,'ALTER INDEX '||
					           LOWER(l_schema||l_ind_prefix||p_indexes.indexid||p_recname));
                                        ins_line(l_type,'LOGGING');
       	                                ins_line(l_type,'/');
               	                END IF;
                       	        IF l_parallel = 'Y' THEN
                               	       ins_line(l_type,'ALTER INDEX '||
					          LOWER(l_schema||l_ind_prefix||p_indexes.indexid||p_recname));
       	                               ins_line(l_type,'NOPARALLEL');
               	                       ins_line(l_type,'/');
                                END IF;
       	                        ins_line(l_type,'');
			END LOOP;
                       	whenever_sqlerror(0,FALSE);
                        IF l_drop_index = 'Y' THEN
                                ins_line(0,l_noalterprefix||'DROP INDEX '
				          ||LOWER(l_schema||'ps'||p_indexes.indexid||p_recname)); --6.9.2007
       			ELSE
                                ins_line(0,'ALTER INDEX '||LOWER(l_schema||'ps'||p_indexes.indexid||p_recname)
                                          ||' RENAME TO old'||LOWER(p_indexes.indexid||p_recname));
                        END IF;
			ins_line(0,'/');
       			whenever_sqlerror(0,TRUE);
                        ins_line(0,l_noalterprefix||'ALTER INDEX ' --6.9.2007 
			                          ||LOWER(l_schema||'gfc'||p_indexes.indexid||p_recname)
			                          ||' RENAME TO ps'||LOWER(p_indexes.indexid||p_recname)); 
			ins_line(0,'/');
                        ins_line(0,'');
--30.6.2005-bugfix-don't drop anything after index only build
--                      IF l_drop_index = 'Y' THEN
--                            	whenever_sqlerror(1,FALSE);
--                              ins_line(1,'DROP INDEX '||LOWER(l_schema||'ps'||p_indexes.indexid||p_recname)||';');
--             			whenever_sqlerror(1,TRUE);
--     			ELSE
--                              ins_line(1,'ALTER INDEX '||LOWER(p_schema||'ps'||p_indexes.indexid||p_recname)||' RENAME TO old'||LOWER(p_indexes.indexid||p_recname)||';');
--                      END IF;
--                      ins_line(1,'ALTER INDEX '||LOWER(l_schema||'gfc'||p_indexes.indexid||p_recname)
--	   		                         ||' RENAME TO ps'||LOWER(p_indexes.indexid||p_recname)||';');
                        ins_line(1,'');
                END LOOP;
		ddltrigger(1,TRUE); --added 10.10.2007
                ins_line(1,'spool off');
        END;

--generate all GLOBAL TEMPORARY indexes defined on record
        PROCEDURE mk_gt_indexes (p_recname VARCHAR2, p_table_name VARCHAR2, p_suffix VARCHAR2) IS
                CURSOR c_indexes (p_recname VARCHAR2) IS
                SELECT  g.indexid, g.uniqueflag
                FROM    gfc_ps_indexdefn g --6.9.2007 removed psindexdefn
                WHERE   g.recname = p_recname
		AND     g.platform_ora = 1 --20.2.2008 added
                ;
                p_indexes c_indexes%ROWTYPE;
                l_ind_def VARCHAR2(100 CHAR);
        BEGIN
                OPEN c_indexes(p_recname);
                LOOP
                        FETCH c_indexes INTO p_indexes;
                        EXIT WHEN c_indexes%NOTFOUND;

------------------------index build for standard build script
			IF l_drop_index = 'Y' THEN
	                        ins_line(1,'DROP INDEX '||LOWER(l_schema2||'ps'||p_indexes.indexid||p_recname||p_suffix));
	                        ins_line(1,'/');
	                        ins_line(1,'');
			END IF;
                        l_ind_def := 'CREATE';
                        IF p_indexes.uniqueflag = 1 THEN
                                l_ind_def := l_ind_def||' UNIQUE';
                        END IF;
                        l_ind_def := l_ind_def||' INDEX ps'||LOWER(p_indexes.indexid||p_recname||p_suffix)
                                                   ||' ON '||LOWER(p_table_name||p_suffix);
                        ins_line(0,l_ind_def);
                        ins_line(1,l_ind_def);
                        ind_cols(0,p_recname,p_indexes.indexid);
                        ind_cols(1,p_recname,p_indexes.indexid);
                        ins_line(0,'/');
                        ins_line(1,'/');
                        ins_line(0,'');
                        ins_line(1,'');
                END LOOP;
                CLOSE c_indexes;
        END;

--match with database
	PROCEDURE match_db IS
	BEGIN
		IF l_forcebuild = 'Y' THEN
			UPDATE	gfc_ps_tables t
			SET	t.match_db = 'N'
			;
		ELSE
			UPDATE	gfc_ps_tables t
			SET	t.match_db = 'N'
			WHERE	t.table_type = 'P'
			AND     t.match_db IS NULL
			AND	(NOT EXISTS( --table not partitioned
					SELECT table_name 
					FROM user_part_tables o
					WHERE o.table_name = t.table_name)
			OR	NOT EXISTS( --index not partitioned
					SELECT table_name 
					FROM user_part_indexes o
					WHERE o.table_name = t.table_name)
			OR	EXISTS(
					SELECT	otc.column_name, otc.column_id
					FROM	gfc_ora_tab_columns otc
					WHERE 	otc.table_name = t.table_name
					MINUS
					SELECT	ptc.fieldname, ptc.fieldnum
					FROM	gfc_ps_tab_columns ptc
					WHERE 	ptc.recname = t.recname)
			OR	EXISTS(
					SELECT	ptc.fieldname, ptc.fieldnum
					FROM	gfc_ps_tab_columns ptc
					WHERE 	ptc.recname = t.recname
					MINUS
					SELECT	otc.column_name, otc.column_id
					FROM	gfc_ora_tab_columns otc
					WHERE 	otc.table_name = t.table_name)
			OR 	EXISTS(
					SELECT	indexid
					FROM	psindexdefn p
					WHERE	p.recname = t.recname
					MINUS	
					SELECT	SUBSTR(i.index_name,3,1)
					FROM	user_indexes i
					WHERE	i.table_name = t.table_name)
			OR 	EXISTS(
					SELECT	SUBSTR(i.index_name,3,1)
					FROM	user_indexes i
					WHERE	i.table_name = t.table_name
					MINUS	
					SELECT	indexid
					FROM	psindexdefn p
					WHERE	p.recname = t.recname)
			);
		END IF;
	END;

--set index tablespace to partititon tablespace
	PROCEDURE set_index_tablespace(p_type NUMBER, p_recname VARCHAR2, p_part_name VARCHAR2) IS
                l_counter INTEGER := 0;
	BEGIN
		FOR p_indexes IN (	
				SELECT	i.indexid, COALESCE(r.idx_tablespace,t.idx_tablespace) idx_tablespace
				FROM	gfc_ps_indexdefn i
				,	gfc_part_tables t
				,	gfc_part_ranges r --qwert add ps defaults
				WHERE	t.recname = p_recname
				AND	r.part_name = p_part_name
				AND	i.recname = p_recname
				AND	r.part_id = t.part_id
				AND	i.platform_ora = 1 --1.4.2009
				ORDER BY 1
				) LOOP
			IF p_indexes.idx_tablespace IS NOT NULL THEN
				ins_line(p_type,'ALTER INDEX '||LOWER(l_schema2||'PS'||p_indexes.indexid||p_recname)||
			                ' MODIFY DEFAULT ATTRIBUTES TABLESPACE '||LOWER(p_indexes.idx_tablespace));
				ins_line(p_type,'/');
				l_counter := l_counter + 1;
			END IF;
		END LOOP;
		IF l_counter > 0 THEN
			ins_line(p_type,'');
		END IF;
	END;

--set index tablespace to partititon tablespace
	PROCEDURE unset_index_tablespace(p_type NUMBER, p_recname VARCHAR2) IS
                l_counter INTEGER := 0;
	BEGIN
		FOR p_indexes IN (	
				SELECT	i.indexid, t.idx_tablespace
				FROM	gfc_ps_indexdefn i
				,	gfc_part_tables t --qwert add ps defaults
				WHERE	t.recname = p_recname
				AND	i.recname = p_recname
				AND	i.platform_ora = 1 --1.4.2009
				ORDER BY 1
				) LOOP
			IF p_indexes.idx_tablespace IS NOT NULL THEN
				ins_line(p_type,'ALTER INDEX '||LOWER(l_schema2||'PS'||p_indexes.indexid||p_recname)||
			                ' MODIFY DEFAULT ATTRIBUTES TABLESPACE '||LOWER(p_indexes.idx_tablespace));
				ins_line(p_type,'/');
				l_counter := l_counter + 1;
			END IF;
		END LOOP;
		IF l_counter > 0 THEN
			ins_line(p_type,'');
		END IF;
	END;

------------------------------------------------
        PROCEDURE subpart_update_indexes(p_type           NUMBER
                                        ,p_recname        VARCHAR2
	                                ,p_part_name      VARCHAR2
	                                ,p_subpart_name   VARCHAR2
                                        ,p_idx_tablespace VARCHAR2
		                        ,p_idx_storage    VARCHAR2
	                                ) IS
		l_sep     VARCHAR2(20 CHAR);
		l_closure VARCHAR2(2 CHAR);

		CURSOR c_ind_subparts(p_recname VARCHAR2) IS
		SELECT	i.*
		FROM	gfc_ps_indexdefn i
		WHERE	i.recname = p_recname
		AND	i.platform_ora = 1
		AND NOT EXISTS(
			SELECT 	'x'
			FROM	gfc_part_indexes p
			WHERE	p.recname = i.recname
			AND	p.indexid = i.indexid)
		ORDER BY i.indexid
		;

                p_ind_subparts c_ind_subparts%ROWTYPE;
	BEGIN
		l_sep := 'UPDATE INDEXES (';
		l_closure := '';
		OPEN c_ind_subparts(p_recname);
		LOOP
       	                FETCH c_ind_subparts INTO p_ind_subparts;
                        EXIT WHEN c_ind_subparts%NOTFOUND;
			l_closure := '))';

                        ins_line(p_type,l_sep||LOWER('PS'||p_ind_subparts.indexid||p_recname)
                                             ||' (SUBPARTITION '
                                             ||LOWER(p_recname||p_ind_subparts.indexid
                                                   ||p_part_name||'_'||p_subpart_name)
                                        );
	                        IF p_idx_tablespace IS NOT NULL THEN
        	                        ins_line(p_type,'TABLESPACE '||LOWER(p_idx_tablespace));
                	        END IF;
                        	IF p_idx_storage IS NOT NULL THEN
	                                ins_line(p_type,idx_storage(p_recname, p_ind_subparts.indexid, 
                                                 p_idx_storage));
        	                END IF;
				l_sep := '),';
			END LOOP;
			CLOSE c_ind_subparts;

			IF l_closure IS NOT NULL THEN
	                        ins_line(p_type,l_closure);
			END IF;
	END subpart_update_indexes;

--generate partition clause on the basis of part ranges table
        PROCEDURE create_subpartex(p_type         NUMBER
                                  ,p_recname      VARCHAR2
                                  ,p_table_name   VARCHAR2
	                          ,p_part_name    VARCHAR2
	                          ,p_create_table IN OUT BOOLEAN 
                                  ) IS
		l_default_subpartition_name VARCHAR2(30 CHAR) := '';
		l_tab_tablespace VARCHAR2(30 CHAR);
		l_tab_storage VARCHAR2(100 CHAR);
		l_idx_tablespace VARCHAR2(30 CHAR);
		l_idx_storage VARCHAR2(100 CHAR);
		l_schema VARCHAR2(31 CHAR);
                l_ind_def VARCHAR2(100 CHAR);
	BEGIN
		SELECT	s.subpartition_name
		,	pt.tab_tablespace
		,	pt.tab_storage
		,	pt.idx_tablespace
		,	pt.idx_storage
		INTO	l_default_subpartition_name
		,	l_tab_tablespace
		,	l_tab_storage
		,	l_idx_tablespace
		,	l_idx_storage
		FROM	gfc_part_tables pt
		,	gfc_part_lists pl
		,	user_tab_subpartitions s
		WHERE	pt.recname           = p_recname
		AND	pl.part_id           = pt.part_id
		AND	s.table_name         = p_table_name
		AND	s.partition_name     = p_recname||'_'||p_part_name
		AND	s.subpartition_name  = p_recname||'_'||p_part_name||'_'||pl.part_name
		AND	UPPER(pl.list_value) = 'DEFAULT'
		;
		
		IF l_default_subpartition_name IS NOT NULL THEN
			IF p_create_table THEN
				--create table
        	                ins_line(p_type,'CREATE TABLE '||LOWER(l_schema||'gfc_'||p_recname));
                	        tab_cols(p_type,p_recname,l_longtoclob);
                        	IF l_tab_tablespace IS NOT NULL THEN
        	                        ins_line(p_type,'TABLESPACE '||l_tab_tablespace);
	                        END IF;
	                        IF l_tab_storage IS NOT NULL THEN
        	                        ins_line(p_type,tab_storage(p_recname, l_tab_storage)); 
                	        END IF;
                        	ins_line(p_type,'/');
	                        ins_line(p_type,'');

				--create indexes
		                FOR p_indexes IN(
        		        	SELECT   g.indexid, g.uniqueflag, g.platform_ora
					,        i.override_schema
        		        	FROM     gfc_ps_indexdefn g
					         LEFT OUTER JOIN gfc_part_indexes i
					         ON i.recname = g.recname
					         AND i.indexid = g.indexid
		        	        WHERE    g.recname = p_recname
					AND      g.platform_ora = 1
					ORDER BY g.indexid)
		                LOOP
			                IF l_explicit_schema = 'Y' THEN
						l_schema := NVL(p_indexes.override_schema,LOWER(l_schema1))||'.';
					ELSE 
						l_schema := '';
					END IF;
	
        	                        l_ind_def := 'CREATE';
       	        	                IF p_indexes.uniqueflag = 1 THEN
               	        	                l_ind_def := l_ind_def||' UNIQUE';
	                       	        END IF;
        	                       	l_ind_def := l_ind_def||' INDEX '||
                	                                    LOWER(l_schema||'gfc'||p_indexes.indexid||p_recname)||
                        	                            ' ON '||LOWER(l_schema||'gfc_'||p_recname);
                                	ins_line(p_type,l_ind_def);
	       	                        ind_cols(p_type,p_recname,p_indexes.indexid);
					--index level storage clause
                	                IF l_idx_tablespace IS NOT NULL THEN
	                	                ins_line(p_type,'TABLESPACE '||l_idx_tablespace);
       		                	END IF;
	                      	        IF l_idx_storage IS NOT NULL THEN
        	                	        ins_line(p_type,idx_storage(p_recname, p_indexes.indexid
						                           ,l_idx_storage));
                        	        END IF;
	                        	ins_line(p_type,'/');
	        	                ins_line(p_type,'');
				
				END LOOP;
				p_create_table := FALSE;
			END IF;

			--exchange
                        ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
                        ins_line(p_type,'EXCHANGE SUBPARTITION '||LOWER(l_default_subpartition_name));
                        ins_line(p_type,'WITH TABLE '||LOWER(l_schema||'gfc_'||p_recname));
                        ins_line(p_type,'INCLUDING INDEXES WITH VALIDATION');
                        ins_line(p_type,'/');
       	                ins_line(p_type,'');

			--drop default
                        ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
                        ins_line(p_type,'DROP SUBPARTITION '||LOWER(l_default_subpartition_name));
                        ins_line(p_type,'/');
       	                ins_line(p_type,'');
		END IF;

	EXCEPTION
		WHEN no_data_found THEN NULL;

	END create_subpartex;

--generate partition clause on the basis of part ranges table
        PROCEDURE drop_subpartex(p_type       NUMBER
                                ,p_recname    VARCHAR2
                                ,p_table_name VARCHAR2
	                        ,p_part_name  VARCHAR2
	                        ,p_drop_table BOOLEAN 
                                ) IS
		l_subpart_name VARCHAR2(30 CHAR);
		l_tab_tablespace VARCHAR2(30 CHAR);
		l_tab_storage VARCHAR2(100 CHAR);
		l_idx_tablespace VARCHAR2(30 CHAR);
		l_idx_storage VARCHAR2(100 CHAR);
		l_schema VARCHAR2(31 CHAR);
	BEGIN

		SELECT	pl.part_name 
		,	pl.tab_tablespace
		,	pl.tab_storage
		,	pl.idx_tablespace
		,	pl.idx_storage
		INTO	l_subpart_name
		,	l_tab_tablespace
		,	l_tab_storage
		,	l_idx_tablespace
		,	l_idx_storage
		FROM	gfc_part_tables pt
		,	gfc_part_lists pl
		,	user_tab_subpartitions s
		WHERE	pt.recname = p_recname
		AND	pl.part_id = pt.part_id
		AND	UPPER(pl.list_value) = 'DEFAULT'
		AND 	s.table_name         = p_table_name
		AND	s.partition_name     = p_recname||'_'||p_part_name
		AND	s.subpartition_name  = p_recname||'_'||p_part_name||'_'||pl.part_name
		;

		
		IF l_subpart_name IS NOT NULL THEN
			--add default partiton
			ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
			ins_line(p_type,'MODIFY PARTITION '||LOWER(p_recname||'_'||p_part_name)
			             ||' ADD SUBPARTITION '||LOWER(p_recname||'_'||p_part_name||'_'||l_subpart_name));
			ins_line(p_type,'VALUES (DEFAULT)');
			subpart_update_indexes(p_type, p_recname, p_part_name
                                              ,l_subpart_name
			                      ,l_idx_tablespace
			                      ,l_idx_storage);
                        ins_line(p_type,'/');
       	                ins_line(p_type,'');
	

			--exchange default partiton back into part table
                        ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
                        ins_line(p_type,'EXCHANGE SUBPARTITION '||LOWER(p_recname||'_'||p_part_name||'_'||l_subpart_name));
                        ins_line(p_type,'WITH TABLE '||LOWER(l_schema||'gfc_'||p_recname));
                        ins_line(p_type,'INCLUDING INDEXES WITH VALIDATION');
                        ins_line(p_type,'/');
       	                ins_line(p_type,'');

			IF p_drop_table THEN
				--drop partex table
        	                ins_line(p_type,'DROP TABLE '||LOWER(l_schema||'gfc_'||p_recname)||l_drop_purge_suffix);
                	        ins_line(p_type,'/');
       	                	ins_line(p_type,'');
			END IF;

		END IF;

	EXCEPTION
		WHEN no_data_found THEN NULL;

	END drop_subpartex;

--generate partition clause on the basis of part ranges table
        PROCEDURE add_tab_subparts(p_type NUMBER, p_recname VARCHAR2, p_table_name VARCHAR2, 
                                   p_part_id      VARCHAR2, 
                                   p_part_type    VARCHAR2, 
                                   p_subpart_type VARCHAR2) IS

                CURSOR c_tab_subparts(p_table_name VARCHAR2
		                     ,p_recname    VARCHAR2) IS
		SELECT	pr.part_no part_no
		,	pr.part_name part_name
                , 	pl.PART_NO subpart_no       
                , 	pl.PART_NAME subpart_name
                , 	pl.LIST_VALUE     
                , 	pl.TAB_TABLESPACE
                , 	pl.IDX_TABLESPACE
                , 	pl.TAB_STORAGE    
                , 	pl.IDX_STORAGE    
		FROM	gfc_part_ranges pr
		,	gfc_part_lists pl
		WHERE	pr.part_id = p_part_id
		AND     pl.part_id = p_part_id
		AND NOT EXISTS(
			SELECT 'x'
			FROM	user_tab_subpartitions tp
			WHERE	tp.table_name = p_table_name
			AND	tp.partition_name = p_recname||'_'||pr.part_name
			AND	tp.subpartition_name = p_recname||'_'||pr.part_name||'_'||pl.part_name
			)
		ORDER BY pr.part_no, pl.part_no
		;

                p_tab_subparts c_tab_subparts%ROWTYPE;

		l_last_part_name VARCHAR2(30 CHAR) := '';
		l_create_flag BOOLEAN := TRUE;
	BEGIN
                OPEN c_tab_subparts(p_table_name, p_recname);
                LOOP
                        FETCH c_tab_subparts INTO p_tab_subparts;
                        EXIT WHEN c_tab_subparts%NOTFOUND;

			IF l_last_part_name IS NULL OR 
			   l_last_part_name != p_tab_subparts.part_name THEN
				IF l_last_part_name IS NOT NULL THEN
					drop_subpartex(p_type
        	      		       	              ,p_recname
			                              ,p_table_name
        			                      ,l_last_part_name
					              ,FALSE);
				END IF;
				l_last_part_name := p_tab_subparts.part_name;
				create_subpartex(p_type
	       	        	                ,p_recname
       	        	                        ,p_table_name
        	        	                ,p_tab_subparts.part_name
				                ,l_create_flag);
			END IF;

			ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name));
			ins_line(p_type,'MODIFY PARTITION '||
			                 LOWER(p_recname||'_'||p_tab_subparts.part_name)||
			                 ' ADD SUBPARTITION '||
			                 LOWER(p_recname||'_'||p_tab_subparts.part_name||'_'||p_tab_subparts.subpart_name));

			IF p_subpart_type = 'L' THEN /*List*/
				ins_line(p_type,'VALUES ('||p_tab_subparts.list_value||')');
			END IF;
                        IF p_tab_subparts.tab_tablespace IS NOT NULL THEN
                                ins_line(p_type,'TABLESPACE '||LOWER(p_tab_subparts.tab_tablespace));
                        END IF;
                        IF p_tab_subparts.tab_storage IS NOT NULL THEN
                                ins_line(p_type,tab_storage(p_recname, p_tab_subparts.tab_storage));
                        END IF;


			subpart_update_indexes(p_type, p_recname
                                              ,p_tab_subparts.part_name
                                              ,p_tab_subparts.subpart_name
			                      ,p_tab_subparts.idx_tablespace
			                      ,p_tab_subparts.idx_storage);

                        ins_line(p_type,'/');
                        ins_line(p_type,'');
                END LOOP;
                CLOSE c_tab_subparts;

		IF l_last_part_name IS NOT NULL THEN
			drop_subpartex(p_type
                       	              ,p_recname
	                              ,p_table_name
	                              ,l_last_part_name
			              ,TRUE);
		END IF;


        END add_tab_subparts;

--generate partition clause on the basis of part ranges table
        PROCEDURE add_tab_parts(p_type NUMBER, p_recname VARCHAR2, p_table_name VARCHAR2, 
                                p_part_id VARCHAR2, p_part_type VARCHAR2, 
                                p_subpart_type VARCHAR2, p_subpartitions INTEGER) IS
                CURSOR c_tab_parts(p_recname VARCHAR2) IS
		SELECT	pr.*
		FROM	gfc_part_ranges pr
		WHERE	pr.part_id = p_part_id
		AND NOT EXISTS(
			SELECT 'x'
			FROM	user_tab_partitions tp
			WHERE	tp.table_name = p_table_name -- DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
			AND	tp.partition_name = p_recname||'_'||pr.part_name
			)
		AND NOT EXISTS(
			SELECT 'x'
			FROM	gfc_part_ranges pr1
			WHERE	pr1.part_id = p_part_id
			AND	pr1.part_value LIKE '%MAXVALUE%'
			)
                ORDER BY part_no, part_name;

                p_tab_parts c_tab_parts%ROWTYPE;
                l_part_def VARCHAR2(200 CHAR);
                l_counter INTEGER := 0;
                l_subpartition INTEGER;
        BEGIN
		debug('ADD_TAB_PARTS:'||p_recname||'/'||p_tab_parts.part_id||'/'||p_part_type||'/'||p_subpart_type);
                OPEN c_tab_parts(p_recname);
                LOOP
                        FETCH c_tab_parts INTO p_tab_parts;
			debug('Part Range:'||p_tab_parts.part_No||'/'||p_tab_parts.part_name);
                        EXIT WHEN c_tab_parts%NOTFOUND;

--			IF l_counter = 0 THEN
--				signature(p_type,FALSE);
--	                        ins_line(p_type,LOWER('spool gfcalter_'||l_dbname||'_'||p_recname||'.lst'));
--				whenever_sqlerror(p_type, TRUE);
--			END IF;
			l_counter := l_counter + 1;

			--alter indexes to set tablespace to partition tablespace
			set_index_tablespace(p_type, p_recname, p_tab_parts.part_name);

			ins_line(p_type,'ALTER TABLE '||LOWER(l_schema2||p_table_name)
                                        ||' ADD PARTITION '||LOWER(p_recname||'_'||p_tab_parts.part_name));
			IF p_part_type = 'R' THEN
				ins_line(p_type,'VALUES LESS THAN ('||p_tab_parts.part_value||')');
			ELSIF p_part_type = 'L' THEN /*List*/
				ins_line(p_type,'VALUES ('||p_tab_parts.part_value||')');
			END IF;
                        IF p_tab_parts.tab_tablespace IS NOT NULL THEN
                                ins_line(p_type,'TABLESPACE '||p_tab_parts.tab_tablespace);
                        END IF;
                        IF p_tab_parts.tab_storage IS NOT NULL THEN
                                ins_line(p_type,tab_storage(p_recname, p_tab_parts.tab_storage)); --6.9.2007
                        END IF;
                        IF p_subpart_type = 'H' AND p_subpartitions > 1 THEN
                                FOR l_subpartition IN 1..p_subpartitions LOOP
                                        IF l_subpartition = 1 THEN
                                                l_part_def := '(';
                                        ELSE
                                                l_part_def := ',';
                                        END IF;
                                        l_part_def := l_part_def||'SUBPARTITION '||LOWER(p_recname||'_'
					              ||p_tab_parts.part_name||'_'||LTRIM(TO_CHAR(l_subpartition,'00')));
                                        ins_line(p_type,l_part_def);
                                END LOOP;
                                ins_line(p_type,')');
			ELSIF p_subpart_type = 'L' THEN
				tab_listparts(p_type, p_recname
                                             ,p_tab_parts.part_id
                                             ,p_tab_parts.part_name);
                        END IF;
                        ins_line(p_type,'/');
                        ins_line(p_type,'');
                END LOOP;
                IF l_counter > 0 THEN
			--reset index tablespace at table level
			unset_index_tablespace(p_type, p_recname);
--			ins_line(p_type,'spool off');
                END IF;
                CLOSE c_tab_parts;
        END;

--process partitioned tables
        PROCEDURE part_tables  
	(p_recname VARCHAR2 DEFAULT ''
	) IS
                CURSOR c_tables IS
                SELECT  t.table_name
                ,       t.table_type
                ,       p.*
                FROM    gfc_ps_tables t
                ,       gfc_part_tables p
                WHERE   t.table_type = 'P'
                AND     t.match_db = 'N'
                AND     t.rectype =0
                AND     t.recname = p.recname
--                AND 	EXISTS( --check that partitioning column exists
--				SELECT	'x'
--				FROM	gfc_ps_tab_columns c
--				WHERE	c.fieldname = p.part_column
--				AND	c.recname = p.recname
--				)
                AND     (NOT t.recname IN( -- supress partitioning of tables with long columns
                                SELECT   c.recname 
                                FROM     gfc_ps_tab_columns c 
                                ,        psdbfield f
                                WHERE    c.fieldname = f.fieldname
                                AND      f.fieldtype IN(1,8,9) 
                                AND      (f.length = 0 OR f.length > 2000)
                                )
			OR  	t.override_schema IS NOT NULL 
			OR	l_longtoclob = 'Y'
			)
		AND	(p.recname LIKE p_recname OR p_recname IS NULL)
                ORDER BY t.recname;

                p_tables c_tables%ROWTYPE;
                l_hint VARCHAR2(100 CHAR) := '+APPEND';
                l_counter INTEGER := 0;
                l_degree VARCHAR2(100 CHAR);
		l_schema VARCHAR2(30 CHAR);
		l_schema2 VARCHAR2(30 CHAR);
        BEGIN
                IF l_logging = 'N' THEN
                        l_hint := l_hint||' NOLOGGING';
                END IF;
                OPEN c_tables;
                LOOP
                        FETCH c_tables INTO p_tables;
                        EXIT WHEN c_tables%NOTFOUND;

	                IF l_explicit_schema = 'Y' THEN
				l_schema := NVL(p_tables.override_schema,LOWER(l_schema1))||'.';
			ELSE 
				l_schema := '';
			END IF;

                        ins_line(0,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
                        ins_line(0,LOWER('spool '||LOWER(l_scriptid)||'_'||l_dbname||'_'||p_tables.recname||'.lst'));
                        signature(0,FALSE);
			ins_line(0,'rem Partitioning Scheme '||p_tables.part_id);
                        whenever_sqlerror(0,TRUE);
			ddltrigger(0,FALSE); --added 10.10.2007
                        whenever_sqlerror(0,FALSE);
                        ins_line(0,'DROP TABLE '||LOWER(l_schema||'old_'||p_tables.recname)||l_drop_purge_suffix);
			ins_line(0,'/');
                        ins_line(0,'');
                        whenever_sqlerror(0,TRUE);
                        rename_parts(p_tables.table_name,l_drop_index);
			IF (   (p_tables.subpart_type = 'L') 
                            OR (p_tables.subpart_type = 'H' AND p_tables.hash_partitions > 1)
                           ) AND p_tables.subpart_column IS NOT NULL THEN
                        	rename_subparts(p_tables.table_name,l_drop_index);
			END IF;
                        pause_sql(0);
                        ins_line(0,'CREATE TABLE '||LOWER(l_schema||'gfc_'||p_tables.recname));
                        tab_cols(0,p_tables.recname,l_longtoclob);
                        IF p_tables.tab_tablespace IS NOT NULL THEN
                                ins_line(0,'TABLESPACE '||p_tables.tab_tablespace);
                        END IF;
                        IF p_tables.tab_storage IS NOT NULL THEN
                                ins_line(0,tab_storage(p_tables.recname, p_tables.tab_storage)); --6.9.2007
                        END IF;
			IF p_tables.part_type = 'R' THEN
	                        ins_line(0,'PARTITION BY RANGE('||p_tables.part_column||')');
        	                IF p_tables.subpart_type = 'H' AND 
                                   p_tables.hash_partitions > 1 AND 
                                   p_tables.subpart_column IS NOT NULL THEN
                	                ins_line(0,'SUBPARTITION BY HASH ('||p_tables.subpart_column
                                                   ||') SUBPARTITIONS '||p_tables.hash_partitions);
				ELSIF p_tables.subpart_type = 'L' AND 
                                      p_tables.subpart_column IS NOT NULL THEN
                	                ins_line(0,'SUBPARTITION BY LIST ('||p_tables.subpart_column||')');
                        	END IF;
	                        tab_part_ranges(p_tables.recname,p_tables.part_id,
				          p_tables.subpart_type,p_tables.hash_partitions);
			ELSIF p_tables.part_type = 'L' THEN
	                        ins_line(0,'PARTITION BY LIST('||p_tables.part_column||')');
	                        tab_part_lists(p_tables.recname,p_tables.part_id,
				          p_tables.subpart_type,p_tables.hash_partitions);
			ELSIF p_tables.part_type = 'H' AND 
                              p_tables.hash_partitions > 1 THEN
				ins_line(0,'PARTITION BY HASH ('||p_tables.part_column||')');
				tab_hashparts(p_type     =>0
				             ,p_recname  =>p_tables.recname
				             ,p_num_parts=>p_tables.hash_partitions);
			ELSIF p_tables.subpart_type = 'L' AND 
                              p_tables.subpart_column IS NOT NULL THEN
				ins_line(0,'PARTITION BY LIST ('||p_tables.subpart_column||')');
                        END IF;

                        ins_line(0,'ENABLE ROW MOVEMENT');
--9.10.2003 - create table with parallelism enabled
                        IF l_parallel = 'Y' THEN
                                ins_line(0,'PARALLEL');
			ELSE
				ins_line(0,'NOPARALLEL');
                        END IF;
                        IF l_logging = 'N' THEN
                                ins_line(0,'NOLOGGING');
                        END IF;
                        ins_line(0,'/');
                        ins_line(0,'');
--9.10.2003 - was UBS specific but made generic
			IF l_roles = 'Y' THEN
                        	ins_line(0,'GRANT SELECT ON '||LOWER(l_schema||'gfc_'||p_tables.recname)
                                                     ||' TO '||LOWER(l_read_all));
				ins_line(0,'/');
       	            	        ins_line(0,'GRANT INSERT, UPDATE, DELETE ON '||LOWER(l_schema||'gfc_'||p_tables.recname)
                                                                     ||' TO '||LOWER(l_update_all));
				ins_line(0,'/');
                	        ins_line(0,'');
			END IF;

--18.9.2003-added trigger to prevent updates on tables whilst being rebuilt - will be dropped when table is dropped
                        ins_line(0,'LOCK TABLE '||LOWER(l_schema||p_tables.table_name)
			         ||' IN EXCLUSIVE MODE'); --lock table to ensure trigger creates
			ins_line(0,'/');
                        ins_line(0,'');
                        ins_line(0,'CREATE OR REPLACE TRIGGER '||LOWER(l_schema||p_tables.recname)||'_nochange');
                        ins_line(0,'BEFORE INSERT OR UPDATE OR DELETE ON '||LOWER(l_schema||p_tables.table_name));
                        ins_line(0,'BEGIN');
                        ins_line(0,'   RAISE_APPLICATION_ERROR(-20100,''NO OPERATIONS ALLOWED ON '
			         ||UPPER(l_schema||p_tables.table_name)||''');');
                        ins_line(0,'END;');
                        ins_line(0,'/');
                        ins_line(0,'');

                        ins_line(0,'LOCK TABLE '||LOWER(l_schema||p_tables.table_name)
			         ||' IN EXCLUSIVE MODE'); --lock table to prevent consistent reads on query
			ins_line(0,'/');
                        ins_line(0,'');

                        ins_line(0,'INSERT /*'||l_hint||'*/ INTO '||LOWER(l_schema||'gfc_'||p_tables.recname)||' (');
                        tab_col_list(p_tables.recname,p_column_name => TRUE);
                        ins_line(0,') SELECT');
                        tab_col_list(p_tables.recname,p_column_name => FALSE);
                        ins_line(0,'FROM '||LOWER(l_schema||p_tables.table_name));

--20.10.2008 - added criteria option
			IF p_tables.criteria IS NOT NULL THEN
	                        ins_line(0,p_tables.criteria);
			END IF;

			ins_line(0,'/');
                        ins_line(0,'');
                        ins_line(0,'COMMIT');
			ins_line(0,'/');
                        pause_sql(0);
                        mk_part_indexes(p_tables.recname,p_tables.table_name, l_schema);
                        pause_sql(0);
--9.10.2003 - alter table to logging and noparallel
			whenever_sqlerror(0,FALSE);
                        ins_line(0,l_noalterprefix||'ALTER TABLE '||LOWER(l_schema||p_tables.table_name)
			                          ||' LOGGING NOPARALLEL MONITORING'); --6.9.2007
			ins_line(0,'/');
			whenever_sqlerror(0,TRUE);
                        ins_line(0,l_noalterprefix||'ALTER TABLE '||LOWER(l_schema||p_tables.table_name)
			                          ||' RENAME TO old_'||LOWER(p_tables.recname)); --6.9.2007
			ins_line(0,'/');
                        ins_line(0,'');
                        ins_line(0,l_noalterprefix||'ALTER TABLE '||LOWER(l_schema||'gfc_'||p_tables.recname)
			                          ||' RENAME TO '||LOWER(p_tables.table_name)); --6.9.2007
			ins_line(0,'/');
                        ins_line(0,'');
                        pause_sql(0);
--                      ins_line(0,'ANALYZE TABLE '||LOWER(l_schema||p_tables.table_name)
--			                           ||' ESTIMATE STATISTICS SAMPLE 1 PERCENT;');
                        ins_line(2,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
                        ins_line(2,LOWER('spool gfcstats_'||l_dbname||'_'||p_tables.recname||'.lst'));
                        signature(2,FALSE);
                        IF l_build_stats = 'Y' THEN
                                l_counter := 0; /*do build stats command in table build script*/
                        ELSE 
                                l_counter := 2; /*don't build stats command in table build script*/
                        END IF;
                        WHILE l_counter <= 2 LOOP
                                ins_line(l_counter,'');
				IF p_tables.stats_type = 'Y' THEN
	                                IF l_oraver >= 8.173 THEN
        	                                ins_line(l_counter,'BEGIN');
                	                        ins_line(l_counter,'sys.dbms_stats.gather_table_stats');
                        	                ins_line(l_counter,'(ownname=>'''||UPPER(l_schema1)||'''');
                                	        ins_line(l_counter,',tabname=>'''||UPPER(p_tables.table_name)||'''');
                                        	IF l_oraver >= 9 THEN
	                                                IF p_tables.sample_size IS NULL THEN
        	                                                ins_line(l_counter,',estimate_percent=>DBMS_STATS.AUTO_SAMPLE_SIZE');
                	                                ELSE
                        	                                ins_line(l_counter,',estimate_percent=>'||p_tables.sample_size); --6.9.2007
                                	                END IF;
                                        	        --30.10.2007: added method opt override
                                                	ins_line(l_counter,',method_opt=>'''
							        ||NVL(p_tables.method_opt,'FOR ALL COLUMNS SIZE AUTO')
								||'''');
--                                              IF l_parallel_max_servers>1 THEN
--                                                      ins_line(l_counter,',degree=>'||TO_CHAR(l_parallel_max_servers)||'');
--                                              ELSE
--                                                      ins_line(l_counter,',degree=>DBMS_STATS.DEFAULT_DEGREE');
--                                              END IF;
	                                        ELSE
        	                                        IF p_tables.sample_size IS NULL THEN
                	                                        ins_line(l_counter,',estimate_percent=>0.1');
                        	                        ELSE
                                	                        ins_line(l_counter,',estimate_percent=>'||p_tables.sample_size);
                                        	        END IF;
                                                	--30.10.2007: added method opt override
	                                                ins_line(l_counter,',method_opt=>'''
        	                                                 ||NVL(p_tables.method_opt,'FOR ALL INDEXED COLUMNS SIZE 1')||'''');
                	                        END IF;
                        	                IF l_block_sample = 'Y' THEN
                                	                ins_line(l_counter,',block_sample=>TRUE'); 
                                        	END IF;
						IF l_oraver >= 10 THEN
        	                                        ins_line(l_counter,',granularity=>''ALL'''); 
						ELSE
	                	                        IF p_tables.subpart_type = 'H' AND 
                                	                   p_tables.hash_partitions > 1 THEN
	                                	                ins_line(l_counter,',granularity=>''ALL'''); 
							ELSE
		                                                ins_line(l_counter,',granularity=>''ALL'''); 
							END IF;
                	                        END IF;
	
	                                        ins_line(l_counter,',cascade=>TRUE);');
        	                                ins_line(l_counter,'END;');
                	                        ins_line(l_counter,'/');
                        	        ELSE /*use analyze on 8.1.7.2*/
                 	        	        ins_line(l_counter,'ANALYZE TABLE '||l_schema1||'.'
						                           ||LOWER(p_tables.table_name)	
						                           ||' ESTIMATE STATISTICS SAMPLE 1 PERCENT;');
						ins_line(l_counter,'/');
	                                END IF;
				ELSIF p_tables.stats_type = 'D' THEN
       	                                ins_line(l_counter,'BEGIN');
               	                        ins_line(l_counter,' sys.dbms_stats.delete_table_stats');
                       	                ins_line(l_counter,' (ownname=>'''||UPPER(l_schema1)||'''');
                               	        ins_line(l_counter,' ,tabname=>'''||UPPER(p_tables.table_name)||'''');
					IF l_oraver >= 10 THEN
	                               	        ins_line(l_counter,' ,force=>TRUE');
						ins_line(l_counter,' );');
						ins_line(l_counter,' sys.dbms_stats.lock_table_stats');
						ins_line(l_counter,' (ownname=>'''||UPPER(l_schema1)||'''');
						ins_line(l_counter,' ,tabname=>'''||UPPER(p_tables.table_name)||'''');
					END IF;
                                        ins_line(l_counter,' );');
                                        ins_line(l_counter,'END;');
               	                        ins_line(l_counter,'/');
				END IF;
                                ins_line(l_counter,'');
                                l_counter := l_counter + 2;
                        END LOOP;
                        pause_sql(0);
                        ins_line(0,l_noalterprefix||'DROP TABLE ' 
			                          ||LOWER(l_schema||'old_'||p_tables.recname)
                                                  ||l_drop_purge_suffix); --6.9.2007
			ins_line(0,'/');
                        ins_line(0,'');
                        ddltrigger(0,TRUE); --added 10.10.2007
                        whenever_sqlerror(0,FALSE); --6.9.2007
                        ins_line(0,'DROP TRIGGER '||LOWER(l_schema||p_tables.recname)||'_nochange'); --6.9.2007
			ins_line(0,'/');
                        ins_line(0,'');

                        ins_line(3,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
                        ins_line(3,LOWER('spool gfcalter_'||l_dbname||'_'||p_tables.recname||'.lst'));
                        signature(3,TRUE);
			ins_line(3,'rem Partitioning Scheme '||p_tables.part_id);

                        --12.2.2008 append/split missing partitions - but what about indexes
			IF p_tables.part_type IN('R','L') THEN
        	                whenever_sqlerror(3,TRUE);
				ddltrigger(3,FALSE); 
	                        ins_line(3,'');

				add_tab_parts(3, p_tables.recname, p_tables.table_name, 
			                      p_tables.part_id, p_tables.part_type, 
				              p_tables.subpart_type, p_tables.hash_partitions);
				IF p_tables.subpart_type = 'L' THEN
					add_tab_subparts(3, p_tables.recname, p_tables.table_name, 
				                         p_tables.part_id, p_tables.part_type, 
					                 p_tables.subpart_type);
				END IF;

				ddltrigger(3,TRUE); 
	                        ins_line(3,'');
			END IF;
                        ins_line(0,'spool off');
                        ins_line(2,'spool off');
                        ins_line(3,'spool off');


                END LOOP;
                CLOSE c_tables;
        END;

--process global temp tables
        PROCEDURE temp_tables  
	(p_recname VARCHAR2 DEFAULT ''
	) IS
		l_tempinstance  INTEGER;
                l_counter       INTEGER := 0;
                l_counter_start INTEGER := 0;
                l_suffix        VARCHAR2(3 CHAR);
        BEGIN
		IF l_build_stats = 'N' AND l_deletetempstats = 'Y' THEN
			l_counter_start := 2; /*don't build stats command in table build script*/
		ELSE 
			l_counter_start := 0; /*do build stats command in table build script*/
		END IF;

		FOR p_tables IN(
			SELECT   *
			FROM     gfc_ps_tables
			WHERE    table_type = 'T'
			AND	(recname LIKE p_recname OR p_recname IS NULL)
			ORDER BY recname)
		LOOP
                        ins_line(0,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
       	                ins_line(0,LOWER('spool '||LOWER(l_scriptid)||'_'||l_dbname||'_'||p_tables.recname||'.lst'));
               	        signature(0,FALSE);

                        ddltrigger(0,FALSE); --added 29.10.2007

			ins_line(1,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
        	        ins_line(1,LOWER('spool gfcindex_'||l_dbname||'_'||p_tables.recname||'.lst'));
			signature(1,FALSE);

	                FOR l_tempinstance IN 0..p_tables.temptblinstances LOOP

      	                        IF l_tempinstance > 0 THEN
        	                        l_suffix := LTRIM(TO_CHAR(l_tempinstance,'999'));
	                               	whenever_sqlerror(0,FALSE); --ignore drop error 
                                ELSE
                                        l_suffix := '';
                                END IF;

                                ins_line(0,'DROP TABLE '||LOWER(l_schema2||p_tables.table_name||l_suffix)
				                        ||l_drop_purge_suffix);
				ins_line(0,'/');
                              	ins_line(0,'');
                               	whenever_sqlerror(0,TRUE);
                               	ins_line(0,'CREATE GLOBAL TEMPORARY TABLE '
				         ||LOWER(l_schema2||p_tables.table_name||l_suffix));
                                tab_cols(0,p_tables.recname, 'N');
                                ins_line(0,'ON COMMIT PRESERVE ROWS');
				ins_line(0,'/');
                                ins_line(0,'');

                              	mk_gt_indexes(p_tables.recname,p_tables.table_name,l_suffix);
                                ins_line(0,'');

                	        IF l_deletetempstats = 'Y' THEN
					l_counter := l_counter_start;

		                        WHILE l_counter <= 2 LOOP
						IF l_counter = 2 AND l_tempinstance = 0 THEN
				                        ins_line(2,'set echo on pause off verify on feedback on timi on autotrace off pause off lines 100');
				       	                ins_line(2,LOWER('spool gfcstats_'||l_dbname||'_'
							                                  ||p_tables.recname||'.lst'));
				               	        signature(2,FALSE);
						END IF;
	        	                        ins_line(l_counter,'');
        	                                ins_line(l_counter,'BEGIN');
                	                        ins_line(l_counter,'sys.dbms_stats.delete_table_stats');
                        	                ins_line(l_counter,'(ownname=>'''||UPPER(l_schema1)||'''');
	                                        ins_line(l_counter,',tabname=>'''||UPPER(p_tables.table_name||l_suffix)||'''');
						IF l_oraver >= 10 THEN
		                                        ins_line(l_counter,',force=>TRUE);');
                 		                        ins_line(l_counter,'sys.dbms_stats.lock_table_stats');
                        		                ins_line(l_counter,'(ownname=>'''||UPPER(l_schema1)||'''');
                                                        ins_line(l_counter,',tabname=>'''||
							                     UPPER(p_tables.table_name||l_suffix)||''');');
						END IF;
       	                        	        ins_line(l_counter,'END;');
               	                        	ins_line(l_counter,'/');
                        	        	ins_line(l_counter,'');
                                		l_counter := l_counter + 2;
                        	        END LOOP;
				END IF;
	                        pause_sql(0);
	                END LOOP;
                        ddltrigger(0,TRUE); --added 29.10.2007

			l_counter := l_counter_start;
			WHILE l_counter <= 2 LOOP
	                       	ins_line(l_counter,'spool off');
        	                ins_line(l_counter,'');
				l_counter := l_counter + 2;
			END LOOP;
	                ins_line(1,'spool off');
        	        ins_line(1,'');

                END LOOP;
        END;


---------------------------------------------------------------
	PROCEDURE exec_sql
	(p_sql VARCHAR2
	) IS
	BEGIN
		EXECUTE IMMEDIATE p_sql;
	END;
	
---------------------------------------------------------------
--drop named table
	PROCEDURE drop_table
	(p_table_name VARCHAR2
	) IS
		table_does_not_exist EXCEPTION;
		PRAGMA EXCEPTION_INIT(table_does_not_exist,-942);
	BEGIN
		EXECUTE IMMEDIATE 'DROP TABLE '||p_table_name||l_drop_purge_suffix;
	EXCEPTION
		WHEN table_does_not_exist THEN NULL;
	END drop_table;

---------------------------------------------------------------
--gfc_ps_tab_columns holds a list of columns for tables to be recreated.   Any sub-records will be expanded recursively
	PROCEDURE ddl_gfc_ps_tab_columns
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ps_tab_columns';
		l_sql := l_sql||l_lf||'(recname    VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',fieldname  VARCHAR2(18 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',useedit    NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',fieldnum   NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',subrecname VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_tab_columns PRIMARY KEY(recname, fieldname)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
--gfc_ora_tab_columns
	PROCEDURE ddl_gfc_ora_tab_columns
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ora_tab_columns';
		l_sql := l_sql||l_lf||'(table_name  VARCHAR2(30 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',column_name VARCHAR2(30 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',column_id   NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ora_tab_columns PRIMARY KEY(table_name, column_name)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ora_tab_columns_idx2 UNIQUE(table_name, column_id)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
--to hold override parameters for function based indexes specified in partdata - 19.10.2007
	PROCEDURE ddl_gfc_ps_idxddlparm
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ps_idxddlparm';
		l_sql := l_sql||l_lf||'(recname   VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',indexid	  VARCHAR2(18 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',parmname  VARCHAR2(8 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',parmvalue VARCHAR2(128 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_idxddlparm PRIMARY KEY(recname,indexid,parmname)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_part_ranges
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_part_ranges';
		l_sql := l_sql||l_lf||'(part_id        VARCHAR2(8 CHAR) NOT NULL'; --ID of partitioning schema
		l_sql := l_sql||l_lf||',part_no        NUMBER NOT NULL'; --sequence number of range
		l_sql := l_sql||l_lf||',part_name      VARCHAR2(30 CHAR) NOT NULL'; --this goes into the partition names
		l_sql := l_sql||l_lf||',part_value     VARCHAR2(100 CHAR) NOT NULL'; --range less than value
		l_sql := l_sql||l_lf||',tab_tablespace VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',idx_tablespace VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',tab_storage    VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',idx_storage    VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_ranges PRIMARY KEY (part_id, part_no)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_ranges2 UNIQUE(part_id, part_name)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
--gfc_ps_tables holds the records for which DDL scripts are to be regeneated by this script
	PROCEDURE ddl_gfc_ps_tables
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ps_tables';
		l_sql := l_sql||l_lf||'(recname          VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',table_name       VARCHAR2(18 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',table_type       VARCHAR2(1 CHAR)';
		l_sql := l_sql||l_lf||',rectype          NUMBER';
		l_sql := l_sql||l_lf||',temptblinstances NUMBER';
		l_sql := l_sql||l_lf||',override_schema  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',match_db         VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_tables PRIMARY KEY(recname)';
		l_sql := l_sql||l_lf||')';

		exec_sql(l_sql);

	END;

---------------------------------------------------------------
--gfc_ps_indexdefn - expanded version of psindexdefn
	PROCEDURE ddl_gfc_ps_indexdefn
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ps_indexdefn';
		l_sql := l_sql||l_lf||'(recname      VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',indexid      VARCHAR2(1 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',subrecname   VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',subindexid   VARCHAR2(1 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',platform_ora NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',custkeyorder NUMBER NOT NULL'; --6.9.2007
		l_sql := l_sql||l_lf||',uniqueflag   NUMBER NOT NULL'; --6.9.2007    
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_indexdefn PRIMARY KEY(recname, indexid)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_indexdefn2 UNIQUE(subrecname, subindexid)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
--gfc_ps_keydefn - expanded version of pskeydefn
	PROCEDURE ddl_gfc_ps_keydefn
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ps_keydefn';
		l_sql := l_sql||l_lf||'(recname   VARCHAR2(15 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',indexid   VARCHAR2(1 CHAR) NOT NULL';
		l_sql := l_sql||l_lf||',keyposn   NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',fieldname VARCHAR2(100 CHAR) NOT NULL'; --6.9.2007
		l_sql := l_sql||l_lf||',ascdesc   NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_keydefn PRIMARY KEY(recname,indexid,keyposn)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_ps_keydefn2 UNIQUE(recname,indexid,fieldname)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_part_tables
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_part_tables';
		l_sql := l_sql||l_lf||'(recname         VARCHAR2(30 CHAR) NOT NULL'; --peoplesoft record name
		l_sql := l_sql||l_lf||',part_id         VARCHAR2(8 CHAR) NOT NULL'; --ID of partitioning strategy.  Many tables can share one 
		l_sql := l_sql||l_lf||',part_column     VARCHAR2(100 CHAR) NOT NULL'; --range partitioning column, or comma separated columns
		l_sql := l_sql||l_lf||',part_type       VARCHAR2(1 CHAR) NOT NULL'; --(R)ange or (L)ist or (H)ash only 
		l_sql := l_sql||l_lf||' CONSTRAINT tables_part_type CHECK (part_type IN(''R'',''L'',''H''))';
		l_sql := l_sql||l_lf||',subpart_type    VARCHAR2(1 CHAR) 	DEFAULT ''N'''; --(L)ist or (H)ash only 
		l_sql := l_sql||l_lf||' CONSTRAINT tables_subpart_type CHECK (subpart_type IN(''L'',''H'',''N''))';
		l_sql := l_sql||l_lf||',subpart_column  VARCHAR2(100 CHAR)'; --sub partitioning column
		l_sql := l_sql||l_lf||',hash_partitions NUMBER DEFAULT 0 NOT NULL'; --number of hash partitions
		l_sql := l_sql||l_lf||' CONSTRAINT tables_hash_partitions_pos CHECK(hash_partitions>=0)';
		l_sql := l_sql||l_lf||',tab_tablespace  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',idx_tablespace  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',tab_storage     VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',idx_storage     VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',stats_type	VARCHAR2(1 CHAR) DEFAULT ''Y''';
		l_sql := l_sql||l_lf||' CONSTRAINT tables_stats_type CHECK (stats_type IN(''Y'',''N'',''D''))';
		l_sql := l_sql||l_lf||',sample_size     NUMBER'; --analyze sample size : null means auto sample size
		l_sql := l_sql||l_lf||',method_opt      VARCHAR2(100 CHAR)'; --override statistics clause in gather_table_stats
		l_sql := l_sql||l_lf||',override_schema VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_tables PRIMARY KEY(recname)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_tables_columns';
                l_sql := l_sql||       ' CHECK(part_column IS NOT NULL OR subpart_column IS NOT NULL)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_tables_types';
                l_sql := l_sql||       ' CHECK(part_type != subpart_type OR subpart_type = ''N'')';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_part_indexes
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_part_indexes';
		l_sql := l_sql||l_lf||'(recname         VARCHAR2(30 CHAR) NOT NULL'; --peoplesoft record name
		l_sql := l_sql||l_lf||',indexid         VARCHAR2(1 CHAR) NOT NULL'; --peoplesoft index id
		l_sql := l_sql||l_lf||',part_id         VARCHAR2(8 CHAR) NOT NULL'; --ID of partitioning strategy.
		l_sql := l_sql||l_lf||',part_column     VARCHAR2(100 CHAR) NOT NULL'; --range partitioning column, or comma separated columns
		l_sql := l_sql||l_lf||',part_type       VARCHAR2(1 CHAR) NOT NULL'; --(R)ange or (L)ist or (H)ash only 
		l_sql := l_sql||       ' CONSTRAINT index_part_type CHECK (part_type IN(''R'',''L'',''H''))';
		l_sql := l_sql||l_lf||',subpart_type    VARCHAR2(1 CHAR) DEFAULT ''N'''; --(L)ist or (H)ash only 
		l_sql := l_sql||       ' CONSTRAINT index_subpart_type CHECK (subpart_type IN(''L'',''H'',''N''))';
		l_sql := l_sql||l_lf||',subpart_column  VARCHAR2(100 CHAR)'; --sub partitioning column
		l_sql := l_sql||l_lf||',hash_partitions NUMBER'; --number of hash partitions
		l_sql := l_sql||       ' CONSTRAINT indexes_hash_partitions_pos CHECK(hash_partitions>=0)';
		l_sql := l_sql||l_lf||',idx_tablespace  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',idx_storage     VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',override_schema VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',CONSTRAINT index_gfc_part_tables PRIMARY KEY(recname, indexid)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_part_lists 
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_part_lists';
		l_sql := l_sql||l_lf||'(part_id         VARCHAR2(8 CHAR) NOT NULL'; --ID of partitioning schema
		l_sql := l_sql||l_lf||',part_no         NUMBER NOT NULL'; --sequence number of range
		l_sql := l_sql||l_lf||',part_name       VARCHAR2(30 CHAR) NOT NULL'; --this goes into the partition names
		l_sql := l_sql||l_lf||',list_value      VARCHAR2(1000) NOT NULL'; --list value
		l_sql := l_sql||l_lf||',tab_tablespace  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',idx_tablespace  VARCHAR2(30 CHAR)';
		l_sql := l_sql||l_lf||',tab_storage     VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',idx_storage     VARCHAR2(100 CHAR)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_lists PRIMARY KEY (part_id, part_no)';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_lists2 UNIQUE(part_id, part_name)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_part_range_lists
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_part_range_lists';
		l_sql := l_sql||l_lf||'(part_id         VARCHAR2(8 CHAR) NOT NULL'; --ID of partitioning schema
		l_sql := l_sql||l_lf||',range_name      VARCHAR2(30 CHAR) NOT NULL'; --this goes into the partition names
		l_sql := l_sql||l_lf||',list_name       VARCHAR2(30 CHAR) NOT NULL'; --this goes into the partition names
		l_sql := l_sql||l_lf||',build           VARCHAR2(1 CHAR) DEFAULT ''Y'' NOT NULL';
		l_sql := l_sql||l_lf||' CONSTRAINT gfc_part_range_lists_build CHECK (build IN(''Y'',''N''))';
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_part_range_Lists PRIMARY KEY(part_id, range_name, list_name)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_temp_tables
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_temp_tables';
		l_sql := l_sql||l_lf||'(recname VARCHAR2(30 CHAR) NOT NULL'; --peoplesoft record name
		l_sql := l_sql||l_lf||',CONSTRAINT gfc_temp_tables PRIMARY KEY(recname)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_ddl_script
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE ';
		IF p_gtt THEN
			l_sql := l_sql||' GLOBAL TEMPORARY';
		END IF;
		l_sql := l_sql||l_lf||'TABLE gfc_ddl_script';
		l_sql := l_sql||l_lf||'(type   NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',lineno NUMBER NOT NULL';
		l_sql := l_sql||l_lf||',line   VARCHAR2(4000 CHAR)';
		l_sql := l_sql||l_lf||')';
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
	PROCEDURE ddl_gfc_ps_alt_ind_cols
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE OR REPLACE VIEW gfc_ps_alt_ind_cols AS';
		l_sql := l_sql||l_lf||'SELECT   c.recname';
		l_sql := l_sql||l_lf||        ',LTRIM(TO_CHAR(RANK() over (PARTITION BY c.recname';
		l_sql := l_sql||l_lf||                                        'ORDER BY c.fieldnum)-1,''9'')) indexid';
		l_sql := l_sql||l_lf||        ',c.subrecname';
		l_sql := l_sql||l_lf||        ',LTRIM(TO_CHAR(RANK() over (PARTITION BY c.recname, c.subrecname ';
		l_sql := l_sql||l_lf||                                        'ORDER BY c.fieldnum)-1,''9'')) subindexid';
		l_sql := l_sql||l_lf||        ',c.fieldname';
		l_sql := l_sql||l_lf||        ',MOD(FLOOR(useedit/64),2) ascdesc';
		l_sql := l_sql||l_lf||'FROM     gfc_ps_tab_columns c';
		l_sql := l_sql||l_lf||'WHERE    MOD(FLOOR(useedit/16),2) = 1';
--AND    RECNAME= 'JOB'
		exec_sql(l_sql);

	END;

---------------------------------------------------------------
--rem 11.2.2003 - view corrected to handled user indexes

	PROCEDURE ddl_gfc_ps_keydefn_vw
	IS
		l_sql VARCHAR2(1000 CHAR);
        BEGIN
		l_sql := 'CREATE OR REPLACE VIEW gfc_ps_keydefn_vw AS';
		l_sql := l_sql||l_lf||'SELECT  j.recname, j.indexid';
		l_sql := l_sql||l_lf||       ',RANK() OVER (PARTITION BY j.recname, j.indexid';
		l_sql := l_sql||                          ' ORDER BY DECODE(j.custkeyorder,1,k.keyposn,c.fieldnum))';
		l_sql := l_sql||                          ' as keyposn'; --6.9.2007
		l_sql := l_sql||l_lf||       ',k.fieldname';
		l_sql := l_sql||l_lf||       ',c.fieldnum';
		l_sql := l_sql||l_lf||       ',RANK() OVER (PARTITION BY j.recname, j.indexid';
		l_sql := l_sql||                          ' ORDER BY c.fieldnum) as fieldposn';
		l_sql := l_sql||l_lf||       ',k.ascdesc';
		l_sql := l_sql||l_lf||'FROM    gfc_ps_indexdefn j';
		l_sql := l_sql||l_lf||       ',gfc_ps_tab_columns c'; --6.9.2007 removed psindexdefn
		l_sql := l_sql||l_lf||       ',pskeydefn k';
		l_sql := l_sql||l_lf||'WHERE   j.indexid = ''_''';
		l_sql := l_sql||l_lf||'AND     c.recname = j.recname';
		l_sql := l_sql||l_lf||'AND     k.recname = c.subrecname';
		l_sql := l_sql||l_lf||'AND     k.indexid = j.subindexid';
		l_sql := l_sql||l_lf||'AND     k.fieldname = c.fieldname';
		l_sql := l_sql||l_lf||'UNION ALL';
		l_sql := l_sql||l_lf||'SELECT  j.recname, j.indexid';
		l_sql := l_sql||l_lf||       ',RANK() OVER (PARTITION BY j.recname, j.indexid';
		l_sql := l_sql||                          ' ORDER BY k.keyposn) as keyposn';
		l_sql := l_sql||l_lf||       ',k.fieldname';
		l_sql := l_sql||l_lf||       ',c.fieldnum';
		l_sql := l_sql||l_lf||       ',RANK() OVER (PARTITION BY j.recname, j.indexid';
		l_sql := l_sql||                          ' ORDER BY c.fieldnum) as fieldposn';
		l_sql := l_sql||l_lf||       ',k.ascdesc';
		l_sql := l_sql||l_lf||'FROM    gfc_ps_indexdefn j';
		l_sql := l_sql||l_lf||       ',gfc_ps_tab_columns c'; --6.9.2007 removed psindexdefn
		l_sql := l_sql||l_lf||       ',pskeydefn k';
		l_sql := l_sql||l_lf||'WHERE   j.indexid BETWEEN ''A'' AND ''Z''';
		l_sql := l_sql||l_lf||'AND     c.recname = j.recname';
		l_sql := l_sql||l_lf||'AND     k.recname = c.recname';
--                                     AND     k.recname = c.subrecname???qwert
		l_sql := l_sql||l_lf||'AND     k.indexid = j.subindexid';
		l_sql := l_sql||l_lf||'AND     k.fieldname = c.fieldname';
		exec_sql(l_sql);

	END;

------------------------------------------------------------------------------------------------------
--will make these public if I can disable syntax checking
------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------
--Spool script
-----------------------------------------------------------------------------------------------------------
	FUNCTION spooler
	(p_type NUMBER DEFAULT 0) 
	RETURN outrecset PIPELINED 
	IS 
	BEGIN
		FOR i IN (
			SELECT * FROM gfc_ddl_script
			WHERE type = p_type
			ORDER BY lineno) 
		LOOP
			PIPE ROW(i.line);
		END LOOP;
	RETURN;
	END;
---------------------------------------------------------------
--create all working storage tables
	PROCEDURE create_tables
	(p_gtt BOOLEAN DEFAULT FALSE) 
	IS
	BEGIN
		ddl_gfc_ps_tables(p_gtt);
		ddl_gfc_ps_tab_columns(p_gtt);
		ddl_gfc_ora_tab_columns(p_gtt);
		ddl_gfc_ps_indexdefn(p_gtt);
		ddl_gfc_ps_keydefn(p_gtt);
		ddl_gfc_ps_idxddlparm(p_gtt);
		ddl_gfc_part_ranges(p_gtt);
		ddl_gfc_part_tables(p_gtt);
		ddl_gfc_part_indexes(p_gtt);
		ddl_gfc_part_lists(p_gtt);
		ddl_gfc_part_range_lists(p_gtt);
		ddl_gfc_temp_tables(p_gtt);
		ddl_gfc_ddl_script(p_gtt);
	
		ddl_gfc_ps_alt_ind_cols;
		ddl_gfc_ps_keydefn_vw;
	END create_tables;

---------------------------------------------------------------
--drop named table
	PROCEDURE drop_tables
	IS
	BEGIN
		read_context;
		oraver;
		drop_table('gfc_ps_tables');
		drop_table('gfc_ps_tab_columns');
		drop_table('gfc_ora_tab_columns');
		drop_table('gfc_ps_indexdefn');
		drop_table('gfc_ps_keydefn');
		drop_table('gfc_ddl_script');
		drop_table('gfc_ps_idxddlparm');
		drop_table('gfc_part_tables');
		drop_table('gfc_part_indexes');
		drop_table('gfc_part_ranges');
		drop_table('gfc_temp_tables');
		drop_table('gfc_part_lists');
		drop_table('gfc_part_range_lists');
	END drop_tables;

------------------------------------------------------------------------------------------------------
--public procedures and functions
------------------------------------------------------------------------------------------------------
--read defaults from contexts
	PROCEDURE banner IS
	BEGIN
		sys.dbms_output.put_line('GFC_PSPART - Partitioned/Global Temporary Table DDL generator for PeopleSoft');
		sys.dbms_output.put_line('(c)Go-Faster Consultancy Ltd. www.go-faster.co.uk 2001-2009');
	END;

--read defaults from contexts
	PROCEDURE display_defaults IS
	BEGIN
		banner;
		read_context;
		sys.dbms_output.put_line(CHR(10)||'PACKAGE DEFAULTS');
		sys.dbms_output.put_line(CHR(10)||
                                         'Character VARCHAR2 definition          : '||l_chardef);
		sys.dbms_output.put_line('Rebuild tables with redo logging       : '||l_logging);
		sys.dbms_output.put_line('Enable parallel index build            : '||l_parallel);
		sys.dbms_output.put_line('Grant privileges to roles              : '||l_roles);
		sys.dbms_output.put_line('Name of update all role                : '||l_update_all);
		sys.dbms_output.put_line('Name of select all role                : '||l_read_all);
		sys.dbms_output.put_line('ID Prefix in files and project         : '||l_scriptid);
		sys.dbms_output.put_line('Drop indexes                           : '||l_drop_index);
		sys.dbms_output.put_line('Pause commands in build script         : '||l_pause);
		sys.dbms_output.put_line('Explicitly specify schema              : '||l_explicit_schema);
		sys.dbms_output.put_line('Block sample table statistics          : '||l_block_sample);
		sys.dbms_output.put_line('Analyze table immediately after rebuild: '||l_build_stats);
		sys.dbms_output.put_line('Delete and lock statistics on GTTs     : '||l_deletetempstats);
		sys.dbms_output.put_line('Force LONGs to CLOBs                   : '||l_longtoclob);
		sys.dbms_output.put_line('Name of DDL trigger to disable on build: '||l_ddltrigger);
		sys.dbms_output.put_line('Drop tables with PURGE option          : '||l_drop_purge);
--		sys.dbms_output.put_line('No alter prefix                        : '||l_noalterprefix);
		sys.dbms_output.put_line('Force rebuild if no change             : '||l_forcebuild);
		sys.dbms_output.put_line('Force descending index                 : '||l_desc_index);
	END display_defaults;

--read defaults from contexts
        PROCEDURE reset_defaults IS
        BEGIN
		reset_variables;
		write_context;
	END reset_defaults;

--set new defaults to contexts
        PROCEDURE set_defaults
        (p_chardef         VARCHAR2 DEFAULT ''
        ,p_logging         VARCHAR2 DEFAULT ''
        ,p_parallel        VARCHAR2 DEFAULT ''
        ,p_roles           VARCHAR2 DEFAULT ''
        ,p_scriptid        VARCHAR2 DEFAULT ''
        ,p_update_all      VARCHAR2 DEFAULT ''
        ,p_read_all        VARCHAR2 DEFAULT ''
        ,p_drop_index      VARCHAR2 DEFAULT ''
        ,p_pause           VARCHAR2 DEFAULT ''
        ,p_explicit_schema VARCHAR2 DEFAULT ''
        ,p_block_sample    VARCHAR2 DEFAULT ''
        ,p_build_stats     VARCHAR2 DEFAULT ''
        ,p_deletetempstats VARCHAR2 DEFAULT ''
        ,p_longtoclob      VARCHAR2 DEFAULT ''
        ,p_ddltrigger      VARCHAR2 DEFAULT '*'
        ,p_drop_purge      VARCHAR2 DEFAULT ''
--      ,p_noalterprefix   VARCHAR2 DEFAULT ''
        ,p_forcebuild      VARCHAR2 DEFAULT ''
        ,p_desc_index      VARCHAR2 DEFAULT ''
        ) IS
        BEGIN
		read_context; --read current defaults
		IF p_chardef IS NOT NULL THEN
			l_chardef := p_chardef;
		END IF;

		IF p_logging IS NOT NULL THEN
			l_logging := p_logging;
		END IF;

		IF p_parallel IS NOT NULL THEN
			l_parallel := p_parallel;
		END IF;

		IF p_roles IS NOT NULL THEN
			l_roles := p_roles;
		END IF;

		IF p_scriptid IS NOT NULL THEN
			l_scriptid := p_scriptid;
		END IF;

		IF p_update_all IS NOT NULL THEN
			l_update_all := p_update_all;
		END IF;

		IF p_read_all IS NOT NULL THEN
			l_read_all := p_read_all;
		END IF;

		IF p_drop_index IS NOT NULL THEN
			l_drop_index := p_drop_index;
		END IF;

		IF p_pause IS NOT NULL THEN
			l_pause := p_pause;
		END IF;

		IF p_explicit_schema IS NOT NULL THEN
			l_explicit_schema := p_explicit_schema;
		END IF;

		IF p_block_sample IS NOT NULL THEN
			l_block_sample := p_block_sample;
		END IF;

		IF p_build_stats IS NOT NULL THEN
			l_build_stats := p_build_stats;
		END IF;

		IF p_deletetempstats IS NOT NULL THEN
			l_deletetempstats := p_deletetempstats;
		END IF;

		IF p_longtoclob IS NOT NULL THEN
			l_longtoclob := p_longtoclob;
		END IF;

		IF p_ddltrigger IS NULL OR p_ddltrigger != '*' THEN
			l_ddltrigger := p_ddltrigger;
		END IF;

		IF p_drop_purge IS NOT NULL THEN
			l_drop_purge := p_drop_purge;
		END IF;

--		IF p_noalterprefix IS NOT NULL THEN
--			l_noalterprefix := p_noalterprefix;
--		END IF;

		IF p_forcebuild IS NOT NULL THEN
			l_forcebuild := p_forcebuild;
		END IF;

		IF p_desc_index IS NOT NULL THEN
			l_desc_index := p_desc_index;
		END IF;
		write_context;
	END set_defaults;

----make sure that working storage tables are empty
	PROCEDURE truncate_tables 
	(p_all BOOLEAN DEFAULT FALSE
	) IS
		l_all NUMBER := 0;
	BEGIN
		IF p_all THEN
			l_all := 1;
		END IF;

		FOR p_tables IN (
			SELECT 	table_name
			FROM 	user_tables
			WHERE	(	l_all = 1
				AND	table_name IN( --set up by partdata script
						'GFC_PS_IDXDDLPARM',
						'GFC_PART_TABLES',
						'GFC_PART_INDEXES',
						'GFC_PART_RANGES',
						'GFC_TEMP_TABLES',
						'GFC_PART_LISTS',
						'GFC_PART_RANGE_LISTS')
				)
			OR 	table_name IN( --maintained by package
					'GFC_PS_TABLES',
					'GFC_PS_TAB_COLUMNS',
					'GFC_ORA_TAB_COLUMNS',
					'GFC_DDL_SCRIPT',
					'GFC_PS_INDEXDEFN',
					'GFC_PS_KEYDEFN')
		) LOOP

			EXECUTE IMMEDIATE 'TRUNCATE TABLE '||p_tables.table_name;

	 	END LOOP;
	END truncate_tables;

--this is the start of the processing
	PROCEDURE main 
	(p_recname     VARCHAR2 DEFAULT ''  --name of table(s) to be built-pattern matching possible-default null implies all
        ,p_rectype     VARCHAR2 DEFAULT 'A' --Build (P)artitioned tables, Global (T)emp tables, or (A)ll tables - default ALL
        ,p_projectname VARCHAR2 DEFAULT ''  --Build records in named Application Designer Project
	)IS
	BEGIN
		read_context;

		IF p_rectype IS NULL OR NOT p_rectype IN('A','T','P') THEN
			RAISE_APPLICATION_ERROR(-20001,'GFCBUILD: Parameter p_rectype, invalid value '''||p_rectype||'''');
		END IF;

        	oraver;
	        dbname;
        	ptver;

		IF p_recname IS NULL AND p_rectype = 'A' THEN --only clear output tables before generating all records
		        truncate_tables;
		END IF;

	        gfc_ps_tables
		(p_recname => p_recname
		,p_rectype => p_rectype
		);

        	gfc_project(p_projectname => p_projectname);

	        gfc_ps_tab_columns(p_recname => p_recname);

        	expand_sbr;
--      	shuffle_long;
	        match_db;

	        gfc_ps_indexdefn(p_recname => p_recname);
        	gfc_ps_keydefn(p_recname => p_recname);


		IF p_rectype IN('A','P') OR p_rectype IS NULL THEN
		        create_roles;
	        	part_tables(p_recname => p_recname);
		END IF;

		IF p_rectype IN('A','T') OR p_rectype IS NULL THEN
		        temp_tables(p_recname => p_recname);
		END IF;

		commit;
	END main;

END gfc_pspart;
/
set echo off
show errors
spool off


