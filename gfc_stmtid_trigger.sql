rem gfc_stmtid_trigger.sql
rem (c)Go-Faster Consultancy Ltd. 2009

rem Trigger GFC_STMTID adds identification comment to stored as they are loaded with Data Mover
rem NB Oracle triggers cannot reference long columns.  Therefore, this trigger will not work if PS_SQLSTMT_TBL.STMT_TEXT is a LONG column
rem From PeopleSoft Application v9 PSSTATUS.DATABASE_OPTIONS should be 2, and LONGs will be created as CLOBs
rem This trigger only fires on insert which is what Data Mover does when loading statements.
rem see also http://blog.psftdba.com/2007/07/changes-to-long-columns-and-unicode-in.html


rollback;
set serveroutput on buffer 1000000000 echo on verify on feedback on pause off

CREATE OR REPLACE TRIGGER gfc_stmtid 
BEFORE INSERT ON ps_sqlstmt_tbl
FOR EACH ROW   
DECLARE
 l_stmt_text CLOB;         /*for stmt text so can use text functions*/
 l_stmt_id   VARCHAR2(18); /*PS stmt ID string*/
 l_len       INTEGER;      /*length of stmt text*/
 l_spcpos    INTEGER;      /*postition of first space*/
 l_compos    INTEGER;      /*postition of first comment*/
 l_compos2   INTEGER;      /*end of first comment*/
 l_idpos     INTEGER;      /*postition of statement id*/
BEGIN
 l_stmt_id   := :new.pgm_name||'_'||:new.stmt_type||'_'||:new.stmt_name;
 l_stmt_text := :new.stmt_text;
 l_spcpos    := INSTR(l_stmt_text,' ');
 l_compos    := INSTR(l_stmt_text,'/*');
 l_compos2   := INSTR(l_stmt_text,'*/');
 l_idpos     := INSTR(l_stmt_text,l_stmt_id);
 l_len       := LENGTH(l_stmt_text);

 IF (l_idpos = 0 AND l_spcpos > 0 AND l_len<=32000) THEN 
  /*no id comment in string and its not too long so add one*/
  IF (l_compos = 0) THEN /*no comment exists*/
   l_stmt_text := SUBSTR(l_stmt_text,1,l_spcpos) ||'/*'||l_stmt_id||'*/'||SUBSTR(l_stmt_text,l_spcpos);
  ELSE /*insert into existing comment*/
   l_stmt_text := SUBSTR(l_stmt_text,1,l_compos2-1)||' '||l_stmt_id||SUBSTR(l_stmt_text,l_compos2);
  END IF;
  :new.stmt_text := l_stmt_text;
 END IF;

END gfc_stmtid;
/
show errors


rem Trigger gfc_stmtstats replaces %UPDATESTATS macro in stored statements will call to wrapper package.
rem Replace: %UPDATESTATS(wibble)
rem With   : BEGIN wrapper.ps_stats(p_ownname=>user,p_tabname=>'wibble'); END;;

rollback;
CREATE OR REPLACE TRIGGER gfc_stmtstats 
BEFORE INSERT ON ps_sqlstmt_tbl
FOR EACH ROW   
DECLARE
 l_stmt_text CLOB;         /*for stmt text so can use text functions*/
 l_stmt_id   VARCHAR2(18); /*PS stmt ID string*/
 l_keyword   VARCHAR2(20):='%UPDATESTATS(';
 l_keypos    INTEGER;
 l_keypos2   INTEGER;
 l_keylen    INTEGER;
BEGIN
 l_stmt_id   := :new.pgm_name||'_'||:new.stmt_type||'_'||:new.stmt_name;
 l_stmt_text := :new.stmt_text;
 l_keylen    := LENGTH(l_keyword);
 l_keypos    := INSTR(l_stmt_text,l_keyword);
 l_keypos2   := INSTR(l_stmt_text,')');

 IF l_keypos > 0 AND l_keypos2 > l_keypos THEN 
  :new.stmt_text := 'BEGIN /*'||l_stmt_id
                 ||'*/ wrapper.ps_stats(p_ownname=>user,p_tabname=>'''
                 ||substr(l_stmt_text,l_keypos+l_keylen,l_keypos2-l_keypos-l_keylen)
                 ||'''); END;'
                 ;
 END IF;

END gfc_stmtstats;
/
show errors

/*--the following section is for testing only
set serveroutput on
DELETE FROM PS_SQLSTMT_TBL WHERE PGM_NAME = 'GPPSERVC' AND STMT_TYPE = 'U' AND STMT_NAME = 'STATH';
INSERT INTO PS_SQLSTMT_TBL (PGM_NAME, STMT_TYPE, STMT_NAME, STMT_TEXT) VALUES ('GPPSERVC', 'U', 'STATH'
, '%UPDATESTATS(PS_GP_PYE_HIST_WRK)');

select trigger_name, status
from user_Triggers
where table_name = 'PS_SQLSTMT_TBL';

set long 100
select *
from ps_sqlstmt_tbl
where pgm_name like 'GPPSERVC%'
and  (stmt_text like '%UPDATESTATS(%'
or    stmt_text like '%wrapper%')
;
*/