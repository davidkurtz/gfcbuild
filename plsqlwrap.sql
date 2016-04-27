REM plsqlwrap.sql
--CREATE OR REPLACE DIRECTORY gfc_pspart AS 'C:\Users\david\Documents\SQL\gfcbuild';
--GRANT read, write ON DIRECTORY gfc_pspart TO sysadm;

set serveroutput on 
DECLARE
  l_counter INTEGER;
  l_source  DBMS_SQL.VARCHAR2A;
  l_wrap    DBMS_SQL.VARCHAR2A;
  fileHandler UTL_FILE.FILE_TYPE;
  l_text    VARCHAR2(4000);
BEGIN
  l_source(1):= 'CREATE OR REPLACE ';
  l_counter := 1;

  FOR i IN (
    SELECT *
    FROM   user_source
    WHERE  type = 'PACKAGE BODY'
    AND    name = 'GFC_PSPART'
    ORDER BY line
  ) LOOP
    l_counter:=l_counter+1;
    l_source(l_counter) := i.text;
  END LOOP;
 
  l_wrap := SYS.DBMS_DDL.WRAP(ddl => l_source,
                              lb  => 1,
                              ub  => l_source.count);

  BEGIN
    fileHandler := UTL_FILE.FOPEN('GFC_PSPART', 'gfcbuildpkgbody.plb', 'W',4000);
    UTL_FILE.PUT_line(fileHandler, 'set echo on');
    UTL_FILE.PUT_line(fileHandler, 'spool gfcbuildpkgbody');

    FOR i IN 1 .. l_wrap.count LOOP
      UTL_FILE.PUT_line(fileHandler, l_wrap(i), TRUE);
    END LOOP;
    UTL_FILE.PUT_line(fileHandler, '/');
    UTL_FILE.PUT_line(fileHandler, 'set echo off');
    UTL_FILE.PUT_line(fileHandler, 'show errors');
    UTL_FILE.PUT_line(fileHandler, 'spool off');

    UTL_FILE.FCLOSE(fileHandler);
  END;
END;
/

