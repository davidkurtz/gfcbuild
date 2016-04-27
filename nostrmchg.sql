spool nostrmchg

CREATE OR REPLACE TRIGGER sysadm.no_strm_changes
BEFORE INSERT OR UPDATE OR DELETE ON sysadm.ps_gp_strm
BEGIN
   RAISE_APPLICATION_ERROR(-20100,'No updates permitted on PS_GP_STRM');
END;
/

ALTER TRIGGER sysadm.no_strm_changes ENABLE;

spool off
