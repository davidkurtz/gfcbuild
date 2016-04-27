set lines 100 pages 100
column min_emp format a11
column max_emp format a11
column cal_run_id format a18
column cal_id format a18
column gp_paygroup format a10

break on cal_run_id skip 1 on gp_paygroup skip 1 on report
compute sum of emps on report
compute sum of num_rows on report

spool strmvols

ttitle gp_pye_seg_stat

select emplid
from ps_gp_pye_seg_stat p
where not exists(select 'x'
from ps_gp_strm s
where p.emplid between s.emplid_from and s.emplid_to)
/

break on cal_run_id skip 1 on gp_paygroup skip 1 on report
compute sum of emps on report
compute sum of num_rows on report

ttitle 'personal_data by stream'

select 	s.strm_num
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp
,	count(*) emps
from 	ps_personal_data p, ps_gp_strm s
where 	p.emplid between s.emplid_from and s.emplid_to
group by s.strm_num
order by 1
/

ttitle 'gp_pye_seg_stat by stream'

select 	s.strm_num 
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from 	ps_gp_pye_seg_stat p, ps_gp_strm s
where 	p.emplid between s.emplid_from and s.emplid_to
group 	by s.strm_num
order by 1
/

ttitle 'gp_pye_seg_stat by calendar and stream '

select 	p.cal_run_id, s.strm_num
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from	ps_gp_pye_seg_stat p, ps_gp_strm s
where 	p.emplid between s.emplid_from and s.emplid_to
group by p.cal_run_id, s.strm_num
order by 1,2
/

ttitle 'gp_pye_seg_stat by calendar, stream and paygroup'

select 	p.cal_run_id, p.gp_paygroup, s.strm_num
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from	ps_gp_pye_seg_stat p, ps_gp_strm s
where 	p.emplid between s.emplid_from and s.emplid_to
group by p.cal_run_id, p.gp_paygroup, s.strm_num
order by 1,2,3
/

ttitle 'gp_pye_seg_stat by calendar, retro and stream '

select 	p.cal_run_id, SUBSTR(p.cal_id,1,LENGTH(p.cal_run_id)) cal_id, s.strm_num
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from	ps_gp_pye_seg_stat p, ps_gp_strm s
where 	p.emplid between s.emplid_from and s.emplid_to
group by p.cal_run_id, SUBSTR(p.cal_id,1,LENGTH(p.cal_run_id)), s.strm_num
order by 1,2,3
/

ttitle 'gp_pye_seg_stat by calendar and retro'

select 	p.cal_run_id, SUBSTR(p.cal_id,1,LENGTH(p.cal_run_id)) cal_id
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from	ps_gp_pye_seg_stat p
group by p.cal_run_id, SUBSTR(p.cal_id,1,LENGTH(p.cal_run_id))
order by 1,2
/

ttitle 'gp_pye_seg_stat by calendar and retro'

select 	p.cal_run_id, p.cal_id
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from	ps_gp_pye_seg_stat p
group by p.cal_run_id, p.cal_id
order by 1,2
/

ttitle 'gp_rslt_ern_ded by calendar and stream'

select 	p.cal_run_id, s.strm_num 
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from 	ps_gp_rslt_ern_ded p, ps_gp_strm s
where 	p.emplid between s.emplid_from and s.emplid_to
group by p.cal_run_id, s.strm_num
order by 1,2
/

ttitle 'gp_rslt_acum by calendar and stream'

select 	p.cal_run_id, s.strm_num 
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from 	ps_gp_rslt_acum p, ps_gp_strm s
where 	p.emplid between s.emplid_from and s.emplid_to
group by p.cal_run_id, s.strm_num
order by 1,2
/

ttitle 'gp_rslt_pin by calendar and stream'

select 	p.cal_run_id, s.strm_num 
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from 	ps_gp_rslt_pin p, ps_gp_strm s
where 	p.emplid between s.emplid_from and s.emplid_to
group by p.cal_run_id, s.strm_num
order by 1,2
/

ttitle 'gp_payment by calendar and stream'

select 	p.cal_run_id, s.strm_num 
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from 	ps_gp_payment p, ps_gp_strm s
where 	p.emplid between s.emplid_from and s.emplid_to
group by p.cal_run_id, s.strm_num
order by 1,2
/

ttitle 'gpgb_payment by calendar and stream'

select 	p.cal_run_id, s.strm_num 
,	min(p.emplid) min_emp 
,	max(p.emplid) max_emp 
,	count(distinct emplid) emps 
,	count(*) num_rows
from 	ps_gpgb_payment p, ps_gp_strm s
where 	p.emplid between s.emplid_from and s.emplid_to
group by p.cal_run_id, s.strm_num
order by 1,2
/

spool off

ttitle comparison

select	p.recname
,	a.num_rows PS
,	b.num_rows DMK
,	c.num_rows OLD
from	(SELECT	table_name
	,	SUBSTR(table_name,4) recname
	FROM	user_part_tables
	WHERE	table_name like 'PS_GP%'
	) p
,	(SELECT	table_name
	,	SUBSTR(table_name,4) recname
	,	num_rows
	FROM	user_tables a
	WHERE	table_name like 'PS_GP%'
	AND 	tablespace_name IS NOT NULL
	UNION
	SELECT	table_name
	,	SUBSTR(table_name,4) recname
	,	SUM(num_rows) num_rows
	FROM	user_tab_partitions a
	WHERE	table_name like 'PS_GP%'
	GROUP BY table_name
	) a
,	(SELECT	table_name
	,	SUBSTR(table_name,5) recname
	,	num_rows
	FROM	user_tables a
	WHERE	table_name like 'DMK_GP%'
	AND 	tablespace_name IS NOT NULL
	UNION
	SELECT	table_name
	,	SUBSTR(table_name,5) recname
	,	SUM(num_rows)
	FROM	user_tab_partitions a
	WHERE	table_name like 'DMK_GP%'
	GROUP BY table_name
	) b
,	(SELECT	table_name
	,	SUBSTR(table_name,5) recname
	,	num_rows
	FROM	user_tables a
	WHERE	table_name like 'OLD_GP%'
	AND 	tablespace_name IS NOT NULL
	UNION
	SELECT	table_name
	,	SUBSTR(table_name,5) recname
	,	SUM(num_rows)
	FROM	user_tab_partitions a
	WHERE	table_name like 'OLD_GP%'
	GROUP BY table_name
	) c
where	a.recname(+) = p.recname
and	b.recname(+) = p.recname
and	c.recname(+) = p.recname
and 1=2
;

ttitle offspool off
