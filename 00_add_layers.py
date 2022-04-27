import sys, os, re, string, subprocess
#from functions import *

sys.path.append('/admin/lib/py')
from ra_functions import *
import psycopg2

space = re.compile(r'\s+')

from time import time
StartTime = time()

#####################################
# Add layers to existing resultant	# 
#				Shuyan	dec 2021   #
#####################################

def add_oldforest(): 

	dmpTabv = 'res_w_old'	
	
	updLog('creating %s'%(dmpTabv), StartTime)
	cur.execute("DROP TABLE IF EXISTS %s CASCADE"%(dmpTabv))
	cur.execute("SELECT distinct exist_au, fut_au, timber_harvest_code, rec_evqo_code, rec_vac_final_va, vli_polygon_no, landscape_unit_name, age_2021, uwr_number, uwr_unit_n, mbec2006, fsw_tag, draft_fsw, unit_no, biodiversity_emphasis_option, natural_disturbance, bgc_label, spp_rep, ndz_label_1, final_pcell_name, final_licensee, esi_bdy, esi_rating, plancell_nbr, c_polyid, s_polyid, supply_blk, district_name, rollup, feature_id, basal_area, site_index, grid_code,protected_label,priority_label, old.unique_id, geom\
		INTO %s\
		FROM res_pg_model_5nov res\
		join res_5nov_w_old old\
		using(unique_id)"%(dmpTabv) )

	updLog('vacuuming', StartTime)
	cur.execute("VACUUM %s"%(dmpTabv))
	updLog('vacuuming DONE', StartTime)	
	
	# ################################add new unique_id################################
	cur.execute("ALTER TABLE %s ADD column new_id SERIAL PRIMARY KEY"%(dmpTabv))
	
	# # ##########################update area and THLB################################		
	cur.execute("ALTER TABLE %s DROP COLUMN IF EXISTS area_ha;"%(dmpTabv))
	cur.execute("ALTER TABLE %s ADD COLUMN area_ha numeric;"%(dmpTabv))	
	cur.execute("UPDATE %s set area_ha = st_area(geom)/10000;"%(dmpTabv))	
	
	cur.execute("ALTER TABLE %s DROP COLUMN IF EXISTS pha;"%(dmpTabv))
	cur.execute("ALTER TABLE %s ADD COLUMN pha numeric;"%(dmpTabv))	
	cur.execute("ALTER TABLE %s DROP COLUMN IF EXISTS nha;"%(dmpTabv))
	cur.execute("ALTER TABLE %s ADD COLUMN nha numeric;"%(dmpTabv))					
	cur.execute("UPDATE %s dmp set pha = res.pha_ratio*area_ha, nha = res.nha_ratio*area_ha\
				from (select distinct unique_id, pha/(st_area(shape)/10000) as pha_ratio, nha/(st_area(shape)/10000) as nha_ratio from res_pg_model_5nov where (st_area(shape)/10000) >0) res\
				where dmp.unique_id  = res.unique_id;"%(dmpTabv))	

	updLog('vacuuming', StartTime)
	cur.execute("VACUUM %s"%(dmpTabv))
	updLog('vacuuming DONE', StartTime)	
##############################################

tblName = 'res_pg_model_5nov'

updLog('connecting...',StartTime)
dbase = 'pg_tsa_new'
conn = psycopg2.connect(dbname = 'pg_tsa_new', host = 'aspen')

conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

###### runfrom here #################\
add_oldforest()
updLog('done',StartTime)