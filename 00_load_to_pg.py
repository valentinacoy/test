import sys, os, re, string, subprocess, shutil
sys.path.append('/admin/lib/py')
from functions import *
from ra_functions import *
space = re.compile(r'\s+')
from operator import itemgetter, attrgetter
from time import time, strftime, localtime	
from io import BytesIO
StartTime = time()
import xlrd
import get_sibec_dct2021
# import get_sibec_dct2021


##################################
#                                #
#                                #
# Multiple steps that took place #
# to create the yield files for  #
# PG TSA model                   #
#                                #
# -create dbase                  #
#                                #
# Val, May 2021                  #
##################################

def createdb():
	
	#### Creating new database for the whole TSA where the old merged resultant will be added 
	#### This is only run once to create the database. Do not rerun 
	updLog('Create database called %s  '%(dbase), StartTime)
	ogrcmd = '/usr/bin/createdb -T template_postgis %s' %dbase
	print ogrcmd
	ret = os.system(ogrcmd)
	
	updLog('done creating databse: %s'%(dbase),StartTime)
	
def uploadPSPL():    
	### Loading PSPL buffer table created by Shuyan 
	print '1:','start load'
	covPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/analysis/py/2021_update/'
	## Source name and postgres name
	covName ='pspl_buffer.shp'
	tblName = 'pspl_buffer'
	##  Check Table and base
	updLog("ADDING TBL: %s to dbase: %s"%(tblName,dbase),StartTime)
	## Check path 
	print '2:',dbase,covPth,tblName
	ogrcmd = '/usr/local/bin/ogr2ogr -f "ESRI Shapefile" PG:"dbname=%s" -lco precision=no %s%s -nln %s -overwrite -t_srs \'EPSG:3005\' --config PG_USE_COPY YES' %(dbase, covPth, covName, tblName)
	ret = os.system(ogrcmd)	 
	print dbase,covPth,covName
	print tblName
	
	cur.execute("COMMENT ON TABLE %s IS 'imported from 2021_update'" \
		%(tblName))
	print '3:','Done'	
   
def upload2020rank1():    

	### Loading 2020 rank1 layer (clipped to PG tsa)
	print '1:','start load'
	covPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/analysis/py/2021_model/'
	## Source name and postgres name
	covName ='rank12020_pg_clip.shp'
	tblName = 'rank12020_pg_clip'
	##  Check Table and base
	updLog("ADDING TBL: %s to dbase: %s"%(tblName,dbase),StartTime)
	## Check path 
	print '2:',dbase,covPth,tblName
	ogrcmd = '/usr/local/bin/ogr2ogr -f "ESRI Shapefile" PG:"dbname=%s" -lco precision=no %s%s -nln %s -overwrite -nlt MULTIPOLYGON -t_srs \'EPSG:3005\' --config PG_USE_COPY YES' %(dbase, covPth, covName, tblName)
	ret = os.system(ogrcmd)	 
	print dbase,covPth,covName
	print tblName
	
	cur.execute("COMMENT ON TABLE %s IS 'imported from %s/rank12020_pg_clip '" \
		%(tblName,covPth))
	print '3:','Done'	

def load_site_index_lookup():

### uploading res 1 lookup gdb to postgres
	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS res_v7_PEM_TEI_Omineca_tsa1_Final")
	updLog('0. Load site series lookup table for res 1: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/adding_in_sinclar_info'
	covPth = '%s/site_series_lookup_19may2021.gdb' %prjPth
	covName = 'res_v7_PEM_TEI_Omineca_tsa1_Final'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/local/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)
	
### uploading res 2 lookup gdb to postgres
	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS res_v7_PEM_TEI_Omineca_tsa2_Final")
	updLog('0. Load site series lookup table for res 2: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/adding_in_sinclar_info'
	covPth = '%s/site_series_lookup_19may2021.gdb' %prjPth
	covName = 'res_v7_PEM_TEI_Omineca_tsa2_Final'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/local/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)
	
### uploading res 3 lookup gdb to postgres
	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS res_v7_PEM_TEI_Omineca_tsa3_Final")
	updLog('0. Load site series lookup table for res 3: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/adding_in_sinclar_info'
	covPth = '%s/site_series_lookup_19may2021.gdb' %prjPth
	covName = 'res_v7_PEM_TEI_Omineca_tsa3_Final'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/local/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)


################### THESE ALL MERGE INTO THE TABLE si_lookup	
	cur.close()
	conn.close() 

def load_site_series_lkp(): 
## uploading res 1 lookup gdb to postgres
	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS pem_res1_final")
	updLog('0. Load pspl lookup table for res 1: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Site_Series_Lookup'
	covPth = '%s/Site_Series.gdb' %prjPth
	covName = 'pem_res1_final'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)
	
### uploading res 2 lookup gdb to postgres
	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS pem_res2_final")
	updLog('0. Load site series lookup table for res 2: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Site_Series_Lookup'
	covPth = '%s/Site_Series.gdb' %prjPth
	covName = 'pem_res2_final'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)
	
### uploading res 3 lookup gdb to postgres
	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS pem_res3_final")
	updLog('0. Load site series lookup table for res 3: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Site_Series_Lookup'
	covPth = '%s/Site_Series.gdb' %prjPth
	covName = 'pem_res3_final'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)

	#merge ss tables together
	updLog('Create merged pspl vri 2020 table', StartTime)
	cur.execute ("DROP TABLE IF EXISTS ss_lkp_vri_2020 CASCADE" )
	cur.execute('CREATE TABLE ss_lkp_vri_2020 AS\
				SELECT * FROM pem_res1_final\
					UNION \
				SELECT * FROM pem_res2_final\
					UNION \
				SELECT * FROM pem_res3_final')
				
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM ss_lkp_vri_2020")	


def load_aos_lkp():

### uploading res 1 lookup gdb to postgres
	# updLog('DROP TABLE', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS aos_res1_final")
	# updLog('0. Load pspl lookup table for res 1: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Spruce_Beetle_AOS_Lookup'
	# covPth = '%s/AOS_2020.gdb' %prjPth
	# covName = 'aos_res1_final'
	# updLog('gdb to postgres', StartTime)
	# print ("importing "+covPth+" "+covName+" into "+dbase)
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)
	
# ### uploading res 2 lookup gdb to postgres
	# updLog('DROP TABLE', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS aos_res2_final")
	# updLog('0. Load site series lookup table for res 2: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Spruce_Beetle_AOS_Lookup'
	# covPth = '%s/AOS_2020.gdb' %prjPth
	# covName = 'aos_res2_final'
	# updLog('gdb to postgres', StartTime)
	# print ("importing "+covPth+" "+covName+" into "+dbase)
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)
	
# ### uploading res 3 lookup gdb to postgres
	# updLog('DROP TABLE', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS aos_res3_final")
	# updLog('0. Load site series lookup table for res 3: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Spruce_Beetle_AOS_Lookup'
	# covPth = '%s/AOS_2020.gdb' %prjPth
	# covName = 'aos_res3_final'
	# updLog('gdb to postgres', StartTime)
	# print ("importing "+covPth+" "+covName+" into "+dbase)
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)
	
	#merge AOS tables together
	updLog('Create merged aos_res table', StartTime)
	cur.execute ("DROP TABLE IF EXISTS aos_res CASCADE" )
	cur.execute('CREATE TABLE aos_res AS\
				SELECT * FROM aos_res1_final\
					UNION \
				SELECT * FROM aos_res2_final\
					UNION \
				SELECT * FROM aos_res3_final')
				
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM aos_res")	
	
	
	cur.close()
	conn.close()     
	
def load_pspl_vri2020(): 

### uploading res 1 lookup gdb to postgres
	# updLog('DROP TABLE', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS vri_2020_1_si_mean")
	# updLog('0. Load pspl lookup table for res 1: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Site_Productivity_Lookup'
	# covPth = '%s/Site_Productivity.gdb' %prjPth
	# covName = 'vri_2020_1_si_mean'
	# updLog('gdb to postgres', StartTime)
	# print ("importing "+covPth+" "+covName+" into "+dbase)
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)
	
# ### uploading res 2 lookup gdb to postgres
	# updLog('DROP TABLE', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS vri_2020_2_si_mean")
	# updLog('0. Load site series lookup table for res 2: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Site_Productivity_Lookup'
	# covPth = '%s/Site_Productivity.gdb' %prjPth
	# covName = 'vri_2020_2_si_mean'
	# updLog('gdb to postgres', StartTime)
	# print ("importing "+covPth+" "+covName+" into "+dbase)
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)
	
# ### uploading res 3 lookup gdb to postgres
	# updLog('DROP TABLE', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS vri_2020_3_si_mean")
	# updLog('0. Load site series lookup table for res 3: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Site_Productivity_Lookup'
	# covPth = '%s/Site_Productivity.gdb' %prjPth
	# covName = 'vri_2020_3_si_mean'
	# updLog('gdb to postgres', StartTime)
	# print ("importing "+covPth+" "+covName+" into "+dbase)
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)

	#merge pspl tables together
	updLog('Create merged pspl vri 2020 table', StartTime)
	cur.execute ("DROP TABLE IF EXISTS pspl_vri_2020 CASCADE" )
	cur.execute('CREATE TABLE pspl_vri_2020 AS\
				SELECT * FROM vri_2020_1_si_mean\
					UNION \
				SELECT * FROM vri_2020_2_si_mean\
					UNION \
				SELECT * FROM vri_2020_3_si_mean')
				
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM pspl_vri_2020")	
	
	cur.close()
	conn.close()     
	
def upload_new_rank1():

	## load the new rank 1 summaries file #1 
	updLog('loading the new rank 1 summaries file #1', StartTime)	
	cur.execute("DROP TABLE IF EXISTS ntdn_plan_vri_06_June_2021_1")
	updLog('0. Load resultant from GIS: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless'
	covPth = '%s/Update_Summaries_June2021.gdb' %prjPth
	covName = 'ntdn_plan_vri_06_June_2021_1'
	updLog('gdb to postgres', StartTime)
	print "importing "+covPth+" "+covName+" into "+dbase
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)
	
	
	# updLog('loading the new rank 1 summaries file #2', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS ntdn_plan_vri_06_June_2021_2")
	# updLog('0. Load resultant from GIS: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless'
	# covPth = '%s/Update_Summaries_June2021.gdb' %prjPth
	# covName = 'ntdn_plan_vri_06_June_2021_2'
	# updLog('gdb to postgres', StartTime)
	# print "importing "+covPth+" "+covName+" into "+dbase
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)

	# updLog('loading the new rank 1 summaries file #3', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS ntdn_plan_vri_06_June_2021_3")
	# updLog('0. Load resultant from GIS: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless'
	# covPth = '%s/Update_Summaries_June2021.gdb' %prjPth
	# covName = 'ntdn_plan_vri_06_June_2021_3'
	# updLog('gdb to postgres', StartTime)
	# print "importing "+covPth+" "+covName+" into "+dbase
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)	

	#merge rank 1 tables together
	updLog('Create merged rank 1 table', StartTime)
	cur.execute ("DROP TABLE IF EXISTS ntdn_plan_vri_06_june_2021_merged CASCADE" )
	cur.execute('CREATE TABLE ntdn_plan_vri_06_june_2021_merged AS\
				SELECT * FROM ntdn_plan_vri_06_june_2021_1\
					UNION \
				SELECT * FROM ntdn_plan_vri_06_june_2021_2\
					UNION \
				SELECT * FROM ntdn_plan_vri_06_june_2021_3')
				
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM ntdn_plan_vri_06_june_2021_merged")	
	
	cur.close()
	conn.close()  

def load_fire_sev_lookup(): 

###### Note from Matt: Fire layer provided were cleaned and 'overlaid' with the new rank 1 
###### on the feature ID to create a lookup table. Duplicates have been removed. Only exported 
###### fires that covered >= 50% of the polygon, otherwise it might select fires that barely touch
###### the VRI polygon and have very little affect. 


	## load fire severity lt file #1 
	# updLog('loading fire severity lt file #1', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS Fires_1_LT_FINAL")
	# updLog('0. Load resultant from GIS: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless'
	# covPth = '%s/Update_Summaries_June2021.gdb' %prjPth
	# covName = 'Fires_1_LT_FINAL'
	# updLog('gdb to postgres', StartTime)
	# print "importing "+covPth+" "+covName+" into "+dbase
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)


	# # # # load fire severity lt file #2 
	updLog('loading fire severity  file #2', StartTime)	
	cur.execute("DROP TABLE IF EXISTS Fires_2_LT_FINAL")
	updLog('0. Load resultant from GIS: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless'
	covPth = '%s/Update_Summaries_June2021.gdb' %prjPth
	covName = 'Fires_2_LT_FINAL'
	updLog('gdb to postgres', StartTime)
	print "importing "+covPth+" "+covName+" into "+dbase
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)
	

	# # #  load fire severity lt file #3 
	updLog('loading fire severity  file #3', StartTime)	
	cur.execute("DROP TABLE IF EXISTS Fires_3_LT_FINAL")
	updLog('0. Load resultant from GIS: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless'
	covPth = '%s/Update_Summaries_June2021.gdb' %prjPth
	covName = 'Fires_3_LT_FINAL'
	updLog('gdb to postgres', StartTime)
	print "importing "+covPth+" "+covName+" into "+dbase
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)


	# # # #  merge fire severity tables together
	updLog('Create merge fire severity table', StartTime)
	cur.execute ("DROP TABLE IF EXISTS fires_lt_merge CASCADE" )
	cur.execute('CREATE TABLE fires_lt_merge AS\
				SELECT * FROM Fires_1_LT_FINAL\
					UNION \
				SELECT * FROM Fires_2_LT_FINAL\
					UNION \
				SELECT * FROM Fires_3_LT_FINAL')
				
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM fires_lt_merge")	
	
	cur.close()
	conn.close()  

def load_supply_block_lookup(): 

	# updLog('Loading supply block lookup table #1', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS supply_block_1_LT_NoDupes")
	# updLog('0. Load resultant from GIS: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/Update_Summaries_June2021'
	# covPth = '%s/supply_block_1_LT_NoDupes.csv' %prjPth
	# updLog('csv to postgres', StartTime)
	# print "importing "+covPth+" into "+dbase
	# ogrcmd = '/usr/bin/ogr2ogr PG:"dbname=%s" -overwrite -lco precision=no \
		# %s'  %(dbase, covPth)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)
	
	# updLog('Loading supply block lookup table #2', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS supply_blocks_2_LT_NoDupes")
	# updLog('0. Load resultant from GIS: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/Update_Summaries_June2021'
	# covPth = '%s/supply_blocks_2_LT_NoDupes.csv' %prjPth
	# updLog('csv to postgres', StartTime)
	# print "importing "+covPth+" into "+dbase
	# ogrcmd = '/usr/bin/ogr2ogr PG:"dbname=%s" -overwrite -lco precision=no \
		# %s'  %(dbase, covPth)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)
		
	# updLog('Loading supply block lookup table #3', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS supply_block_3_LT_NoDupes")
	# updLog('0. Load resultant from GIS: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/Update_Summaries_June2021'
	# covPth = '%s/supply_block_3_LT_NoDupes.csv' %prjPth
	# updLog('csv to postgres', StartTime)
	# print "importing "+covPth+" into "+dbase
	# ogrcmd = '/usr/bin/ogr2ogr PG:"dbname=%s" -overwrite -lco precision=no \
		# %s'  %(dbase, covPth)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)
	
	
	###merge lookup tables together
	# updLog('Create merged lookup table', StartTime)
	# cur.execute ("DROP TABLE IF EXISTS supply_block_lt_merged CASCADE" )
	# cur.execute('CREATE TABLE supply_block_lt_merged AS\
				# SELECT * FROM supply_block_1_LT_NoDupes\
					# UNION \
				# SELECT * FROM supply_blocks_2_LT_NoDupes\
					# UNION \
				# SELECT * FROM supply_block_3_LT_NoDupes')
				
	# updLog('vacuuming', StartTime)
	# cur.execute("VACUUM supply_block_lt_merged")	
	
			   
	cur.close()
	conn.close()	
	
def load_sept13_res(): 

	########## GIS created a resultant per Valentina's request 
	########## to include the following layers that were missed in the reallocation resultant 
	########## landscape unit, ndu subunit, biodiversity emphasis option, NDT, proposed FSW 
	
	
	# # # # # load sept13 resultant #1 
	# # updLog('loading sept13 resultant #1', StartTime)	
	# # cur.execute("DROP TABLE IF EXISTS res_1_13sept2021 ")
	# # updLog('0. Load resultant from GIS: ',StartTime)
	# # prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/overlays'
	# # covPth = '%s/Resultants.gdb' %prjPth
	# # covName = 'res_1_13sept2021 '
	# # updLog('gdb to postgres', StartTime)
	# # print "importing "+covPth+" "+covName+" into "+dbase
	# # ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# # %s %s'  %(dbase, covPth, covName)
	# # ret = os.system(ogrcmd)
	# # updLog('done', StartTime)


	# # # load sept13 resultant #2 
	# updLog('load sept13 resultant #2 ', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS res_2_13sept2021 ")
	# updLog('0. Load resultant from GIS: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/overlays'
	# covPth = '%s/Resultants.gdb' %prjPth
	# covName = 'res_2_13sept2021 '
	# updLog('gdb to postgres', StartTime)
	# print "importing "+covPth+" "+covName+" into "+dbase
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)
	

	# # # # # # # load sept13 resultant #3 
	# # updLog('load sept13 resultant #3 ', StartTime)	
	# # cur.execute("DROP TABLE IF EXISTS res_3_13sept2021 ")
	# # updLog('0. Load resultant from GIS: ',StartTime)
	# # prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/overlays'
	# # covPth = '%s/Resultants.gdb' %prjPth
	# # covName = 'res_3_13sept2021 '
	# # updLog('gdb to postgres', StartTime)
	# # print "importing "+covPth+" "+covName+" into "+dbase
	# # ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# # %s %s'  %(dbase, covPth, covName)
	# # ret = os.system(ogrcmd)
	# # updLog('done', StartTime)

	###merge resultants
	updLog('Merge resultants together', StartTime)
	cur.execute ("DROP TABLE IF EXISTS res_pg_model_13sept CASCADE" )
	cur.execute('CREATE TABLE res_pg_model_13sept AS\
				SELECT * FROM res_1_13sept2021\
					UNION \
				SELECT * FROM res_2_13sept2021\
					UNION \
				SELECT * FROM res_3_13sept2021')
				
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM res_pg_model_13sept")	

def create_res_ver_tol_13sept2021():
	##removed org_fid
	
	updLog('Create table %s'%(ver_tol), StartTime)
	cur.execute ("DROP TABLE IF EXISTS %s CASCADE" %(ver_tol))
	cur.execute('CREATE TABLE %s AS\
				SELECT objectid, a25_retent, area_type, c_polyid, s_polyid, herd_name, bgc_label, licence, cp, cutblock, district_name, org_unit, elev_1520m, esa1, esi_bdy, ndu, subunit, region, final_pcell_name, final_district_code, final_supply_block, final_licensee, ches_carrier, saikuz, stellaten, echiniko, fsw_tag, unit_no, wildlife_h, harvest, approval_d, common_spe,  herd_stat, ecotype, is_mill, grid_code, herrick_creek, llheidli, lheidli, licensee_v2, logmaprvid, lumpndu, ogma_nlg, own, prot_name, plancell_nbr, ung_wntr_rng_proposed_id, timber_harvest_code, rec_status, rec_label, sra, supply_blk, timber_typ, nrfl_a93061_block_id, a4_block_id, a4_status, esi_rating, a4_addition, uwr_number, uwr_unit_n, uwr_number2, harv1, harv2, wha_tag, buf_dist, wma_name,  vli_polygon_no, rec_vac_final_va, rec_evqo_code, ada, tsa_number_description, pipe_power, update_roads, feature_id, bclcs_level_1, bclcs_level_2, bclcs_level_3, bclcs_level_4, bclcs_level_5, reference_year, projected_date, harvest_date, crown_closure, site_index, basal_area, vri_live_stems_per_ha, vri_dead_stems_per_ha, species_cd_1, species_pct_1, species_cd_2, species_pct_2, species_cd_3, species_pct_3, species_cd_4, species_pct_4, species_cd_5, species_pct_5, species_cd_6, species_pct_6, proj_age_1, proj_height_1, live_stand_volume_125, live_stand_volume_175, dead_stand_volume_125, dead_stand_volume_175, ctheme as ctheme_old, gha as old_gha, pha as old_pha, nha as old_nha, rsv_rm as old_rsv_rm, total_fire, burn_sevr_, block_id_2020, licence_2020, permit_2020, reserve_2020, licencee_2020, year_2020, license_2021, block_id_2021, permit_2021, year_2021, licensee_2021, block_stat_2021, reserve_2021, log_year, log_src, log_year_p, orig_fid_12, road_748, glob_unid_1 as old_glob_unid, landscape_unit_name, biodiversity_emphasis_option, natural_disturbance, ndz_name, ndz_subunit, ndz_name_1,ndz_label, draft_fsw, fsw_stat, gaze_name, unit_no_1, fsw_tags, shape_length, shape_area, glob_unid_2 as glob_unid_1,shape\
					FROM %s\
					UNION\
					SELECT objectid, a25_retent, area_type, c_polyid, s_polyid, herd_name, bgc_label, licence, cp, cutblock, district_name, org_unit, elev_1520m, esa1, esi_bdy, ndu, subunit, region, final_pcell_name, final_district_code, final_supply_block, final_licensee, ches_carrier, saikuz, stellaten, echiniko, fsw_tag, unit_no, wildlife_h, harvest, approval_d, common_spe, herd_stat, ecotype, is_mill, grid_code, herrick_creek, llheidli, lheidli, licensee_v2, logmaprvid, lumpndu, ogma_nlg, own, prot_name, plancell_nbr, ung_wntr_rng_proposed_id, timber_harvest_code, rec_status, rec_label, sra, supply_blk, timber_typ, nrfl_a93061_block_id, a4_block_id, a4_status, esi_rating, a4_addition, uwr_number, uwr_unit_n, uwr_number2, harv1, harv2, wha_tag, buf_dist, wma_name, vli_polygon_no, rec_vac_final_va, rec_evqo_code, ada, tsa_number_description, pipe_power, update_roads, feature_id, bclcs_level_1, bclcs_level_2, bclcs_level_3, bclcs_level_4, bclcs_level_5, reference_year, projected_date, harvest_date, crown_closure, site_index, basal_area, vri_live_stems_per_ha, vri_dead_stems_per_ha, species_cd_1, species_pct_1, species_cd_2, species_pct_2, species_cd_3, species_pct_3, species_cd_4, species_pct_4, species_cd_5, species_pct_5, species_cd_6, species_pct_6, proj_age_1, proj_height_1, live_stand_volume_125, live_stand_volume_175, dead_stand_volume_125, dead_stand_volume_175, ctheme as ctheme_old, gha as old_gha, pha as old_pha, nha as old_nha, rsv_rm as old_rsv_rm, total_fire, burn_sevr_, block_id_2020, licence_2020, permit_2020, reserve_2020, licencee_2020, year_2020, license_2021, block_id_2021, permit_2021, year_2021, licensee_2021, block_stat_2021, reserve_2021, log_year, log_src, log_year_p, orig_fid_12, road_748, glob_unid_2 as old_glob_unid, landscape_unit_name, biodiversity_emphasis_option, natural_disturbance, ndz_name, ndz_subunit, ndz_name_1, ndz_label, draft_fsw, fsw_stat, gaze_name, unit_no_1, fsw_tags, shape_length, shape_area, glob_unid_2_2 as glob_unid_1, shape \
						FROM %s \
						UNION\
						SELECT objectid, a25_retent, area_type, c_polyid, s_polyid, herd_name, bgc_label, licence, cp, cutblock, district_name, org_unit, elev_1520m, esa1, esi_bdy, ndu, subunit, region, final_pcell_name, final_district_code, final_supply_block, final_licensee, ches_carrier, saikuz, stellaten, echiniko, fsw_tag, unit_no, wildlife_h, harvest, approval_d, common_spe, herd_stat, ecotype, is_mill, grid_code, herrick_creek, llheidli, lheidli, licensee_v2, logmaprvid, lumpndu, ogma_nlg, own, prot_name, plancell_nbr, ung_wntr_rng_proposed_id, timber_harvest_code, rec_status, rec_label, sra, supply_blk, timber_typ, nrfl_a93061_block_id, a4_block_id, a4_status, esi_rating, a4_addition, uwr_number, uwr_unit_n, uwr_number2, harv1, harv2, wha_tag, buf_dist, wma_name, vli_polygon_no, rec_vac_final_va, rec_evqo_code, ada, tsa_number_description, pipe_power, update_roads, feature_id, bclcs_level_1, bclcs_level_2, bclcs_level_3, bclcs_level_4, bclcs_level_5, reference_year, projected_date, harvest_date, crown_closure, site_index, basal_area, vri_live_stems_per_ha, vri_dead_stems_per_ha, species_cd_1, species_pct_1, species_cd_2, species_pct_2, species_cd_3, species_pct_3, species_cd_4, species_pct_4, species_cd_5, species_pct_5, species_cd_6, species_pct_6, proj_age_1, proj_height_1, live_stand_volume_125, live_stand_volume_175, dead_stand_volume_125, dead_stand_volume_175, ctheme as ctheme_old, gha as old_gha, pha as old_pha, nha as old_nha, rsv_rm as old_rsv_rm,  total_fire, burn_sevr_, block_id_2020, licence_2020, permit_2020, reserve_2020, licencee_2020, year_2020, license_2021, block_id_2021, permit_2021, year_2021, licensee_2021, block_stat_2021, reserve_2021, log_year, log_src, log_year_p, orig_fid_12,road_748, glob_unid_3 as old_glob_unid, landscape_unit_name, biodiversity_emphasis_option, natural_disturbance, ndz_name, ndz_subunit, ndz_name_1, ndz_label, draft_fsw, fsw_stat, gaze_name, unit_no_1, fsw_tags, shape_length, shape_area, glob_unid_33 as glob_unid_1, shape\
						FROM %s'%(ver_tol,ver1,ver2,ver3))		
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM %s"%(ver_tol))	
	
	updLog('Comment on table %s'%(ver_tol), StartTime)
	cur.execute("COMMENT ON TABLE %s IS 'CMM from ntdn v18 and ntdn  Total PG TSA with new blocks and constraints from ntdn table and only pha'"%ver_tol )
	
	updLog('Primary Key %s  is glob_unid_1'%(ver_tol), StartTime)
	updLog('Must have PRIMARY KEY ON %s'%ver_tol, StartTime)
	cur.execute("ALTER TABLE %s  DROP CONSTRAINT IF EXISTS  %s_pk;"%(ver_tol,ver_tol))
	cur.execute("ALTER TABLE %s ADD CONSTRAINT %s_pk PRIMARY KEY (glob_unid_1);"%(ver_tol, ver_tol))	
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM %s"%(ver_tol))	


def create_licensee_lookup():
	##removed org_fid
	
	updLog('Create table %s'%(licensee_lkp), StartTime)
	cur.execute ("DROP TABLE IF EXISTS %s CASCADE" %(licensee_lkp))
	cur.execute('CREATE TABLE %s AS\
				SELECT glob_unid_2 as glob_unid_1, plancell_nbr,final_licensee \
					FROM %s\
					UNION\
					SELECT glob_unid_2_2 as glob_unid_1, plancell_nbr,final_licensee  \
						FROM %s \
						UNION\
						SELECT glob_unid_33 as glob_unid_1, plancell_nbr,final_licensee \
						FROM %s'%(licensee_lkp,ver1,ver2,ver3))		
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM %s"%(licensee_lkp))	
	
	updLog('Comment on table %s'%(licensee_lkp), StartTime)
	cur.execute("COMMENT ON TABLE %s IS 'CMM from ntdn v18 and ntdn  Total PG TSA with new blocks and constraints from ntdn table and only pha'"%licensee_lkp )
	
	updLog('Primary Key %s  is glob_unid_1'%(licensee_lkp), StartTime)
	updLog('Must have PRIMARY KEY ON %s'%licensee_lkp, StartTime)
	cur.execute("ALTER TABLE %s  DROP CONSTRAINT IF EXISTS  %s_pk;"%(licensee_lkp,licensee_lkp))
	cur.execute("ALTER TABLE %s ADD CONSTRAINT %s_pk PRIMARY KEY (glob_unid_1);"%(licensee_lkp, licensee_lkp))	
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM %s"%(licensee_lkp))	

def load_vdyp_input_2020vri(): 
## uploading VDYP INPUT POLY 
	# updLog('DROP TABLE', StartTime)	
	# cur.execute("DROP TABLE IF EXISTS VEG_COMP_VDYP7_INPUT_POLY")
	# updLog('0. Load vdyp input layer- poly: ',StartTime)
	# prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/source/VEG_COMP_VDYP7_INPUT_POLY_AND_LAYER_2020.gdb'
	# covPth = '%s/VEG_COMP_VDYP7_INPUT_POLY_AND_LAYER_2020.gdb' %prjPth
	# covName = 'VEG_COMP_VDYP7_INPUT_POLY'
	# updLog('gdb to postgres', StartTime)
	# print ("importing "+covPth+" "+covName+" into "+dbase)
	# ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# %s %s'  %(dbase, covPth, covName)
	# ret = os.system(ogrcmd)
	# updLog('done', StartTime)    


## uploading VDYP INPUT LAYER 
	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS veg_comp_vdyp7_input_layer")
	updLog('0. Load vdyp input layer- layer: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/source/VEG_COMP_VDYP7_INPUT_POLY_AND_LAYER_2020.gdb'
	covPth = '%s/VEG_COMP_VDYP7_INPUT_POLY_AND_LAYER_2020.gdb' %prjPth
	covName = 'veg_comp_vdyp7_input_layer'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)    

def load_detailed_schedule(): 

	##### s0j
	updLog('Loading detailed schedule', StartTime)	
	cur.execute("DROP TABLE IF EXISTS detailedSchedule_cc_s0g")
	updLog('0. Load resultant from GIS: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/analysis/pw/runs/s0j/reports/reports'
	covPth = '%s/detailedSchedule_cc.csv' %prjPth
	updLog('csv to postgres', StartTime)
	print "importing "+covPth+" into "+dbase
	ogrcmd = '/usr/bin/ogr2ogr PG:"dbname=%s" -overwrite -lco precision=no \
		%s'  %(dbase, covPth)
	ret = os.system(ogrcmd)
	cur.execute("ALTER TABLE detailedSchedule_cc RENAME TO detailedSchedule_cc_s0j")
	updLog('done', StartTime)
	

	###### s0k
	updLog('Loading detailed schedule', StartTime)	
	cur.execute("DROP TABLE IF EXISTS detailedSchedule_cc_s0k")
	updLog('0. Load resultant from GIS: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/analysis/pw/runs/s0k/reports'
	covPth = '%s/detailedSchedule_cc.csv' %prjPth
	updLog('csv to postgres', StartTime)
	print "importing "+covPth+" into "+dbase
	ogrcmd = '/usr/bin/ogr2ogr PG:"dbname=%s" -overwrite -lco precision=no \
		%s'  %(dbase, covPth)
	ret = os.system(ogrcmd)
	cur.execute("ALTER TABLE detailedSchedule_cc RENAME TO detailedSchedule_cc_s0k")
	updLog('done', StartTime)

def load_old_growth_layers(): 
	
# # ## uploading OLD priority OG 
	# # updLog('DROP TABLE', StartTime)	
	# # cur.execute("DROP TABLE IF EXISTS Map1_PriorityDeferral_final")
	# # updLog('0. Load OLD old growth layer  ',StartTime)
	# # prjPth = '/projects/basedata/Old_Growth_Deferral_Spatial_Data_Nov2021/Spatial_Data/Shapefile_Data/Shapefile'
	# # covPth = '%s/shapefiles_w_TSA.gdb' %prjPth
	# # covName = 'Map1_PriorityDeferral_final'
	# # updLog('gdb to postgres', StartTime)
	# # print ("importing "+covPth+" "+covName+" into "+dbase)
	# # ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# # %s %s'  %(dbase, covPth, covName)
	# # ret = os.system(ogrcmd)
	# # cur.execute("ALTER TABLE Map1_PriorityDeferral_final RENAME TO priority_og_old")
	# # updLog('done', StartTime) 	

# # ## uploading NEW priority OG 
	# # updLog('DROP TABLE', StartTime)	
	# # cur.execute("DROP TABLE IF EXISTS PriorityDeferral")
	# # updLog('0. Load OLD old growth layer  ',StartTime)
	# # prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/web_mapping_tool'
	# # covPth = '%s/PG_Realloaction_WebMap_Data.gdb' %prjPth
	# # covName = 'PriorityDeferral'
	# # updLog('gdb to postgres', StartTime)
	# # print ("importing "+covPth+" "+covName+" into "+dbase)
	# # ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		# # %s %s'  %(dbase, covPth, covName)
	# # ret = os.system(ogrcmd)
	# # cur.execute("ALTER TABLE PriorityDeferral RENAME TO priority_og_new")
	# # updLog('done', StartTime) 	


## uploading OLD protected OG 
	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS ProtectedForest_final")
	updLog('0. Load OLD old growth layer  ',StartTime)
	prjPth = '/projects/basedata/Old_Growth_Deferral_Spatial_Data_Nov2021/Spatial_Data/Shapefile_Data/Shapefile'
	covPth = '%s/shapefiles_w_TSA.gdb' %prjPth
	covName = 'ProtectedForest_final'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	cur.execute("ALTER TABLE ProtectedForest_final RENAME TO protected_og_old")
	updLog('done', StartTime) 	

## uploading NEW protected OG 
	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS Protected")
	updLog('0. Load OLD old growth layer  ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/web_mapping_tool'
	covPth = '%s/PG_Realloaction_WebMap_Data.gdb' %prjPth
	covName = 'Protected'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	cur.execute("ALTER TABLE Protected RENAME TO protected_og_new")
	updLog('done', StartTime) 	

def load_prof_table(): 

	# cur.execute("DROP TABLE IF EXISTS pg_tsa_model_prof_2feb2022 CASCADE")
	# csv2pg(conn, '/projects/canfor/ke14033cfp_provincial_fibre_flow/analysis/r/pg_tsa_model_prof_2feb2022.csv'  , tbl = None, pk = None, header = True)

	updLog('Loading supply block lookup table #1', StartTime)	
	cur.execute("DROP TABLE IF EXISTS pg_tsa_model_prof_2feb2022")
	updLog('0. Load resultant from GIS: ',StartTime)
	prjPth = '/projects/canfor/ke14033cfp_provincial_fibre_flow/analysis/r'
	covPth = '%s/pg_tsa_model_prof_2feb2022.csv' %prjPth
	updLog('csv to postgres', StartTime)
	print "importing "+covPth+" into "+dbase
	ogrcmd = '/usr/bin/ogr2ogr PG:"dbname=%s" -overwrite -lco precision=no \
		%s'  %(dbase, covPth)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)


def load_tsa24_careas(): 

	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS candidate_areas_final")
	updLog('0. Load OLD old growth layer  ',StartTime)
	prjPth = '/projects/canfor/140033_cfp_n2e_analysis/gis/seamless'
	covPth = '%s/tsa24_candidate_areas.gdb' %prjPth
	covName = 'candidate_areas_final'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime) 	


def load_first_nations_lookup():

	updLog('Loading FN lookup table #1', StartTime)	
	cur.execute("DROP TABLE IF EXISTS res_1_15july2021_FN")
	updLog('0. Load resultant from GIS: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Resultant_Join_FIrst_Nations_Territories'
	covPth = '%s/res_1_15july2021_FN.csv' %prjPth
	updLog('csv to postgres', StartTime)
	print "importing "+covPth+" into "+dbase
	ogrcmd = '/usr/bin/ogr2ogr PG:"dbname=%s" -overwrite -lco precision=no \
		%s'  %(dbase, covPth)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)	


	updLog('Loading FN lookup table #2', StartTime)	
	cur.execute("DROP TABLE IF EXISTS res_2_15july2021_FN")
	updLog('0. Load resultant from GIS: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Resultant_Join_FIrst_Nations_Territories'
	covPth = '%s/res_2_15july2021_FN.csv' %prjPth
	updLog('csv to postgres', StartTime)
	print "importing "+covPth+" into "+dbase
	ogrcmd = '/usr/bin/ogr2ogr PG:"dbname=%s" -overwrite -lco precision=no \
		%s'  %(dbase, covPth)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)	


	updLog('Loading FN lookup table #3', StartTime)	
	cur.execute("DROP TABLE IF EXISTS res_3_15july2021_FN")
	updLog('0. Load resultant from GIS: ',StartTime)
	prjPth = '/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/gis/seamless/2021_PG_TSA_Rebuild/seamless/Resultant_Join_FIrst_Nations_Territories'
	covPth = '%s/res_3_15july2021_FN.csv' %prjPth
	updLog('csv to postgres', StartTime)
	print "importing "+covPth+" into "+dbase
	ogrcmd = '/usr/bin/ogr2ogr PG:"dbname=%s" -overwrite -lco precision=no \
		%s'  %(dbase, covPth)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)	


	#merge ss tables together
	updLog('Create merged fn lookup', StartTime)
	cur.execute ("DROP TABLE IF EXISTS res_15july2021_FN CASCADE" )
	cur.execute('CREATE TABLE res_15july2021_FN AS\
				SELECT * FROM res_1_15july2021_FN\
					UNION \
				SELECT * FROM res_2_15july2021_FN\
					UNION \
				SELECT * FROM res_3_15july2021_FN')
				
	updLog('vacuuming', StartTime)
	cur.execute("VACUUM res_15july2021_FN")	

def load_vdyp_QA_pspl(): 

	updLog('Loading vdyp QA pspl lookup for entire BC', StartTime)	
	cur.execute("DROP TABLE IF EXISTS pspl_lookup_bc")
	prjPth = '/projects/resource_inventory/vri2020/py/VDYP_docs/VRI_checks'
	covPth = '%s/pspl_lookup_bc.csv' %prjPth
	updLog('csv to postgres', StartTime)
	print "importing "+covPth+" into "+dbase
	ogrcmd = '/usr/bin/ogr2ogr PG:"dbname=%s" -overwrite -lco precision=no \
		%s'  %(dbase, covPth)
	ret = os.system(ogrcmd)
	updLog('done', StartTime)
	
def load_chinook_inv_data(): 
	updLog('DROP TABLE', StartTime)	
	cur.execute("DROP TABLE IF EXISTS Tesera_Inventory_06nov2020")
	updLog('0. Load chinook inventory 06 nov 2020  ',StartTime)
	prjPth = '/projects/chinook_comfor/fg_19_518_ccf_chinookcf_timber_supply/gis/src/from_Tesera_6nov2020'
	covPth = '%s/HRIS_hris_v111_20201106_ChinookCF_prj.gdb' %prjPth
	covName = 'Tesera_Inventory_06nov2020'
	updLog('gdb to postgres', StartTime)
	print ("importing "+covPth+" "+covName+" into "+dbase)
	ogrcmd = '/usr/bin/ogr2ogr -f "fileGDB" PG:"dbname=%s" -overwrite  -nlt MULTIPOLYGON -lco precision=no \
		%s %s'  %(dbase, covPth, covName)
	ret = os.system(ogrcmd)
	updLog('done', StartTime) 	

	
	# cur.execute("COMMENT ON TABLE %s IS 'Imported from %s\%s on %s'" \
	# %(tblName, covPth, covName, strftime("%Y/%m/%d %H:%M:%S", localtime()))) 

	# updLog('Adding primary key ', StartTime)
	# cur.execute("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s_pk;" %(tblName, tblName))
	# cur.execute("ALTER TABLE %s ADD CONSTRAINT %s_pk PRIMARY KEY (unique_id);" %(tblName, tblName))

	# updLog("vacuum table %s " %(tblName))
	# cur.execute("vacuum %s " %(tblName))
		
	
######################
tblName = 'res_total_feb2021'
tblName = 'ntdn_plan_vri_06_june_2021_merged'  ##new vri 
dbase = 'pg_tsa_new'
# dbase = 'chinookcf'
host = 'aspen'

# licensee_lkp = 'res_pg_model_13sept_plancell'
# ver_tol = 'res_pg_model_13sept'
# ver1= 'res_1_13sept2021'
# ver2= 'res_2_13sept2021'
# ver3= 'res_3_13sept2021'
	
try:
	conn = psycopg2.connect(dbname=dbase, host=host)  
	conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)          
except Exception, exc:
	print "connection problem", 
	print "%s: %s" % (exc.__class__.__name__, exc)
	raise


cur=conn.cursor(cursor_factory=psycopg2.extras.DictCursor)    
StartTime = time()

load_chinook_inv_data()


# createdb() 
# uploadPSPL()
# upload2020rank1()
# upload_new_rank1() 
# load_supply_block_lookup()
# load_site_index_lookup ()
# load_fire_sev_lookup()
# load_pspl_vri2020()
# load_site_series_lkp()
# load_aos_lkp()
# load_sept13_res()
# create_res_ver_tol_13sept2021()
# create_licensee_lookup()
# load_vdyp_input_2020vri()
# load_detailed_schedule()
# load_old_growth_layers()
# load_prof_table()
# load_tsa24_careas() 
# load_first_nations_lookup()
# load_vdyp_QA_pspl()

