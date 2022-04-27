import sys, os, re, string, subprocess, shutil
sys.path.append('/admin/lib/py')
from functions import *
from ra_functions import *
space = re.compile(r'\s+')
from operator import itemgetter, attrgetter
from time import time, strftime, localtime	
from io import BytesIO
StartTime = time()

##################################
#                                #
#                                #
# Code to create rmz table       #
# with model contraints          #
#                                #
# Val, May 2021                  #
##################################


def calc_rmz():

	rmzDCT = {}
	count=0
	rowcnt = cur.rowcount
	
	### when running the 16 dec version use 
	groups = open(folder + '/groups_newbase.csv','w')
	# groups = open(folder + '/groups_21dec.csv','w')
	# groups = open(folder + '/groups_16dec.csv','w')
	## when running the 5nov version change the  to new_id
	# groups = open(folder + '/groups_5nov.csv','w')
	# groups = open(folder + '/groups_26oct.csv','w')
	# groups = open(folder + '/groups_base.csv','w')
	# groups = open(folder + '/groups_dmp.csv','w')    

	groups.write('BLOCK,GROUP\n')
	datacheck = open(folder + '/datacheck.csv','w')
	cur.execute ("DROP TABLE IF EXISTS pw_rmz_%s"%(ver)) 
	cur.execute('CREATE TABLE pw_rmz_%s(\
		  nha numeric,\
		  pha numeric,\
		  maxmin character varying,\
		  per numeric,\
		  constr_type character varying,\
		  val numeric,\
		  grp character varying,\
		  "number" character varying,\
		  zone character varying PRIMARY KEY\
		  );'%(ver))
		  
	##VQO_VAC: % alteration in perspective view	  
	### PR obtained from assumptions document, the rest from PG TSA DP
	#### Set retention VQO to 0 as per pg tsa CMM assumptions doc 
	#### Ran another RMZ version with the DP retention assumptions 
	
	VQO = {		
		'R_L' : 0 ,
		'R_M' : 0.75, ## 0 # we had set R_M to 0 previously following CMM assumptions
		'R_H' : 1.5, ## 0 # we had set R_M to 0 previously following CMM assumptions
		'PR_L' : 1.6,	
		'PR_M' : 4.3,	
		'PR_H' : 7.0,
		'M_L'  : 7.1,
		'M_M'  : 12.6,
		'M_H'  : 18.0,
		'MM_L' : 18.1,
		'MM_M' : 24.1,
		'MM_H' : 30.0,
		}
	updLog('VQO',StartTime)	
		  
	
	AV_SLP={}
	updLog('Fetching VQO Slope',StartTime)
	##av_slope must be multiplied by its percentage slope. I.e if slope class is 10%, then multiply by 10, or if 5% then multiply grid_code by 5.
	
	
	# # # # # # # # # # run with big resultant 
	cur.execute("SELECT vli_polygon_no,rec_evqo_code, rec_vac_final_va, \
		((sum(grid_code*pha) / sum(pha))*10) as av_slope \
		FROM %s WHERE pha > 0 and vli_polygon_no > 0 and rec_evqo_code <> '' and rec_vac_final_va <> '' \
		GROUP BY 1,2,3 ORDER BY 1,2,3;" %tblName)	


	for rVQO in cur.fetchall(): 
		AV_SLP[ (rVQO['vli_polygon_no'], rVQO['rec_evqo_code'], rVQO['rec_vac_final_va']) ] = rVQO['av_slope']
		
	updLog('RMZ',StartTime)	
	updLog('Fetching Data',StartTime)

# # # # # # # # Have been modifying this query as we use more updated resultants. This is for the res_w_old including old layers
	cur.execute("SELECT new_id, timber_harvest_code, rec_evqo_code, rec_vac_final_va, vli_polygon_no, landscape_unit_name, \
	age_2021, uwr_number, uwr_unit_n, basal_area, mbec2006, fsw_tag, final_licensee, draft_fsw, unit_no, biodiversity_emphasis_option, district_name, \
	natural_disturbance, bgc_label, supply_blk,label as protected_label, tap_classification_label as priority_label, candidate_ as candidate_areas,\
	spp_rep, ndz_label_1, pha::numeric, pha - nha as rha, nha::numeric\
	FROM %s where pha > 0 and feature_id not in ('0') and (exist_au_fix like 'exist%%' or site_index not in ('0')) ;" %(tblName))	
   
   
	rowcnt = cur.rowcount
	updLog("calculating rmz and writing group file",StartTime)
	
	for r in ResultIter(cur,10000) :
		
		if round(r['nha'],6) == 0 : r['nha'] = 0
		if round(r['pha'],6) == 0 : r['pha'] = 0
		# if round(r['rha'],6) == 0 : r['rha'] = 0

		clas = 'X'
		if r['nha'] > 0: clas = 'H'
		elif r['pha'] > 0: clas = 'C'
		nha =  r['nha']
		pha = r['pha']
		if clas <> 'X':




			######################################
			######################################
			######################################
			###       VQO    ##################### 
			######################################
			######################################
			######################################

			grpName = 'vqo'
			
			#####these fields have been changed to pg tsa but need to check structure
			if (r['rec_evqo_code'] not in ('',' ') and r['rec_vac_final_va'] not in ('',' ')) and ('_'.join([r['rec_evqo_code'], r['rec_vac_final_va']]) in VQO):
		
				nm = '_'.join([r['rec_evqo_code'], r['rec_vac_final_va'], str(r['vli_polygon_no'])])
				rmzName = '.'.join([grpName, nm])
				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				constr_type, maxmin, per, val = ('ht_lt','max', 100, 0)
				per = VQO['_'.join([r['rec_evqo_code'], r['rec_vac_final_va']])]
				av_slope = AV_SLP[(r['vli_polygon_no'], r['rec_evqo_code'], r['rec_vac_final_va'])]
				veg_ht, p2p_ratio = avSLP(av_slope)
				constr_type, maxmin, per, val = ('ht_lt','max',int(per * p2p_ratio), veg_ht)
				# print 'vqo:',av_slope,constr_type, maxmin, per, val 
				zone = rmzName +'.'+ '_'.join([nm, maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': 'vqo', 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}
			
			

			######################################
			### MPB Shelf Life ################### 
			######################################
			##### Added a 10 yr linear decline see 02a code 
						
			
			
			###############################################
			###############################################
			###############################################
			##### Landscape Level Objectives ##############
			##### Locked and Surplus Merged BEC ###########
			###############################################
			###############################################
			###############################################
			### Line up with target 
			grpName = ''
			rmzName = ''
			grpName = 'oldseral'
			nm = ''
			zone = ''
			bec = r['mbec2006']
			age = r['age_2021']
			bgc = r['bgc_label']
			

			if bec in ('D2','D3','D5','E2','E3','A7','A10'):
				#### merged becs where old growth is considered >120 yrs as per 2004 biodiversity order 
				
				nm = bec 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 17, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}




			elif bec in ('D4','D6','D7','E4','E5','A8','A9','A11','A12','A13','A3'):
				#### merged becs where old growth is considered >120 yrs as per 2004 biodiversity order 
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 12, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha} 
						
						


			elif bec in ('E12','E14','E15','E16','E17'):
				#### merged becs where old growth is considered >120 yrs as per 2004 biodiversity order 
				
				nm = bec   
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 16, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}
						

		  


			elif bec in ('D1','A5','A6'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 29, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}

		  


			elif bec in ('E1','E10','E11'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 41, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}
						
						



			elif bec in ('E6','E7'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 37, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}
						
						
					


			elif bec in ('A2','A4','A16','E8'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 26, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}                        



			elif bec in ('E9'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 58, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}  



			# grpName = ''
			# rmzName = ''
			# grpName = 'oldseral'
			# nm = ''
			# zone = ''
			# bec = r['mbec2006']
			# age = r['age_2021']
			# bgc = r['bgc_label']


			elif bec in ('E13'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 23, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}    


			# grpName = ''
			# rmzName = ''
			# grpName = 'oldseral'
			# nm = ''
			# zone = ''
			# bec = r['mbec2006']
			# age = r['age_2021']
			# bgc = r['bgc_label']


			elif bec in ('A1'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 33, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}                        
						
			
			# grpName = ''
			# rmzName = ''
			# grpName = 'oldseral'
			# nm = ''
			# zone = ''
			# bec = r['mbec2006']
			# age = r['age_2021']
			# bgc = r['bgc_label']


			elif bec in ('A14','A17'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 50, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha} 



			# grpName = ''
			# rmzName = ''
			# grpName = 'oldseral'
			# nm = ''
			# zone = ''
			# bec = r['mbec2006']
			# age = r['age_2021']
			# bgc = r['bgc_label']


			elif bec in ('A15'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec   
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 84, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha} 
						

			# grpName = ''
			# rmzName = ''
			# grpName = 'oldseral'
			# nm = ''
			# zone = ''
			# bec = r['mbec2006']
			# age = r['age_2021']
			# bgc = r['bgc_label']


			elif bec in ('A18','A20'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 80, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}


			# grpName = ''
			# rmzName = ''
			# grpName = 'oldseral'
			# nm = ''
			# zone = ''
			# bec = r['mbec2006']
			# age = r['age_2021']
			# bgc = r['bgc_label']


			elif bec in ('A19','A21'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 48, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}



			# grpName = ''
			# rmzName = ''
			# grpName = 'oldseral'
			# nm = ''
			# zone = ''
			# bec = r['mbec2006']
			# age = r['age_2021']
			# bgc = r['bgc_label']


			elif bec in ('A22','A23'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 53, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}


			# grpName = ''
			# rmzName = ''
			# grpName = 'oldseral'
			# nm = ''
			# zone = ''
			# bec = r['mbec2006']
			# age = r['age_2021']
			# bgc = r['bgc_label']


			elif bec in ('A24'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 30, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}


			# grpName = ''
			# rmzName = ''
			# grpName = 'oldseral'
			# nm = ''
			# zone = ''
			# bec = r['mbec2006']
			# age = r['age_2021']
			# bgc = r['bgc_label']


			elif bec in ('A25'):
				#### merged becs where old growth is considered >140 yrs as per 2004 biodiversity order 
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 46, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}                        

			###############################################			
			###############################################			
			###############################################
			### Old Seral Non-Pine  ####
			###############################################
			###############################################
			###############################################


			grpName = ''
			rmzName = ''
			grpName = 'oldseral_nonpine'
			nm = ''
			zone = ''
			bec = r['mbec2006']
			age = r['age_2021']
			bgc = r['bgc_label']
			species = r['spp_rep']

			if bec in ('A5') and species not in ('PL'):
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 12, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}  

			
			elif bec in ('A6') and species not in ('PL'):
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 28, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}  

			elif bec in ('A7','A10') and species not in ('PL'):
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 14, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}   

			elif bec in ('A8') and species not in ('PL'):
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 1, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}                        


			elif bec in ('A9','D2','D6') and species not in ('PL'):
				
				nm = bec 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 3, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}												

			
			elif bec in ('A11','D4','D7') and species not in ('PL'):
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 2, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}	

			
			elif bec in ('A12','E4') and species not in ('PL'):
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 4, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}	

			elif bec in ('A13','E5') and species not in ('PL'):
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 6, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}	

			elif bec in ('E3','E14','E16') and species not in ('PL'):
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 10, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}	

			elif bec in ('E2','E15') and species not in ('PL'):
				
				nm = bec 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 13, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}	

			elif bec in ('E1') and species not in ('PL'):
				
				nm = bec  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 33, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}	
						
			elif bec in ('D3','D5') and species not in ('PL'):
				
				nm = bec 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 5, 120)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}	


			elif bec in ('D1') and species not in ('PL'):
				
				nm = bec
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('age_gt','min', 16, 140)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}	                        
						
						
						
			###############################################			
			###############################################			
			###############################################
			### UWR Conditional Harvest and Section 7  ####
			###############################################
			###############################################
			###############################################
					   

			
			

			#########################################################
			######## MODELLING IT AS PARTIAL CUT, MOVED TO 02 CODE
			#########################################################
			
			# # grpName = ''
			# # rmzName = ''
			# # grpName = 'uwrConditional'
			# # nm = ''
			# # zone = ''
			# # uwr_n = r['uwr_number']
			# # uwr_unit = r['uwr_unit_n']
			# # bec = r['bgc_label']
			# # basal_area = r['basal_area']
			
			
			# # if uwr_n == 'u-7-003' and uwr_unit in ('T-001', 'T-002', 'T-004', 'T-007', 'T-008', 'T-011', 'T-013', 'T-015', 'T-017', 'T-018', 'T-019'):
				
				# # nm = 'vol'
				# # rmzName = '.'.join([grpName,nm])
				# # groups.write('%s,%s\n' %(r['new_id'],rmzName))
				# # constr_type, maxmin, per, val = ('age_lt','max', 30/80, 1)
				# # # maximum 30% volume removal on a cutblock area every 80 years 
				# # ## 30/80 % per year
				
				# # zone = grpName +'.'+ '_'.join([maxmin, \
					# # str(per).replace('.0','').replace('.',''),\
					# # constr_type, str(val).replace('.','') ])

				# # if rmzDCT.has_key(zone):
					# # rmzDCT[zone]['nha'] += r['nha']
					# # rmzDCT[zone]['pha'] += r['pha']
					
				# # else:
					# # rmzDCT[zone] = {
						# # 'maxmin': maxmin, 'per': per, 'val': val,
						# # 'constr_type': constr_type, 'grp': rmzName,
						# # 'number': '', 'zone': zone,
						# # 'per': per, 'val': val, 'nha': r['nha'],
						# # 'pha': r['pha']}
						
						
			grpName = ''
			rmzName = ''
			grpName = 'uwrConditional'
			nm = ''
			zone = ''
			uwr_n = r['uwr_number']
			uwr_unit = r['uwr_unit_n']
			bec = r['bgc_label']
			basal_area = r['basal_area']
			age = r['age_2021']
			
			if (uwr_n == 'u-7-012' and uwr_unit in ('LE-1-001', 'LE-1-002', 'LE-1-003', 'LE-1-004', 'LE-1-005', 'LE-1-006', 'LE-1-007', 'LE-1-008', 'LE-1-009', 'LE-2-001', 'LE-2-011','LE-2-012','LE-2-013','LE-2-014','LE-2-015','LE-2-017','LE-2-018','LE-4-001')) or (uwr_n == 'u-7-015' and uwr_unit in ('','u-7-026')) or (uwr_n == 'u-7-015' and uwr_unit == 'u-7-025'):
			
				nm = uwr_n
				rmzName = '.'.join([grpName,nm])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				constr_type, maxmin, per, val = ('age_lt','max', 50, 1)
				# 1st pass 20 years rotation, 2 pass 140 year rotation (each pass harvest 50% +/- 20% of the economically viable timber)
				## cmm doc used 50% reduction 
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
						
				else:
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val,
						'constr_type': constr_type, 'grp': rmzName,
						'number': '', 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['pha']}
                               
								
			##### AGE CONSTRAINTS                                    
			elif uwr_n == 'u-5-001' and uwr_unit == 'dqu_14' and bec[:3] <>'ICH' and basal_area in ('>=40') :
			
				nm = uwr_n
				rmzName = '.'.join([grpName,nm])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				constr_type, maxmin, per, val = ('age_lt','max', 20, 40)
				### Maximum 20% age less than 40 years 
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
						
				else:
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val,
						'constr_type': constr_type, 'grp': rmzName,
						'number': '', 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['pha']}
						

   
			elif (uwr_n == 'u-7-002' and uwr_unit in ('1','2','3','4','5','11','12','14') or (uwr_n == 'u-7-011' and uwr_unit == 'VD-001' and bec == 'SBS mc 2') or (uwr_n == 'u-7-011' and uwr_unit == 'VD-002' and bec == 'SBS mc 2') or (uwr_n == 'u-7-011' and uwr_unit == 'VD-004' and bec == 'SBS dw 3') or (uwr_n == 'u-7-011' and uwr_unit == 'VD-005' and bec == 'SBS mc 2') or (uwr_n == 'u-7-011' and uwr_unit == 'VD-006' and bec == 'SBS mc 2') or (uwr_n == 'u-7-013' and uwr_unit in('PGD-004','PGD-008','PGD-010','PGD-011','PGD-013','PGD-015','PGD-023','PGD-026','PGD-027','PGD-028','PGD-029','PGD-031','PGD-038','PGD-040','PGD-041','PGD-042','PGD-043','PGD-044','PGD-045','PGD-046','PGD-047','PGD-048','PGD-049','PGD-050','PGD-051','PGD-052','PGD-055','PGD-063','PGD-064','PGD-065')): 
			
				nm = uwr_n
				rmzName = '.'.join([grpName,nm,uwr_unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				constr_type, maxmin, per, val = ('age_gt','min', 40, 140)
				##### Minimum 40% age greater than 140 
	
	
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
						
				else:
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val,
						'constr_type': constr_type, 'grp': rmzName,
						'number': '', 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['pha']}
						


			elif (uwr_n == 'u-7-002' and uwr_unit in ('9','10','15','16','17','18') or (uwr_n == 'u-7-013' and uwr_unit in ('PGD-001','PGD-002','PGD-012','PGD-014','PGD-019','PGD-020','PGD-021','PGD-022','PGD-035','PGD-054','PGD-066') : 
			
				nm= uwr_n
				rmzName = '.'.join([grpName,nm,uwr_unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				constr_type, maxmin, per, val = ('age_gt','min', 50, 140)
				##### Minimum 50% age greater than 140 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
					
				else:
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val,
						'constr_type': constr_type, 'grp': rmzName,
						'number': '', 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['pha']}
						           

			grpName = ''
			rmzName = ''
			grpName = 'uwrConditional'
			nm = ''
			zone = ''
			uwr_n = r['uwr_number']
			uwr_unit = r['uwr_unit_n']

			if uwr_n == 'u-7-003' and uwr_unit in ('P-001','P-004','P-005','P-009','P-013','P-015','P-017','P-018','P-026','P-028','P-029','P-039','P-042','P-044','P-046','P-047','P-050','P-051','P-052','P-059','P-061','P-062', 'P-063','P-070','P-073','T-005','T-009','T-010','T-012'):
			
				nm = uwr_n
				rmzName = '.'.join([grpName,nm])
				groups.write('%s,%s\n' %(r['new_id'],rmzName)) 
				constr_type, maxmin, per, val = ('age_gt','min', 20, 100)
				##### Minimum 20% age greater than 100
				
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
						
				else:
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val,
						'constr_type': constr_type, 'grp': rmzName,
						'number': '', 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['pha']}                      

  
			###### HEIGHT CONSTRAINTS
			if uwr_n == 'u-7-003' and uwr_unit in ('P-001','P-004','P-005','P-009','P-013','P-015','P-017','P-018','P-026','P-028','P-029','P-039','P-042','P-044','P-046','P-047','P-050','P-051','P-052','P-059','P-061','P-062', 'P-063','P-070','P-073','T-005','T-009','T-010','T-012')  :
				
				nm = uwr_n
				rmzName = '.'.join([grpName,nm]) 
				# groups.write('%s,%s\n' %(r['new_id'],rmzName))  #### will have the same groups as above so no need to write twice
				constr_type, maxmin, per, val = ('ht_lt','max', 20, 3)
				##### Maximum 20% height less than 3 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
					
				else:
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val,
						'constr_type': constr_type, 'grp': rmzName,
						'number': '', 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['pha']}


			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   25 % ECA ####################
			######################################
			nm = ''
			zone = ''
			rmzName = ''
			grpName = 'fsw'
			ws = r['fsw_tag']
			unit = r['unit_no']
		
			
			if (ws == 'f-7-001' and unit in ('2','3')) or (ws == 'f-7-002' and unit >= 2 and unit <= 11 ) or (ws == 'f-7-005') or (ws == 'f-7-006' and unit in ('1','2')) \
				or (ws == 'f-7-007' and unit in ('1','2')) or (ws == 'f-7-008' and unit in ('1','2')) or (ws == 'f-7-021' and unit in ('6','8')):
				# print ws
				rmzName = '.'.join([grpName, ws,unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
								
				constr_type, maxmin, per, val = ('eca','max', 25, 0)
				#### Maintain an ECA of less than 25%

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}  

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   30 % ECA ####################
			######################################						

			
			elif ws == 'f-7-009' and unit in ('1','2'):
				# print ws
				rmzName = '.'.join([grpName, ws,unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
								
				constr_type, maxmin, per, val = ('eca','max', 30, 0)
				#### Maintain an ECA of less than 30% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}					   

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   19 % ECA ####################
			######################################
			
		
			
			elif (ws == 'f-7-011' and unit in ('1','2')) or (ws == 'f-7-016') or (ws == 'f-7-017') or (ws == 'f-7-023' and unit == '7') :
				# print ws
				rmzName = '.'.join([grpName, ws,unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
			
				constr_type, maxmin, per, val = ('eca','max', 19, 0)
				#### Maintain an ECA of less than 19% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   17 % ECA ####################
			######################################

		
			
			elif (ws == 'f-7-012' and unit in ('1','2','3')) or (ws == 'f-7-013' and unit in ('1','2','3')) :
				# print ws
				rmzName = '.'.join([grpName, ws,unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 17, 0)
				#### Maintain an ECA of less than 17% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   21 % ECA ####################
			######################################

		
			
			elif (ws == 'f-7-014' and unit in ('1','2')) or (ws == 'f-7-020' and unit == '6') or (ws == 'f-7-021' and unit in ('2','3','4')) or (ws == 'f-7-022' and unit =='3') or (ws == 'f-7-023' and unit == '4') :
				# print ws
				rmzName = '.'.join([grpName, ws,unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 21, 0)
				#### Maintain an ECA of less than 21% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   20 % ECA ####################
			######################################

			
			elif (ws == 'f-7-015' and unit in ('1','2')) or (ws == 'f-7-018') or (ws == 'f-7-022' and unit in ('2','7')):
				# print ws
				rmzName = '.'.join([grpName, ws, unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 20, 0)
				#### Maintain an ECA of less than 20% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   13 % ECA ####################
			######################################						
			
			elif ws == 'f-7-019':
				# print ws
				rmzName = '.'.join([grpName, ws, unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 13, 0)
				#### Maintain an ECA of less than 13% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}                        

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   27 % ECA ####################
			######################################
		
			
			elif (ws == 'f-7-020' and unit == '2') or (ws == 'f-7-021' and unit == '5') or (ws == 'f-7-022' and unit == '8') or (ws == 'f-7-023' and unit in ('2','3','5')):
				# print ws
				rmzName = '.'.join([grpName, ws, unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 27, 0)
				#### Maintain an ECA of less than 27% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}  

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   31 % ECA ####################
			######################################			
					
					
			elif ws == 'f-7-020' and unit == '3':
				# print ws
				rmzName = '.'.join([grpName, ws, unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 31, 0)
				#### Maintain an ECA of less than 31% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']} 

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   18 % ECA ####################
			######################################

		
			
			elif (ws == 'f-7-020' and unit == '4') or (ws == 'f-7-022' and unit == '5') :
				# print ws
				rmzName = '.'.join([grpName, ws, unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 18, 0)
				#### Maintain an ECA of less than 18% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']} 
			
			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   24 % ECA ####################
			######################################

			
			elif (ws == 'f-7-020' and unit == '5') or (ws == 'f-7-022' and unit == '4') :
				# print ws
				rmzName = '.'.join([grpName, ws, unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 24, 0)
				#### Maintain an ECA of less than 24% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   22 % ECA ####################
			######################################
		
			
			elif (ws == 'f-7-020' and unit == '7'):
				# print ws
				rmzName = '.'.join([grpName, ws, unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 22, 0)
				#### Maintain an ECA of less than 22% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}
			
			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   40 % ECA ####################
			######################################

		
			
			elif ws == 'f-7-020' and unit == '8' :
				# print ws
				rmzName = '.'.join([grpName, ws, unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 40, 0)
				#### Maintain an ECA of less than 40% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   33 % ECA ####################
			######################################

			
			elif ws == 'f-7-020' and unit in ('9','10') :
				# print ws
				rmzName = '.'.join([grpName, ws, unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 33, 0)
				#### Maintain an ECA of less than 33% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}

			######################################
			###################################### 
			######FSWS- Fisheries Sensitive WS ###
			######   23 % ECA ####################
			######################################

		
			
			elif (ws == 'f-7-021' and unit == '7') or (ws == 'f-7-022' and unit == '6') or (ws == 'f-7-023' and unit in ('6','8','9')):
				# print ws
				rmzName = '.'.join([grpName, ws, unit])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca','max', 23, 0)
				#### Maintain an ECA of less than 23% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}                          


			######################################
			###################################### 
			###### Proposed FSW  #################
			######            ####################
			######################################
			nm = ''
			zone = ''
			rmzName = ''
			grpName = 'fsw_prop'
			ws = r['draft_fsw']
			# unit = r['unit_no']
									
		
			
			if ws in ('Charlotte/Leech Lake','Duncan Creek','Nithi Lake Watershed','Omenica','Ormond Creek Watershed','Sutherland River Lower 2'\
			'Sutherland River Middle 2','Sutherland River Middle 3','Sutherland River Upper','Targe Creek Lower','Sutherland River Middle 1'):
				# print ws
				rmzName = '.'.join([grpName, ws])
				groups.write('%s,%s\n' %(r['new_id'],rmzName))
				
				constr_type, maxmin, per, val = ('eca_prop','max', 23, 0)
				#### Maintain an ECA of less than 23% 

				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
						
				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += r['nha']
					rmzDCT[zone]['pha'] += r['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': r['nha'],
						'pha': r['nha']}  						
			
			
			######################
			## Young patch size targets- by NDU as per the PG TSA Landscape Biodiversity Objectives 2004 Order
			#########################
			# young forest means forested areas between 0 and 20 years old 
			#### McGregor  
			grpName = ''
			nm = ''
			zone = ''
			rmzName = ''		
			grpName = 'patch'
			ndt = r['ndz_label_1'] 
			if ndt <> '':			
				if  ndt == 'McGregor Plateau':
					nm = 'mcgregor'
					rmzName = '.'.join([grpName, nm])
					groups.write('%s,%s\n' %(r['new_id'],rmzName))

					constr_type, maxmin, per, val = ('age_lt','min', 100, 20)
					
					zone = grpName +'.'+ '_'.join([nm,maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
					if rmzDCT.has_key(zone):
						rmzDCT[zone]['nha'] += r['nha']
						rmzDCT[zone]['pha'] += r['pha']
					else:	
						rmzDCT[zone] = {
							'maxmin': maxmin, 'per': per, 'val': val, 
							'constr_type': constr_type, 'grp': rmzName,
							'number': grpName, 'zone': zone,
							'per': per, 'val': val, 'nha': r['nha'],
							'pha': r['pha']}
			
			###### Moist Interior - Mountain			
			# elif ndt <> '':			
				elif  ndt == 'Moist Interior-Mountain':
					nm = 'moist_interior_mount'
					rmzName = '.'.join([grpName, nm])
					groups.write('%s,%s\n' %(r['new_id'],rmzName))

					constr_type, maxmin, per, val = ('age_lt','min', 100, 20)
					
					zone = grpName +'.'+ '_'.join([nm,maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
					if rmzDCT.has_key(zone):
						rmzDCT[zone]['nha'] += r['nha']
						rmzDCT[zone]['pha'] += r['pha']
					else:	
						rmzDCT[zone] = {
							'maxmin': maxmin, 'per': per, 'val': val, 
							'constr_type': constr_type, 'grp': rmzName,
							'number': grpName, 'zone': zone,
							'per': per, 'val': val, 'nha': r['nha'],
							'pha': r['pha']}

			###### Moist Interior - Valley'			
			# elif ndt <> '':			
				elif  ndt == 'Moist Interior-Valley':
					nm = 'moist_interior_valley'
					rmzName = '.'.join([grpName, nm])
					groups.write('%s,%s\n' %(r['new_id'],rmzName))

					constr_type, maxmin, per, val = ('age_lt','min', 100, 20)
					
					zone = grpName +'.'+ '_'.join([nm,maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
					if rmzDCT.has_key(zone):
						rmzDCT[zone]['nha'] += r['nha']
						rmzDCT[zone]['pha'] += r['pha']
					else:	
						rmzDCT[zone] = {
							'maxmin': maxmin, 'per': per, 'val': val, 
							'constr_type': constr_type, 'grp': rmzName,
							'number': grpName, 'zone': zone,
							'per': per, 'val': val, 'nha': r['nha'],
							'pha': r['pha']}                            
							
			###### Northern Boreal Mountains			 
			# elif ndt <> '':			
				elif  ndt == 'Northern Boreal Mountains':
					nm = 'northern_boreal_mount'
					rmzName = '.'.join([grpName, nm])
					groups.write('%s,%s\n' %(r['new_id'],rmzName))

					constr_type, maxmin, per, val = ('age_lt','min', 100, 20)
					
					zone = grpName +'.'+ '_'.join([nm,maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
					if rmzDCT.has_key(zone):
						rmzDCT[zone]['nha'] += r['nha']
						rmzDCT[zone]['pha'] += r['pha']
					else:	
						rmzDCT[zone] = {
							'maxmin': maxmin, 'per': per, 'val': val, 
							'constr_type': constr_type, 'grp': rmzName,
							'number': grpName, 'zone': zone,
							'per': per, 'val': val, 'nha': r['nha'],
							'pha': r['pha']}                             
 
 
			###### Omineca - Mountains			
			# elif ndt <> '':			
				elif  ndt == 'Omineca-Mountain':
					nm = 'omineca_mt'
					rmzName = '.'.join([grpName, nm])
					groups.write('%s,%s\n' %(r['new_id'],rmzName))

					constr_type, maxmin, per, val = ('age_lt','min', 100, 20)
					
					zone = grpName +'.'+ '_'.join([nm,maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
					if rmzDCT.has_key(zone):
						rmzDCT[zone]['nha'] += r['nha']
						rmzDCT[zone]['pha'] += r['pha']
					else:	
						rmzDCT[zone] = {
							'maxmin': maxmin, 'per': per, 'val': val, 
							'constr_type': constr_type, 'grp': rmzName,
							'number': grpName, 'zone': zone,
							'per': per, 'val': val, 'nha': r['nha'],
							'pha': r['pha']}                
				
				
			###### Omineca - Valley			
			# elif ndt <> '':			
				elif  ndt == 'Omineca-Valley':
					nm = 'omineca_vl'
					rmzName = '.'.join([grpName, nm])
					groups.write('%s,%s\n' %(r['new_id'],rmzName))

					constr_type, maxmin, per, val = ('age_lt','min', 100, 20)
					
					zone = grpName +'.'+ '_'.join([nm,maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
					if rmzDCT.has_key(zone):
						rmzDCT[zone]['nha'] += r['nha']
						rmzDCT[zone]['pha'] += r['pha']
					else:	
						rmzDCT[zone] = {
							'maxmin': maxmin, 'per': per, 'val': val, 
							'constr_type': constr_type, 'grp': rmzName,
							'number': grpName, 'zone': zone,
							'per': per, 'val': val, 'nha': r['nha'],
							'pha': r['pha']}                     


			###### Wet Mountain 		
			# elif ndt <> '':			
				elif  ndt == 'Wet Mountain':
					nm = 'wet_mountain'
					rmzName = '.'.join([grpName, nm])
					groups.write('%s,%s\n' %(r['new_id'],rmzName))

					constr_type, maxmin, per, val = ('age_lt','min', 100, 20)
					
					zone = grpName +'.'+ '_'.join([nm,maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
					if rmzDCT.has_key(zone):
						rmzDCT[zone]['nha'] += r['nha']
						rmzDCT[zone]['pha'] += r['pha']
					else:	
						rmzDCT[zone] = {
							'maxmin': maxmin, 'per': per, 'val': val, 
							'constr_type': constr_type, 'grp': rmzName,
							'number': grpName, 'zone': zone,
							'per': per, 'val': val, 'nha': r['nha'],
							'pha': r['pha']}				
				

			###### Wet Trench - Mountain		
			# elif ndt <> '':			
				elif  ndt == 'Wet Trench-Mountain':
					nm = 'wet_trench_mtn'
					rmzName = '.'.join([grpName, nm])
					groups.write('%s,%s\n' %(r['new_id'],rmzName))

					constr_type, maxmin, per, val = ('age_lt','min', 100, 20)
					
					zone = grpName +'.'+ '_'.join([nm,maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
					if rmzDCT.has_key(zone):
						rmzDCT[zone]['nha'] += r['nha']
						rmzDCT[zone]['pha'] += r['pha']
					else:	
						rmzDCT[zone] = {
							'maxmin': maxmin, 'per': per, 'val': val, 
							'constr_type': constr_type, 'grp': rmzName,
							'number': grpName, 'zone': zone,
							'per': per, 'val': val, 'nha': r['nha'],
							'pha': r['pha']}                
				
				
			###### Wet Trench - Valley		
			# elif ndt <> '':			
				elif  ndt == 'Wet Trench-Valley':
					nm = 'wet_trench_vl'
					rmzName = '.'.join([grpName, nm])
					groups.write('%s,%s\n' %(r['new_id'],rmzName))

					constr_type, maxmin, per, val = ('age_lt','min', 100, 20)
					
					zone = grpName +'.'+ '_'.join([nm,maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])
					if rmzDCT.has_key(zone):
						rmzDCT[zone]['nha'] += r['nha']
						rmzDCT[zone]['pha'] += r['pha']
					else:	
						rmzDCT[zone] = {
							'maxmin': maxmin, 'per': per, 'val': val, 
							'constr_type': constr_type, 'grp': rmzName,
							'number': grpName, 'zone': zone,
							'per': per, 'val': val, 'nha': r['nha'],
							'pha': r['pha']}                   
				
				

			###############################################			
			###############################################			
			###############################################
			### Partitions####
			###############################################
			###############################################
			###############################################


			########### SUPPLY BLOCK 
			grpName = ''
			rmzName = ''
			grpName = 'partition'
			nm = ''
			zone = ''
			supply_blk = r['supply_blk']
			sp = r['spp_rep']
			district = r['district_name']

			if supply_blk in ('Block A') or  supply_blk in ('Block B'):
				
				nm = 'block_A_B'  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('vol','min', 0, 0)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}  
						
						

			elif supply_blk not in ('Block A') and supply_blk not in ('Block B'):
				
				nm = 'blocks_other'  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('vol','min', 0, 0)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}  
												   

			########### DECID 	

			if supply_blk not in ('Block A','Block B') and sp in ('AT','decid_other') and district not in ('Vanderhoof Natural Resource District'):
				
				nm = 'decid'  
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('vol','min', 0, 0)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}  						                                                      

			########### LICENSEE 
			grpName = ''
			rmzName = ''
			grpName = 'licensee'
			nm = ''
			zone = ''
			lic = r['final_licensee']
	

			if lic not in ('none'): 
				
				nm = lic 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('vol','min', 0, 0)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}

			
			####### ADDED 21 DEC 2021 -USING NEW VERSION OF RESULTANT WITH OG LAYERS 
			##########  OLD GROWTH - PRIORITY  			
			grpName = ''
			rmzName = ''
			grpName = 'og_deferral'
			nm = ''
			zone = ''
			priority = r['priority_label']

			if priority is not None: 
				# print 'priority', priority
				
				nm = 'priority_deferral' 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('ht_lt','max', 0, 3)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}


			###########  OLD GROWTH - PROTECTED  
			grpName = ''
			rmzName = ''
			grpName = 'og_deferral'
			nm = ''
			zone = ''
			protected = r['protected_label']	


			if protected is not None: 
				# print 'protected',protected
				
				nm = 'protected' 
				rmzName = '.'.join([grpName, nm])				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('ht_lt','max', 0, 3)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}

			########################################################
			####### 24 FEB 2022 -PG SPATIALIZATION AREAS 	
			#######################################################
			grpName = ''
			rmzName = ''
			grpName = 'pg_spatialization_areas'
			nm = ''
			zone = ''
			spatialization = r['candidate_areas']

			if (spatialization == 'Y'): 
				
				nm = '' 
				rmzName = grpName				
				groups.write('%s,%s\n' %(r['new_id'],rmzName))             
				constr_type, maxmin, per, val = ('ht_lt','max', 0, 3)
				
				zone = rmzName +'.'+ '_'.join([maxmin, \
					str(per).replace('.0','').replace('.',''),\
					constr_type, str(val).replace('.','') ])

				if rmzDCT.has_key(zone):
					rmzDCT[zone]['nha'] += nha
					rmzDCT[zone]['pha'] += pha
					#print '140', rmzDCT[zone]['nha'], rmzDCT[zone]['pha']
				else:	
					rmzDCT[zone] = {
						'maxmin': maxmin, 'per': per, 'val': val, 
						'constr_type': constr_type, 'grp': rmzName,
						'number': grpName, 'zone': zone,
						'per': per, 'val': val, 'nha': nha,
						'pha': pha}
		
						
		else: pass
		####### PROGRESS MONITOR FOR PROGRAM #########################

		if ( count % 40000== 0):
			# print 'the count is ', cur.rownumber, rowcnt, cur.rownumber % 10000
			percent = int(float(count ) / rowcnt * 100)
			updLog('%s%% done' %percent, StartTime)
		count +=1
		#############################

	lod_bulkins(conn, rmzDCT.values(), 'pw_rmz_%s'%(ver), StartTime)
	groups.close()
	cur.execute("VACUUM pw_rmz_%s"%(ver))   


		
######## This is a linking dictionary to the rmz table for VQO . You have identify av_slope.
######## You need to define av_slope in the calc_rmz  		
######## In Val_cf case that is based on slope and the field name is grid_code
def avSLP(av_slope):
	# veg height and p2p ratio by slope class %
	veg_ht = 0
	p2p_ratio = 0

	if av_slope <= 5:
		veg_ht = 3.0
		p2p_ratio = 4.68
	elif av_slope <= 10:
		veg_ht = 3.5
		p2p_ratio = 4.68
	elif av_slope <= 15:
		veg_ht = 4.0
		p2p_ratio = 3.77
	elif av_slope <= 20:
		veg_ht = 4.5
		p2p_ratio = 3.77
	elif av_slope <= 25:
		veg_ht = 5.0
		p2p_ratio = 3.04
	elif av_slope <= 30:
		veg_ht = 5.5
		p2p_ratio = 3.04
	elif av_slope <= 35:
		veg_ht = 6.0
		p2p_ratio = 2.45
	elif av_slope <= 40:
		veg_ht = 6.5
		p2p_ratio = 2.45	
	elif av_slope <= 45:
		veg_ht = 6.5
		p2p_ratio = 1.98
	elif av_slope <= 50:
		veg_ht = 7.0
		p2p_ratio = 1.98
	elif av_slope <= 55:
		veg_ht = 7.5
		p2p_ratio = 1.6
	elif av_slope <= 60:
		veg_ht = 8.0
		p2p_ratio = 1.6
	elif av_slope <= 65:
		veg_ht = 8.5
		p2p_ratio = 1.29
	elif av_slope <= 70:
		veg_ht = 8.5
		p2p_ratio = 1.29
	else:
		veg_ht = 8.5
		p2p_ratio = 1.04

	return veg_ht, p2p_ratio
	
	
tblName = 'res_24feb22' ### vector old growth
# tblName = 'res_w_old' ## raster old growth
# tblName = 'res_pg_model_5nov'
# tblName = 'res_pg_model_27sept'
# tblName = 'res_pg_model_13sept'
dbase = 'pg_tsa_new'
# ver = 'base'
# ver = 'vqo'
ver = 'newbase' ###  correct vqo and og layers accounted for 
# ver = 'oldgrowth'
# ver = '5nov'
# ver = '26oct'
# ver = 'dmp'


folder = "/projects/canfor/fg_17_546_cfp_pg_tsa_cell_reallocation/analysis/pw/tracks"

try:
	conn = psycopg2.connect(dbname = dbase, host = 'aspen')  
	conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)          
except Exception, exc:
	print "connection problem", 
	print "%s: %s" % (exc.__class__.__name__, exc)
	raise
	
cur=conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


calc_rmz()