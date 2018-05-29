# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import MySQLdb

#connRemote=MySQLdb.connect(host="192.168.150.79",user="root",passwd="vortex",db="ImitateHIS",charset="utf8")#连接远程数据库
connLocal=MySQLdb.connect(host="127.0.0.1",user="ccy",passwd="123456",db="football_betting",charset="utf8")#连接本地数据库
#curRemote=connRemote.cursor()
curLocal=connLocal.cursor()
"""
sqlLocal = "insert into ftb_match (category,start_time,home,away,score) values ('亚预赛','2016-10-11 21:00:00'\
,'乌兹别克','中国','1:0')" 
selRowsNumCur = curLocal.execute(sqlLocal)

try:
	connLocal.commit()
except:
	connLocal.rollback()

curLocal.close()

connLocal.close()
"""
def selectByUniqueIndex(table_name,unique_code):
	if table_name == "football_match":
		sqlLocal = "select id,version,status,source_url,match_category,score,half_score,match_season,match_group," \
				   "match_round, match_data_id, home, away from "+table_name+" where unique_code = '"+unique_code+"'"
	elif table_name == "europe_odds":
		sqlLocal = "select id, home_odds, draw_odds, away_odds, relative_match_update_time, version, source_url from " + table_name +" where unique_code = '"+unique_code+"'"
	else:
		sqlLocal = "select id,version,source_url from "+table_name+" where unique_code = '"+unique_code+"'"
	selRowsNumCur = curLocal.execute(sqlLocal)
	record_tuple = curLocal.fetchall()
	
	if (len(record_tuple)) == 0:
		return None
	else:
		return record_tuple

def selectByMatchId(table_name,match_id):
	if table_name == "match_statistics":
		sqlLocal = "select id,version,source_url from "+table_name+" where match_id = '"+match_id+"'"
	else:
		sqlLocal = "select id,version,source_url from "+table_name+" where match_id = '"+match_id+"'"
	selRowsNumCur = curLocal.execute(sqlLocal)
	record_tuple = curLocal.fetchall()
	
	if (len(record_tuple)) == 0:
		return None
	else:
		return record_tuple

def insert(sql, param):
	if None!=param:
		selRowsNumCur = curLocal.execute(sql,param)
	else:
		selRowsNumCur = curLocal.execute(sql)
	try:
		connLocal.commit()
	except:
		print "############################"
		print "insert error"
		connLocal.rollback()

def update(sql):
	selRowsNumCur = curLocal.execute(sql)
	try:
		connLocal.commit()
	except:
		print "############################"
		print "update error"
		connLocal.rollback()

status_list = ["UPDATING","FINISHED"]
class FootbettingPipeline(object):
    def process_item(self, item, spider):
        return item

class MatchPipeline(object):
	def process_item(self, item, spider):
		if (item['item_id'] == 'Match'):

			record_tuple = selectByUniqueIndex("football_match",item['unique_code'])

			if (not record_tuple):

				if item['score'] == "-:-":
					item['status'] = status_list[0]
				else:
					item['status'] = status_list[1]

				item['version'] = "1"
				item['created_by'] = "ccy"
				item['updated_by'] = "ccy"

				# sqlLocal = "insert into football_match (start_time,home,score,away,half_score,handicap,unique_code,\
				# source_url,status,version,created_by,created_at,updated_by,updated_at,category,season,group) values ('"+item['start_time']+"','"+\
				# item['home']+"','"+item['score']+"','"+item['away']+"','"+item['half_score']+"','"+item['handicap']+\
				# "','"+item['unique_code']+"','"+item['source_url']+"','"+item['status']+"','"+item['version']\
				# +"','"+item['created_by']+"',now(),'"+item['updated_by']+"',now(),'"+item['match_category']+"','"+item['match_season']\
				# 		   +"','"+item['match_group']+"')"
				sqlLocal = "insert into football_match (start_time,home,score,away,half_score,handicap,unique_code,\
								source_url,status,version,created_by,created_at,updated_by,updated_at,match_category, \
						   match_season,match_group,match_round, match_data_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s,now(),%s,%s,%s,%s,%s)"
				#print "#########"
				#print sqlLocal
				#return item
				param = (item['start_time'],item['home'],item['score'],item['away'],item['half_score'],item['handicap'],item['unique_code']
						 ,item['source_url'],item['status'],item['version'],item['created_by'],item['updated_by'],item['match_category']
						 ,item['match_season'],item['match_group'],item['match_round'], item['match_data_id'])
				insert(sqlLocal, param)

				record_tuple = selectByUniqueIndex("football_match",item['unique_code'])

				item['match_id'] = unicode(record_tuple[0][0])
			else:
				if (len(record_tuple)) == 1:
					item['match_id'] = unicode(record_tuple[0][0])
					if item['score'] == "-:-":
						item['status'] = status_list[0]
					else:
						item['status'] = status_list[1]

					# id, version, status, source_url, category, score, half_score, season, group

					sqlLocal = "update football_match set "
					if item['status'] != unicode(record_tuple[0][2]):
						sqlLocal = sqlLocal + "status = '"+item['status']+"',"
					if(item['match_category'] != unicode(record_tuple[0][4])):
						sqlLocal = sqlLocal + "match_category = '" + item['match_category'] + "',"
					if (item['score'] != unicode(record_tuple[0][5])):
						sqlLocal = sqlLocal + "score = '" + item['score'] + "',"
					if (item['half_score'] != unicode(record_tuple[0][6])):
						sqlLocal = sqlLocal + "half_score = '" + item['half_score'] + "',"
					if (item['match_season'] != unicode(record_tuple[0][7])):
						sqlLocal = sqlLocal + "match_season = '" + item['match_season'] + "',"
					if (item['match_group'] != unicode(record_tuple[0][8])):
						sqlLocal = sqlLocal + "match_group = '" + item['match_group'] + "',"
					if (item['match_round'] != unicode(record_tuple[0][9])):
						sqlLocal = sqlLocal + "match_round = '" + item['match_round'] + "',"
					if (item['match_data_id'] != unicode(record_tuple[0][10])):
						sqlLocal = sqlLocal + "match_data_id = '" + item['match_data_id'] + "',"
					if (item['home'] != unicode(record_tuple[0][11])):
						sqlLocal = sqlLocal + "home = '" + item['home'] + "',"
					if (item['away'] != unicode(record_tuple[0][12])):
						sqlLocal = sqlLocal + "away = '" + item['away'] + "',"

					if sqlLocal != "update football_match set ":
						sqlLocal = sqlLocal + "version = " +  str(int(record_tuple[0][1])+1)+ ","
						sqlLocal = sqlLocal + "updated_at = now(),updated_by = 'ccy'"
						sqlLocal = sqlLocal + " where id ="+item['match_id']+" and version = "+unicode(record_tuple[0][1])
						update(sqlLocal)
				else:
					print "表:%s里的唯一索引:%s不唯一",("football_match",item['unique_code'])
					raise Exception
		
		return item

class EuropeOddsPipeline(object):
	def process_item(self, item, spider):
		if (item['item_id'] == 'EuropeOdds'):
			#,relative_match_update_time
			# if item['bookmaker'] == '竞彩官方(胜平负)':
			# 	print item['bookmaker']
			record_tuple = selectByUniqueIndex("europe_odds",item['unique_code'])
			if (not record_tuple):

				item['version'] = "1"
				item['created_by'] = "ccy"
				item['updated_by'] = "ccy"
				if (item['draw_kellyindex'] != ""):
					sqlLocal = "insert into europe_odds (match_id,bookmaker,home_odds,draw_odds,away_odds,update_time,\
					home_prob,draw_prob,away_prob,home_kellyindex,draw_kellyindex,away_kellyindex,return_rate,unique_code,\
					source_url,version,relative_match_update_time,created_by,created_at,updated_by,updated_at) values ('"+item['match_id']+"','"+\
					item['bookmaker']+"','"+item['home_odds']+"','"+item['draw_odds']+"','"+item['away_odds']+"','"+item['update_time']\
					+"','"+item['home_prob']+"','"+item['draw_prob']+"','"+item['away_prob']+"','"+item['home_kellyindex']\
					+"','"+item['draw_kellyindex']+"','"+item['away_kellyindex']+"','"+item['return_rate']+"','"+item['unique_code']\
					+"','"+item['source_url']+"','"+item['version']+"','"+item['relative_match_update_time']\
					+"','"+item['created_by']+"',now(),'"+item['updated_by']+"',now())"
				else:
					sqlLocal = "insert into europe_odds (match_id,bookmaker,home_odds,draw_odds,away_odds,update_time,\
					home_prob,draw_prob,away_prob,home_kellyindex,away_kellyindex,return_rate,unique_code,source_url,version," \
							   "relative_match_update_time,created_by,created_at,updated_by,updated_at) values ('"+item['match_id']+"','"+\
					item['bookmaker']+"','"+item['home_odds']+"','"+item['draw_odds']+"','"+item['away_odds']+"','"+item['update_time']\
					+"','"+item['home_prob']+"','"+item['draw_prob']+"','"+item['away_prob']+"','"+item['home_kellyindex']\
					+"','"+item['away_kellyindex']+"','"+item['return_rate']+"','"+item['unique_code']+"','"+item['source_url']\
					+"','"+item['version']+"','"+item['relative_match_update_time']\
					+"','"+item['created_by']+"',now(),'"+item['updated_by']+"',now())"

				insert(sqlLocal, None)

			else:
				if (len(record_tuple)) == 1:
					item['europe_odds_id'] = unicode(record_tuple[0][0])
					# id, home_odds, draw_odds, away_odds, relative_match_update_time, version, source_url

					sqlLocal = "update europe_odds set "
					if item['home_odds'] != unicode(record_tuple[0][1]):
						sqlLocal = sqlLocal + "home_odds = '" + item['home_odds'] + "',"
					if item['draw_odds'] != unicode(record_tuple[0][2]):
						sqlLocal = sqlLocal + "draw_odds = '" + item['draw_odds'] + "',"
					if item['away_odds'] != unicode(record_tuple[0][3]):
						sqlLocal = sqlLocal + "away_odds = '" + item['away_odds'] + "',"
					if item['relative_match_update_time'] != unicode(record_tuple[0][4]):
						sqlLocal = sqlLocal + "relative_match_update_time = '" + item['relative_match_update_time'] + "',"
					if sqlLocal != "update europe_odds set ":
						sqlLocal = sqlLocal + "version = " + str(int(record_tuple[0][5]) + 1)+ ","
						sqlLocal = sqlLocal + "updated_at = now(),updated_by = 'ccy'"
						sqlLocal = sqlLocal + " where id =" + item['europe_odds_id'] + " and version = " + unicode(
							record_tuple[0][5])
						update(sqlLocal)
				else:
					print "表:%s里的唯一索引:%s不唯一",("europe_odds",item['unique_code'])
					raise Exception
		
		return item

class AsianOddsPipeline(object):
	def process_item(self, item, spider):
		if (item['item_id'] == 'AsianOdds'):
			#,relative_match_update_time
			record_tuple = selectByUniqueIndex("asian_odds",item['unique_code'])
			if (not record_tuple):

				item['version'] = "1"
				item['created_by'] = "ccy"
				item['updated_by'] = "ccy"
				
				sqlLocal = "insert into asian_odds (match_id,bookmaker,home_odds,handicap,away_odds,update_time,\
				home_prob,away_prob,home_kellyindex,away_kellyindex,return_rate,unique_code,source_url,version,created_by,created_at,updated_by,updated_at) values ('"+item['match_id']+"','"+\
				item['bookmaker']+"','"+item['home_odds']+"','"+item['handicap']+"','"+item['away_odds']+"','"+item['update_time']\
				+"','"+item['home_prob']+"','"+item['away_prob']+"','"+item['home_kellyindex']\
				+"','"+item['away_kellyindex']+"','"+item['return_rate']+"','"+item['unique_code']+"','"+item['source_url']+"','"+item['version']\
				+"','"+item['created_by']+"',now(),'"+item['updated_by']+"',now())"

				insert(sqlLocal, None)

			elif (len(record_tuple)) != 1:
				print "表:%s里的唯一索引:%s不唯一",("europe_odds",item['unique_code'])
				raise Exception
		return item

class OverUnderOddsPipeline(object):
	def process_item(self, item, spider):
		if (item['item_id'] == 'OverUnderOdds'):
			#,relative_match_update_time
			record_tuple = selectByUniqueIndex("over_under_odds",item['unique_code'])
			if (not record_tuple):

				item['version'] = "1"
				item['created_by'] = "ccy"
				item['updated_by'] = "ccy"
				
				sqlLocal = "insert into over_under_odds (match_id,bookmaker,over_odds,handicap,under_odds,update_time,\
				over_prob,under_prob,over_kellyindex,under_kellyindex,return_rate,unique_code,source_url,version,created_by,created_at,updated_by,updated_at) values ('"+item['match_id']+"','"+\
				item['bookmaker']+"','"+item['over_odds']+"','"+item['handicap']+"','"+item['under_odds']+"','"+item['update_time']\
				+"','"+item['over_prob']+"','"+item['under_prob']+"','"+item['over_kellyindex']\
				+"','"+item['under_kellyindex']+"','"+item['return_rate']+"','"+item['unique_code']+"','"+item['source_url']+"','"+item['version']\
				+"','"+item['created_by']+"',now(),'"+item['updated_by']+"',now())"

				insert(sqlLocal, None)

			elif (len(record_tuple)) != 1:
				print "表:%s里的唯一索引:%s不唯一",("europe_odds",item['unique_code'])
				raise Exception
		return item

class StrokeAnalysisPipeline(object):
	def process_item(self, item, spider):
		if (item['item_id'] == 'StrokeAnalysis'):
			#,relative_match_update_time
			record_tuple = selectByMatchId("match_statistics",item['match_id'])
			if (not record_tuple):

				item['version'] = "1"
				item['created_by'] = "ccy"
				item['updated_by'] = "ccy"

				if (item.get('home_ballcontrol_time_ratio','')!=''):
					item['home_ballcontrol_time_ratio'] = unicode(float(item['home_ballcontrol_time_ratio'][:-1])/100.0)
				if (item.get('away_ballcontrol_time_ratio','')!=''):
					item['away_ballcontrol_time_ratio'] = unicode(float(item['away_ballcontrol_time_ratio'][:-1])/100.0)

				sqlLocal = "insert into match_statistics ("
				key_value_list = item.items()
				for key_value in key_value_list:
					if (key_value[0]!='item_id'):
						sqlLocal += key_value[0]+","

				sqlLocal += "created_at,updated_at) values("
				for key_value in key_value_list:
					if (key_value[0]!='item_id'):
						sqlLocal += "'"+key_value[1]+"',"

				sqlLocal += "now(),now())"

				#print "###############"

				#print sqlLocal

				"""
				
				sqlLocal = "insert into match_statistics (match_id,home_situation,away_situation,home_shoot_times,away_shoot_times,\
				home_shoot_target_times,away_shoot_target_times,home_foul_times,away_foul_times,home_corner_times,away_corner_times,\
				home_freekick_times,away_freekick_times,home_offside_times,away_offside_times,home_yellowcard_times,away_yellowcard_times,\
				home_redcard_times,away_redcard_times,home_ballcontrol_time_ratio,away_ballcontrol_time_ratio,home_headballs,away_headballs,\
				home_saveballs,away_saveballs,source_url,version,created_by,created_at,updated_by,updated_at) values ('"+item['match_id']+"','"+\
				item['home_situation']+"','"+item['away_situation']+"','"+item['home_shoot_times']+"','"+item['away_shoot_times']\
				+"','"+item['home_shoot_target_times']+"','"+item['away_shoot_target_times']+"','"+item['home_foul_times']\
				+"','"+item['away_foul_times']+"','"+item['home_corner_times']+"','"+item['away_corner_times']+"','"+item['home_freekick_times']\
				+"','"+item['away_freekick_times']+"','"+item['home_offside_times']+"','"+item['away_offside_times']+"','"+item['home_yellowcard_times']+"','"+item['away_yellowcard_times']\
				+"','"+item['home_redcard_times']+"','"+item['away_redcard_times']+"','"+item['home_ballcontrol_time_ratio']+"','"+item['away_ballcontrol_time_ratio']\
				+"','"+item['home_headballs']+"','"+item['away_headballs']+"','"+item['home_saveballs']+"','"+item['away_saveballs']\
				+"','"+item['source_url']+"','"+item['version']+"','"+item['created_by']+"',now(),'"+item['updated_by']+"',now())"
				"""
				insert(sqlLocal, None)

			elif (len(record_tuple)) != 1:
				print "表:%s里的唯一索引:%s不唯一",("europe_odds",item['unique_code'])
				raise Exception
		return item
"""
class EuropeOddsUpdateHistoryPipeline(object):
	def process_item(self, item, spider):
		return item
"""
class ClosePipeline(object):
	def process_item(self, item, spider):
		return item
	def close_spider(self, spider):
		curLocal.close()
		connLocal.close()

