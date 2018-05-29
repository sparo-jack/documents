# -*- coding: utf-8 -*-
import MySQLdb

#connRemote=MySQLdb.connect(host="192.168.150.79",user="root",passwd="vortex",db="ImitateHIS",charset="utf8")#连接远程数据库
connLocal=MySQLdb.connect(host="127.0.0.1",user="ccy",passwd="123456",db="football_betting",charset="utf8")#连接本地数据库
#curRemote=connRemote.cursor()
curLocal=connLocal.cursor()

sqlLocal = "select id,version,status,source_url,category,score,half_score,season,macth_group from " \
           "football_match where unique_code = '2017-10-07 23:59_直布罗陀_爱沙尼亚'"

selRowsNumCur = curLocal.execute(sqlLocal)
record_tuple = curLocal.fetchall()

sqlLocal = "update football_match set "

sqlLocal = sqlLocal + "version = " +  str(int(record_tuple[0][1])+1)
sqlLocal = sqlLocal + " where id =1 and version = "+unicode(record_tuple[0][1])

def update(sql):
	selRowsNumCur = curLocal.execute(sql)
	try:
		connLocal.commit()
	except:
		print "############################"
		print "update error"
		connLocal.rollback()
update(sqlLocal)
print record_tuple