# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import MySQLdb
import re
import datetime
import numpy as np
import cPickle

# connRemote=MySQLdb.connect(host="192.168.150.79",user="root",passwd="vortex",db="ImitateHIS",charset="utf8")#连接远程数据库
connLocal = MySQLdb.connect(host="127.0.0.1", user="ccy", passwd="123456", db="football_betting",
                            charset="utf8")  # 连接本地数据库
matchCurLocal = connLocal.cursor()
europeOddsCur = connLocal.cursor()

bookmaker_list = ['平均欧赔','威廉希尔','Bet365','立博']
status_list = ["UPDATING","FINISHED"]
fetch_size = 100
def make_features():
    match_sql = "select id,start_time,score from football_match where status = 'FINISHED' order by start_time desc"
    selRowsNumCur = matchCurLocal.execute(match_sql)
    mid_labels_features = []
    max_odds = 0
    min_odds = 100000
    max_dist = 0
    min_dist = 1000000000000000
    for _ in xrange(selRowsNumCur/fetch_size):
        match_record_tuple = matchCurLocal.fetchmany(fetch_size)
        for match_row in match_record_tuple:
            labels_features = []
            match_id = match_row[0]
            match_start_time = match_row[1]
            match_scroe = match_row[2]
            re_match = re.match(r'^(\d+):(\d+)', match_scroe)
            home_goals = int(re_match.group(1))
            away_goals = int(re_match.group(2))
            start_odd_fea = []
            last_odd_fea = []
            for bookmaker in bookmaker_list:
                europe_odds_sql = "select home_odds, draw_odds, away_odds,update_time from europe_odds where " \
                                  "match_id=%s and bookmaker=%s and update_time<=%s order by update_time asc"
                europeOddsCur.execute(europe_odds_sql,(match_id,bookmaker,match_start_time- datetime.timedelta(minutes=15)))
                europe_odds_record_tuple = europeOddsCur.fetchall()
                if len(europe_odds_record_tuple) == 0:
                    # print match_id
                    break
                start_odd_tuple = europe_odds_record_tuple[0]
                # start_time_dist =
                start_odd_tuple_fea = list(start_odd_tuple[:3])
                if max(start_odd_tuple_fea) > max_odds:
                    max_odds = max(start_odd_tuple_fea)
                if min(start_odd_tuple_fea) < min_odds:
                    min_odds = min(start_odd_tuple_fea)
                start_odd_tuple_fea.append( (match_start_time - start_odd_tuple[-1]).total_seconds())
                if start_odd_tuple_fea[-1] > max_dist:
                    max_dist = start_odd_tuple_fea[-1]
                if start_odd_tuple_fea[-1] < min_dist:
                    min_dist = start_odd_tuple_fea[-1]
                start_odd_fea.extend(start_odd_tuple_fea)
                last_odds_tuple = europe_odds_record_tuple[-1]
                # last_time_dist = match_start_time - last_odds_tuple[-1]
                last_odd_tuple_fea = list(last_odds_tuple[:3])
                if max(last_odd_tuple_fea) > max_odds:
                    max_odds = max(last_odd_tuple_fea)
                if min(last_odd_tuple_fea) < min_odds:
                    min_odds = min(last_odd_tuple_fea)
                last_odd_tuple_fea.append((match_start_time - last_odds_tuple[-1]).total_seconds())
                if last_odd_tuple_fea[-1] > max_dist:
                    max_dist = last_odd_tuple_fea[-1]
                if last_odd_tuple_fea[-1] < min_dist:
                    min_dist = last_odd_tuple_fea[-1]
                last_odd_fea.extend(last_odd_tuple_fea)
                # if max_odds >=250:
                #     print match_id
            if len(start_odd_fea) == 16 and len(last_odd_fea) == 16:
                if home_goals > away_goals:
                    labels_features.append(0)
                elif home_goals == away_goals:
                    labels_features.append(1)
                else:
                    labels_features.append(2)
                labels_features.append((start_odd_fea,last_odd_fea))
                mid_labels_features.append((match_id, labels_features))
    print "make features finished"
    print len(mid_labels_features)
    print max_odds, min_odds, max_dist, min_dist
    print mid_labels_features[:1]

    features_list = []
    labels_list = []
    match_id_list = []
    for i in range(len(mid_labels_features)):
        match_id_list.append(mid_labels_features[i][0])
        features_list.append(mid_labels_features[i][1][1])
        labels_list.append(mid_labels_features[i][1][0])
    features_np = np.array(features_list)
    labels_np = np.array(labels_list)

    features_np = features_np.reshape((features_np.shape[0]*features_np.shape[1],
                                    features_np.shape[2]))
    dist_slice = features_np[:, [3, 7, 11, 15]]
    features_np[:, [3, 7, 11, 15]] = (dist_slice-dist_slice.min())*1.0/(dist_slice.max()-dist_slice.min())
    # print dist_slice[0]
    # print features_np[0]
    odds_slice = features_np[:, [0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14]]
    features_np[:, [0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14]] = (odds_slice-odds_slice.min())*1.0/(odds_slice.max()-odds_slice.min())
    # print odds_slice[0]
    # print features_np[0]
    features_np = features_np.reshape((features_np.shape[0]/2,
                                                            2,features_np.shape[1]))
    assert features_np.shape[0] == labels_np.shape[0]

    cPickle.dump((match_id_list, features_np, labels_np), open("make_featues_1_5.pkl","wb"), -1)
    # print features_np.shape
    # print features_np[0]


    # print features_np[:, [3, 7, 11, 15]].max()
    # print features_np[:, [3, 7, 11, 15]].min()
    # print features_np[:, [0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14]].max()
    # print features_np[:, [0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14]].min()
    # features_np[:,:].max(axis=2)
    # print features_np.shape
    # print mid_labels_features[:5]

    connLocal.close()
if __name__ == '__main__':
    make_features()

