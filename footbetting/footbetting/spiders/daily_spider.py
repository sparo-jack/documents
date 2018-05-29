# coding:utf-8
import sys

reload(sys)
sys.setdefaultencoding("utf-8")
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from footbetting.items import *
import json
from decimal import Decimal
import re
from datetime import datetime,timedelta
from footbetting.crawl_proxy_ip import crawl_proxy_ip

europe_big_five_football_leagues_list = ["http://saishi.zgzcw.com/soccer/league/36/",
                                         "http://saishi.zgzcw.com/soccer/league/8/",
                                         "http://saishi.zgzcw.com/soccer/league/34/",
                                         "http://saishi.zgzcw.com/soccer/league/11/",
                                         "http://saishi.zgzcw.com/soccer/league/31/"]
europe_big_five_football_leagues_name_list = ["英超", "西甲", "法甲", "德甲", "意甲"]
season_list = ["2017-2018", "2016-2017", "2015-2016"]
situation_dict = {1: "入球", 2: "红牌", 3: "黄牌", 7: "点球", 8: "乌龙", 9: "两黄变红", 11: "换人"}

bookmaker_list = ['平均欧赔', '竞彩官方(胜平负)', '威廉希尔', '伟德(直布罗陀)', 'Bet365', '立博', '澳门', '皇冠',
                  '香港马会', 'bwin', '明升', '利记', '金宝博', '10BET', 'Coral', 'Interwetten']
single_season_match_category_list = ["世界杯", "亚冠", "日职", "挪超", "瑞典超", "韩K联", "美职", "日乙", "中超", "巴甲"]

proxy_ip = "124.193.51.249"
proxy_port = "3128"

# proxy_ip = "223.96.95.229"
# proxy_port = "3128"

# proxy_ip = "124.133.230.254"
# proxy_port = "80"

proxy_ip = "124.133.230.254"
proxy_port = "80"
#
#
#
# proxy_ip , proxy_port = crawl_proxy_ip()



# proxy_ip = "118.114.77.47"
# proxy_port = "8080"

# proxy_ip = "101.68.73.54"
# proxy_port = "53281"

# proxy_ip = "115.159.219.156"
# proxy_port = "8080"

proxy_addres = "http://" + proxy_ip + ":" + proxy_port

class FBBetSpider(Spider):
    name = "daily_spider"
    # allowed_domains = ["saishi.zgzcw.com"]
    # match_category_id = "36"
    start_urls = [

        "http://live.zgzcw.com/"

    ]

    download_delay = 2

    custom_settings = {
        'CONCURRENT_REQUESTS': '1',
    }

    """
    headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip,deflate",
    "Accept-Language": "en-US,en;q=0.8,zh-TW;q=0.6,zh;q=0.4",
    "Connection": "keep-alive",
    "Content-Type":" application/x-www-form-urlencoded; charset=UTF-8",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",
    "Referer": "http://www.zhihu.com/"
    }
    """

    def parse(self, response):

        # print response.css("#main_1::text").extract_first()
        """
        for group_num in response.css("div.rounds_b ul li a::text").extract():
            print type(group_num)
            print group_num.strip()
        """

        daily_match_tr_list = response.css("table#matchTable tr")
        get_request_flag = False
        if len(daily_match_tr_list) >0:
            # get request
            get_request_flag = True
            assert len(daily_match_tr_list[0].css("th")) == 13
            daily_match_tr_list = daily_match_tr_list[1:]
        else:
            # post request
            get_request_flag = False
            daily_match_tr_list = response.css("tr")
        for daily_match_row_selt in daily_match_tr_list:
            # firsttime = daily_match_row_selt.css("::attr(firsttime)").extract_first().strip()
            # lasttime = bookmaker_row_selt.css("::attr(lasttime)").extract_first().strip()
            td_list = daily_match_row_selt.css("td")
            assert len(td_list) == 13

            # item = EuropeOddsItem()
            # match_category = td_list[1].css("a span::text").extract_first().strip()
            match_category_href = td_list[1].css("a::attr(href)").extract_first().strip()
            daily_match_group = td_list[2].css("::text").extract_first().strip()
            # start_time = td_list[3].css("::attr(date)").extract_first().strip()

            if "东准决赛" == daily_match_group:
                pass

            # start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M")
            home = td_list[5].css("span a::text").extract_first().strip()
            away = td_list[7].css("span a::text").extract_first().strip()
            # daily_unique_code = start_time + "_" + home + "_" + away
            daily_unique_code = home + "_" + away

            match_type = re.search(r'http://saishi.zgzcw.com/soccer/([^/]*)', match_category_href).group(1)
            if "league" == match_type:
                # match_category_id = re.search(r'http://saishi.zgzcw.com/soccer/league/(\d+)/',
                #                               match_category_href).group(1)
                daily_match_group_re = re.search(r'(\d+)', daily_match_group)
                if daily_match_group_re:
                    daily_match_group = daily_match_group_re.group(1)

            elif "cup" == match_type:
                pass
                # match_category_id = re.search(r'http://saishi.zgzcw.com/soccer/cup/(\d+)/', match_category_href).group(1)
            else:
                return
            # start_time = td_list[3].css("::attr(date))").extract_first().strip()
            if "分组赛南" == daily_match_group:
                pass
            daily_match_simple_info_dict = {"daily_match_group": daily_match_group,
                                            "daily_unique_code": daily_unique_code}
            yield scrapy.Request(match_category_href, callback=self.match_parse,
                                 meta={'proxy': proxy_addres,
                                       'daily_match_simple_info_dict': daily_match_simple_info_dict}, dont_filter = True)

        #scrapy yesteraday daily match data info
        # yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        if get_request_flag:
            # get request
            yesterday = (
            datetime.strptime(response.css("select#matchSel option[selected='selected']::text").extract_first().strip(),
                              "%Y-%m-%d") - timedelta(days=1)). \
                strftime('%Y-%m-%d')
            yield scrapy.FormRequest("http://live.zgzcw.com/ls/AllData.action",
                                     formdata={"code": '201', "date": yesterday,
                                               "ajax": "true"},
                                     callback=self.parse,
                                     meta={'proxy': proxy_addres}, dont_filter = True)


    def match_parse(self, response):

        # print response.css("#main_1::text").extract_first()
        """
        for group_num in response.css("div.rounds_b ul li a::text").extract():
            print type(group_num)
            print group_num.strip()
        """
        daily_match_simple_info_dict = response.meta['daily_match_simple_info_dict']
        daily_match_group = daily_match_simple_info_dict['daily_match_group']
        daily_unique_code = daily_match_simple_info_dict['daily_unique_code']
        if "东准决赛" == daily_match_group:
            pass
        match_category_url = response.url
        match_type = re.search(r'http://saishi.zgzcw.com/soccer/([^/]*)', match_category_url).group(1)
        if "league" == match_type:
            match_category_id = re.search(r'http://saishi.zgzcw.com/soccer/league/(\d+)', match_category_url).group(1)
        elif "cup" == match_type:
            match_category_id = re.search(r'http://saishi.zgzcw.com/soccer/cup/(\d+)', match_category_url).group(1)
        else:
            return
        match_category = response.css("div.left div.left_head dl.head_dl dd::text").extract_first().strip()
        if (-1 != match_category.find('[')):
            match_category = match_category[match_category.index('[') + 1:]
        if (-1 != match_category.find(']')):
            match_category = match_category[:match_category.index(']')]

        page_title = response.css("title::text").extract_first().strip()
        match_season_re = re.search(r'^\d+-\d+', page_title)
        if match_season_re:
            match_season = match_season_re.group(0)
        else:
            match_season = re.search(r'^\d+', page_title).group(0)
        # if match_category in single_season_match_category_list:
        #
        # else:
        #     match_season = re.search(r'^\d+-\d+', match_season).group(0)
        if "league" == match_type:
            find_match = False
            for round_li_selt in response.css("ul#fjsai li"):
                if find_match:
                    break
                match_round = round_li_selt.css("::text").extract_first().strip()
                if match_round == "常规赛":
                    for group_selt in response.css("div.box.luncib em"):

                        group_name = group_selt.css("::text").extract_first().strip()
                        if group_name == daily_match_group:
                            big_five_leagues_group_table_dict = {"match_season": match_season,
                                                                 "match_category": match_category, "match_round": match_round,
                                                                 "group_name": group_name,"daily_unique_code":daily_unique_code}
                            yield scrapy.FormRequest("http://saishi.zgzcw.com/summary/liansaiAjax.action",
                                                     formdata={"source_league_id": match_category_id, "currentRound": group_name,
                                                               "season": match_season, "seasonType": ""},
                                                     callback=self.parse_big_five_leagues_group_table,
                                                     meta={'proxy': proxy_addres,
                                                           'big_five_leagues_group_table_dict': big_five_leagues_group_table_dict}, dont_filter=True)
                            find_match = True
                            break
                elif match_round == "附加赛":
                    # for group_selt in response.css("div.rounds.rounds_new div.rounds_b ul em"):
                    #     group_name = group_selt.css("::text").extract_first().strip()
                    #     if group_name == daily_match_group:
                    big_five_leagues_group_table_dict = {"match_season": match_season,
                                                         "match_category": match_category, "match_round": match_round,
                                                         "group_name": daily_match_group,"daily_unique_code":daily_unique_code}
                    yield scrapy.FormRequest("http://saishi.zgzcw.com/summary/liansaifjAction.action",
                                             formdata={"source_league_id": match_category_id, "hostTeamId": '0',"guestTeamId":'0',
                                                       "season": match_season, "seasonType": "1"},
                                             callback=self.parse_big_five_leagues_group_table,
                                             meta={'proxy': proxy_addres,
                                                   'big_five_leagues_group_table_dict': big_five_leagues_group_table_dict}, dont_filter=True)

                else:
                    pass

        elif "cup" == match_type:
            find_match = False
            for round_li_selt in response.css("ul#tabs9 li"):
                if find_match:
                    break
                match_round = round_li_selt.css("::text").extract_first().strip()
                round_li_id = round_li_selt.css("::attr(id)").extract_first().strip()
                group_tab_selt_id = "tabs9_" + round_li_id
                for group_selt in response.css("ul#" + group_tab_selt_id + " div.rounds_b ul li"):
                    group_id = group_selt.css("::attr(id)").extract_first().strip()
                    group_name = group_selt.css("a::text").extract_first().strip()
                    if match_round == daily_match_group:
                        div_id = "div_" + group_id
                        # print div_id
                        group_tab_selt = response.css("#" + div_id + " table.zstab")
                        items = self.group_tab_selt_process(response, group_tab_selt, match_season,
                                                    match_category,
                                                    match_round, group_name, daily_unique_code)
                        if len(items) == 1:
                            yield items[0]
                            for match_data_request in self.match_data_process(response, items[0]):
                                yield match_data_request
                            find_match = True
                            break
        else:
            return
            # for international_continent_catgory_li_selt in response.css("div.team_lian div.leagueSelBox ul.left2.chooseHead li"):
            # 	international_continent_catgory_id = international_continent_catgory_li_selt.css("::attr(id)").extract_first().strip()
            # 	international_continent_catgory_name = international_continent_catgory_li_selt.css(
            # 		"::attr(data-id)").extract_first().strip()
            # 	if international_continent_catgory_name == "欧洲赛事":
            # 		# for international_continent_catgory_tab_selt in response.css("div.team_lian div.leagueSelBox.clearfix div.gamesSelect.fl div"):
            # 		international_continent_catgory_tab_selt = response.css("div.team_lian div.leagueSelBox div.gamesSelect.fl div.right2.rlea."
            # 																+international_continent_catgory_name+"_content")
            # 		for match_category_dd_selt in international_continent_catgory_tab_selt.css("dl.league dd"):
            # 			match_category_name = match_category_dd_selt.css("a::text").extract_first().strip()
            # 			match_category_href = match_category_dd_selt.css("a::attr(href)").extract_first().strip()
            # 			if match_category_name in europe_big_five_football_leagues_name_list:
            # 				self.match_category_id = re.search(r'/soccer/league/(\d+)/', match_category_href).group(1)
            # 				yield scrapy.Request(response.urljoin("http://saishi.zgzcw.com" + match_category_href), callback=self.parse)

    def parse_big_five_leagues_group_table(self, response):
        big_five_leagues_group_table_dict = response.meta['big_five_leagues_group_table_dict']
        match_round = big_five_leagues_group_table_dict['match_round']
        if match_round == "常规赛":
            group_tab_id = "table#tab_" + big_five_leagues_group_table_dict['group_name'] + ".zstab"
            group_tab_selt = response.css(group_tab_id)
        elif match_round == "附加赛":
            group_tab_id = "table#tab_.zstab"
            group_tab_selt = response.css(group_tab_id)
        else:
            pass

        items = self.group_tab_selt_process(response, group_tab_selt,
                                    big_five_leagues_group_table_dict['match_season'],
                                    big_five_leagues_group_table_dict['match_category'],
                                    big_five_leagues_group_table_dict['match_round'],
                                    big_five_leagues_group_table_dict['group_name'],
                                    big_five_leagues_group_table_dict['daily_unique_code'])
        if len(items) == 1:
            yield items[0]
            for match_data_request in self.match_data_process(response, items[0]):
                yield match_data_request

    def op_parse(self, response):

        match_id = response.meta['match_id']
        # print '###################'
        # print match_id
        # print len(response.css("#data-body table tr"))
        bookmaker_tr_list = response.css("#data-body table tr")
        for bookmaker_row_selt in bookmaker_tr_list:

            firsttime = bookmaker_row_selt.css("::attr(firsttime)").extract_first().strip()
            # lasttime = bookmaker_row_selt.css("::attr(lasttime)").extract_first().strip()
            td_list = bookmaker_row_selt.css("td")
            assert len(td_list) == 17

            # item = EuropeOddsItem()
            bookmaker = td_list[1].css("::text").extract_first().strip()

            # if (bookmaker != "平均欧赔"):
            if bookmaker in bookmaker_list:
                home_first_odds = td_list[2].css("::attr(data)").extract_first().strip()
                draw_first_odds = td_list[3].css("::attr(data)").extract_first().strip()
                away_first_odds = td_list[4].css("::attr(data)").extract_first().strip()

                update_history_href = td_list[5].css("a::attr(href)").extract_first().strip()

                if (td_list[8].css("em::attr(title)").extract_first() == None):
                    latest_update_time = "更新时间：赛前0分"
                else:
                    latest_update_time = td_list[8].css("em::attr(title)").extract_first().strip()
                # print latest_update_time
                # return

                home_prob = td_list[9].css("::attr(data)").extract_first().strip()
                draw_prob = td_list[10].css("::attr(data)").extract_first().strip()
                away_prob = td_list[11].css("::attr(data)").extract_first().strip()

                home_kellyindex = td_list[12].css("::attr(data)").extract_first().strip()
                draw_kellyindex = td_list[13].css("::attr(data)").extract_first().strip()
                away_kellyindex = td_list[14].css("::attr(data)").extract_first().strip()

                return_rate = td_list[15].css("::attr(data)").extract_first().strip()

                hypothesis_first_index_dict = {'match_id': match_id, 'bookmaker': bookmaker,
                                               'home_first_odds': home_first_odds, 'draw_first_odds': draw_first_odds,
                                               'away_first_odds': \
                                                   away_first_odds, 'firsttime': firsttime,
                                               'latest_update_time': latest_update_time, 'home_prob': home_prob,
                                               'draw_prob': draw_prob, \
                                               'away_prob': away_prob, 'home_kellyindex': home_kellyindex,
                                               'draw_kellyindex': draw_kellyindex, 'away_kellyindex': away_kellyindex, \
                                               'return_rate': return_rate}

                update_history_href = response.urljoin(update_history_href)
                yield scrapy.Request(update_history_href, callback=self.op_update_history_parse,
                                     meta={'proxy': proxy_addres,
                                           'hypothesis_first_index_dict': hypothesis_first_index_dict})

    def op_update_history_parse(self, response):
        # match_id = response.meta['match_id']
        hypothesis_first_index_dict = response.meta['hypothesis_first_index_dict']
        # if (len(response.css("table.dxzkt-tab tr")) == 2):
        item = EuropeOddsItem()
        item['item_id'] = 'EuropeOdds'

        item['bookmaker'] = hypothesis_first_index_dict['bookmaker']
        item['update_time'] = hypothesis_first_index_dict['firsttime']
        item['relative_match_update_time'] = hypothesis_first_index_dict['latest_update_time']
        item['match_id'] = hypothesis_first_index_dict['match_id']
        item['home_odds'] = hypothesis_first_index_dict['home_first_odds']
        item['draw_odds'] = hypothesis_first_index_dict['draw_first_odds']
        item['away_odds'] = hypothesis_first_index_dict['away_first_odds']

        item['home_prob'] = hypothesis_first_index_dict['home_prob']
        item['draw_prob'] = hypothesis_first_index_dict['draw_prob']
        item['away_prob'] = hypothesis_first_index_dict['away_prob']
        item['home_kellyindex'] = hypothesis_first_index_dict['home_kellyindex']
        item['draw_kellyindex'] = hypothesis_first_index_dict['draw_kellyindex']
        item['away_kellyindex'] = hypothesis_first_index_dict['away_kellyindex']
        item['return_rate'] = hypothesis_first_index_dict['return_rate']
        item['unique_code'] = item['match_id'] + "_" + item['bookmaker'] + "_" + item['update_time']
        item['source_url'] = response.url
        yield item
        if (len(response.css("table.dxzkt-tab tr")) > 2):

            for history_item_selt in response.css("table.dxzkt-tab tr")[2:]:
                td_list = history_item_selt.css("td")
                assert len(td_list) == 11

                item = EuropeOddsItem()
                item['item_id'] = 'EuropeOdds'
                item['bookmaker'] = hypothesis_first_index_dict['bookmaker']
                item['match_id'] = hypothesis_first_index_dict['match_id']
                item['update_time'] = td_list[1].css("::text").extract_first().strip()
                item['relative_match_update_time'] = td_list[2].css("span::text").extract_first().strip()

                pattern = re.compile(r'^\d+.\d+')

                item['home_odds'] = pattern.search(td_list[3].css("span::text").extract_first().strip()).group(0)
                # u'1.42'.isdecimal() is False
                # if (td_list[3].css("span::text").extract_first().strip().isdecimal()):
                # 	item['home_odds'] = td_list[3].css("span::text").extract_first().strip()
                # else:
                # 	item['home_odds'] = td_list[3].css("span::text").extract_first().strip()[:-1]

                item['draw_odds'] = pattern.search(td_list[4].css("span::text").extract_first().strip()).group(0)
                # if (td_list[4].css("span::text").extract_first().strip().isdecimal()):
                # 	item['draw_odds'] = td_list[4].css("span::text").extract_first().strip()
                # else:
                # 	item['draw_odds'] = td_list[4].css("span::text").extract_first().strip()[:-1]

                item['away_odds'] = pattern.search(td_list[5].css("span::text").extract_first().strip()).group(0)
                # if (td_list[5].css("span::text").extract_first().strip().isdecimal()):
                # 	item['away_odds'] = td_list[5].css("span::text").extract_first().strip()
                # else:
                # 	item['away_odds'] = td_list[5].css("span::text").extract_first().strip()[:-1]


                item['home_prob'] = td_list[6].css("::text").extract_first().strip()

                item['away_prob'] = td_list[7].css("::text").extract_first().strip()

                item['draw_prob'] = unicode(Decimal(100) - Decimal(item['home_prob']) - Decimal(item['away_prob']))

                item['home_kellyindex'] = td_list[8].css("::text").extract_first().strip()
                item['draw_kellyindex'] = ""
                item['away_kellyindex'] = td_list[9].css("::text").extract_first().strip()
                item['return_rate'] = td_list[10].css("::text").extract_first().strip()
                item['unique_code'] = item['match_id'] + "_" + item['bookmaker'] + "_" + item['update_time']
                item['source_url'] = response.url
                yield item
        elif (len(response.css("table.dxzkt-tab tr")) != 2):
            raise Exception

    def yp_parse(self, response):

        match_id = response.meta['match_id']

        for bookmaker_row_selt in response.css("#data-body table tr"):
            firsttime = bookmaker_row_selt.css("::attr(firsttime)").extract_first().strip()
            td_list = bookmaker_row_selt.css("td")
            assert len(td_list) == 15

            # item = AsianOddsItem()

            bookmaker = td_list[1].css("::text").extract_first().strip()

            home_first_odds = td_list[2].css("::text").extract_first().strip()
            first_handicap = td_list[3].css("::text").extract_first().strip()
            away_first_odds = td_list[4].css("::text").extract_first().strip()

            update_history_href = td_list[5].css("a::attr(href)").extract_first().strip()

            if (td_list[8].css("em::attr(title)").extract_first() == None):
                latest_update_time = "更新时间：赛前0分"
            else:
                latest_update_time = td_list[8].css("em::attr(title)").extract_first().strip()

            home_prob = td_list[9].css("::text").extract_first().strip()
            away_prob = td_list[10].css("::text").extract_first().strip()

            home_kellyindex = td_list[11].css("::text").extract_first().strip()
            away_kellyindex = td_list[12].css("::text").extract_first().strip()
            return_rate = td_list[13].css("::text").extract_first().strip()

            hypothesis_first_index_dict = {'match_id': match_id, 'bookmaker': bookmaker,
                                           'home_first_odds': home_first_odds, 'first_handicap': first_handicap,
                                           'away_first_odds': \
                                               away_first_odds, 'firsttime': firsttime,
                                           'latest_update_time': latest_update_time, 'home_prob': home_prob, \
                                           'away_prob': away_prob, 'home_kellyindex': home_kellyindex,
                                           'away_kellyindex': away_kellyindex, \
                                           'return_rate': return_rate}

            update_history_href = response.urljoin(update_history_href)
            yield scrapy.Request(update_history_href, callback=self.yp_update_history_parse,
                                 meta={'proxy': proxy_addres,
                                       'hypothesis_first_index_dict': hypothesis_first_index_dict})

    def yp_update_history_parse(self, response):
        # match_id = response.meta['match_id']
        hypothesis_first_index_dict = response.meta['hypothesis_first_index_dict']
        # if (len(response.css("table.dxzkt-tab tr")) == 2):
        item = AsianOddsItem()
        item['item_id'] = 'AsianOdds'

        item['bookmaker'] = hypothesis_first_index_dict['bookmaker']
        item['update_time'] = hypothesis_first_index_dict['firsttime']
        item['relative_match_update_time'] = hypothesis_first_index_dict['latest_update_time']
        item['match_id'] = hypothesis_first_index_dict['match_id']
        item['home_odds'] = hypothesis_first_index_dict['home_first_odds']
        item['handicap'] = hypothesis_first_index_dict['first_handicap']
        item['away_odds'] = hypothesis_first_index_dict['away_first_odds']

        item['home_prob'] = hypothesis_first_index_dict['home_prob']
        item['away_prob'] = hypothesis_first_index_dict['away_prob']
        item['home_kellyindex'] = hypothesis_first_index_dict['home_kellyindex']
        item['away_kellyindex'] = hypothesis_first_index_dict['away_kellyindex']
        item['return_rate'] = hypothesis_first_index_dict['return_rate']
        item['unique_code'] = item['match_id'] + "_" + item['bookmaker'] + "_" + item['update_time']
        item['source_url'] = response.url
        yield item
        if (len(response.css("table.dxzkt-tab tr")) > 2):

            for history_item_selt in response.css("table.dxzkt-tab tr")[2:]:
                td_list = history_item_selt.css("td")
                assert len(td_list) == 11

                item = AsianOddsItem()
                item['item_id'] = 'AsianOdds'
                item['bookmaker'] = hypothesis_first_index_dict['bookmaker']
                item['match_id'] = hypothesis_first_index_dict['match_id']
                item['update_time'] = td_list[1].css("::text").extract_first().strip()
                item['relative_match_update_time'] = td_list[2].css("span::text").extract_first().strip()

                if (td_list[3].css("span::text").extract_first().strip().isdecimal()):
                    item['home_odds'] = td_list[3].css("span::text").extract_first().strip()
                else:
                    item['home_odds'] = td_list[3].css("span::text").extract_first().strip()[:-1]

                item['handicap'] = td_list[4].css("span::text").extract_first().strip()

                if (td_list[5].css("span::text").extract_first().strip().isdecimal()):
                    item['away_odds'] = td_list[5].css("span::text").extract_first().strip()
                else:
                    item['away_odds'] = td_list[5].css("span::text").extract_first().strip()[:-1]

                item['home_prob'] = td_list[6].css("::text").extract_first().strip()
                item['away_prob'] = td_list[7].css("::text").extract_first().strip()

                item['home_kellyindex'] = td_list[8].css("::text").extract_first().strip()
                item['away_kellyindex'] = td_list[9].css("::text").extract_first().strip()
                item['return_rate'] = td_list[10].css("::text").extract_first().strip()
                item['unique_code'] = item['match_id'] + "_" + item['bookmaker'] + "_" + item['update_time']
                item['source_url'] = response.url
                yield item
        elif (len(response.css("table.dxzkt-tab tr")) != 2):
            raise Exception

    def dx_parse(self, response):

        match_id = response.meta['match_id']

        for bookmaker_row_selt in response.css("#data-body table tr"):
            td_list = bookmaker_row_selt.css("td")
            firsttime = bookmaker_row_selt.css("::attr(firsttime)").extract_first().strip()
            assert len(td_list) == 15

            # item = OverUnderOddsItem()

            bookmaker = td_list[1].css("::text").extract_first().strip()
            over_first_odds = td_list[2].css("::text").extract_first().strip()
            first_handicap = td_list[3].css("::text").extract_first().strip()
            under_first_odds = td_list[4].css("::text").extract_first().strip()

            update_history_href = td_list[5].css("a::attr(href)").extract_first().strip()

            if (td_list[8].css("em::attr(title)").extract_first() == None):
                latest_update_time = "更新时间：赛前0分"
            else:
                latest_update_time = td_list[8].css("em::attr(title)").extract_first().strip()

            over_prob = td_list[9].css("::text").extract_first().strip()
            under_prob = td_list[10].css("::text").extract_first().strip()

            over_kellyindex = td_list[11].css("::text").extract_first().strip()
            under_kellyindex = td_list[12].css("::text").extract_first().strip()

            return_rate = td_list[13].css("::text").extract_first().strip()

            hypothesis_first_index_dict = {'match_id': match_id, 'bookmaker': bookmaker,
                                           'over_first_odds': over_first_odds, 'first_handicap': first_handicap,
                                           'under_first_odds': \
                                               under_first_odds, 'firsttime': firsttime,
                                           'latest_update_time': latest_update_time, 'over_prob': over_prob, \
                                           'under_prob': under_prob, 'over_kellyindex': over_kellyindex,
                                           'under_kellyindex': under_kellyindex, \
                                           'return_rate': return_rate}

            update_history_href = response.urljoin(update_history_href)
            yield scrapy.Request(update_history_href, callback=self.dx_update_history_parse,
                                 meta={'proxy': proxy_addres,
                                       'hypothesis_first_index_dict': hypothesis_first_index_dict})

            """

            print td_list[1].css("::text").extract_first().strip()

            print td_list[2].css("::text").extract_first().strip()
            print td_list[3].css("::text").extract_first().strip()
            print td_list[4].css("::text").extract_first().strip()

            if( len(td_list[5].css("a::text").extract_first().strip()) == 5):
                print td_list[5].css("a::text").extract_first().strip()[:-1]
            elif (len(td_list[5].css("a::text").extract_first().strip()) == 4):
                print td_list[5].css("a::text").extract_first().strip()

            print td_list[6].css("a::text").extract_first().strip()

            if (len(td_list[7].css("a::text").extract_first().strip()) == 5):
                print td_list[7].css("a::text").extract_first().strip()[:-1]
            elif (len(td_list[7].css("a::text").extract_first().strip()) == 4):
                print td_list[7].css("a::text").extract_first().strip()

            print td_list[9].css("::text").extract_first().strip()
            print td_list[10].css("::text").extract_first().strip()

            print td_list[11].css("::text").extract_first().strip()
            print td_list[12].css("::text").extract_first().strip()


            print td_list[13].css("::text").extract_first().strip()
            """

    def dx_update_history_parse(self, response):
        # match_id = response.meta['match_id']
        hypothesis_first_index_dict = response.meta['hypothesis_first_index_dict']
        # if (len(response.css("table.dxzkt-tab tr")) == 2):
        item = OverUnderOddsItem()
        item['item_id'] = 'OverUnderOdds'

        item['bookmaker'] = hypothesis_first_index_dict['bookmaker']
        item['update_time'] = hypothesis_first_index_dict['firsttime']
        item['relative_match_update_time'] = hypothesis_first_index_dict['latest_update_time']
        item['match_id'] = hypothesis_first_index_dict['match_id']
        item['over_odds'] = hypothesis_first_index_dict['over_first_odds']
        item['handicap'] = hypothesis_first_index_dict['first_handicap']
        item['under_odds'] = hypothesis_first_index_dict['under_first_odds']

        item['over_prob'] = hypothesis_first_index_dict['over_prob']
        item['under_prob'] = hypothesis_first_index_dict['under_prob']
        item['over_kellyindex'] = hypothesis_first_index_dict['over_kellyindex']
        item['under_kellyindex'] = hypothesis_first_index_dict['under_kellyindex']
        item['return_rate'] = hypothesis_first_index_dict['return_rate']
        item['unique_code'] = item['match_id'] + "_" + item['bookmaker'] + "_" + item['update_time']
        item['source_url'] = response.url
        yield item
        if (len(response.css("table.dxzkt-tab tr")) > 2):

            for history_item_selt in response.css("table.dxzkt-tab tr")[2:]:
                td_list = history_item_selt.css("td")
                assert len(td_list) == 11

                item = OverUnderOddsItem()
                item['item_id'] = 'OverUnderOdds'
                item['bookmaker'] = hypothesis_first_index_dict['bookmaker']
                item['match_id'] = hypothesis_first_index_dict['match_id']
                item['update_time'] = td_list[1].css("::text").extract_first().strip()
                item['relative_match_update_time'] = td_list[2].css("span::text").extract_first().strip()

                if (td_list[3].css("span::text").extract_first().strip().isdecimal()):
                    item['over_odds'] = td_list[3].css("span::text").extract_first().strip()
                else:
                    item['over_odds'] = td_list[3].css("span::text").extract_first().strip()[:-1]

                item['handicap'] = td_list[4].css("span::text").extract_first().strip()

                if (td_list[5].css("span::text").extract_first().strip().isdecimal()):
                    item['under_odds'] = td_list[5].css("span::text").extract_first().strip()
                else:
                    item['under_odds'] = td_list[5].css("span::text").extract_first().strip()[:-1]

                item['over_prob'] = td_list[6].css("::text").extract_first().strip()
                item['under_prob'] = td_list[7].css("::text").extract_first().strip()

                item['over_kellyindex'] = td_list[8].css("::text").extract_first().strip()
                item['under_kellyindex'] = td_list[9].css("::text").extract_first().strip()
                item['return_rate'] = td_list[10].css("::text").extract_first().strip()
                item['unique_code'] = item['match_id'] + "_" + item['bookmaker'] + "_" + item['update_time']
                item['source_url'] = response.url
                yield item
        elif (len(response.css("table.dxzkt-tab tr")) != 2):
            raise Exception

    def tj_parse(self, response):

        # 44,1,天亚高 (助攻:桑塔纳)
        match_id = response.meta['match_id']
        item = StrokeAnalysisItem()
        item['match_id'] = match_id
        item['item_id'] = 'StrokeAnalysis'
        home_situation_item_dict = {}
        away_situation_item_dict = {}
        for li_selt in response.css("div.jstj-l ul.zhudui li"):
            situation_item = li_selt.css("::attr(data)").extract_first().strip()
            situation_item_list = situation_item.split(",")
            assert len(situation_item_list) == 3
            time = situation_item_list[0]
            home_situation_item_dict[time] = situation_item

        home_situation_item_json = json.dumps(home_situation_item_dict)

        for li_selt in response.css("div.jstj-l ul.kedui li"):
            situation_item = li_selt.css("::attr(data)").extract_first().strip()
            situation_item_list = situation_item.split(",")
            assert len(situation_item_list) == 3
            time = situation_item_list[0]
            away_situation_item_dict[time] = situation_item

        away_situation_item_json = json.dumps(away_situation_item_dict)

        item['home_situation'] = home_situation_item_json
        item['away_situation'] = away_situation_item_json
        for jstj_row_selt in response.css("div.jstj-r div.jstj-r-con table tr"):
            td_list = jstj_row_selt.css("td")
            assert len(td_list) == 5
            home_statis = td_list[0].css("::text").extract_first().strip()

            statis_desc = td_list[2].css("::text").extract_first().strip()

            away_statis = td_list[4].css("::text").extract_first().strip()

            if (statis_desc == "射门次数"):
                item['home_shoot_times'] = home_statis
                item['away_shoot_times'] = away_statis
            elif (statis_desc == "射正次数"):
                item['home_shoot_target_times'] = home_statis
                item['away_shoot_target_times'] = away_statis
            elif (statis_desc == "犯规次数"):
                item['home_foul_times'] = home_statis
                item['away_foul_times'] = away_statis
            elif (statis_desc == "角球次数"):
                item['home_corner_times'] = home_statis
                item['away_corner_times'] = away_statis
            elif (statis_desc == "任意球次数"):
                item['home_freekick_times'] = home_statis
                item['away_freekick_times'] = away_statis
            elif (statis_desc == "越位次数"):
                item['home_offside_times'] = home_statis
                item['away_offside_times'] = away_statis
            elif (statis_desc == "黄牌数"):
                item['home_yellowcard_times'] = home_statis
                item['away_yellowcard_times'] = away_statis
            elif (statis_desc == "红牌数"):
                item['home_redcard_times'] = home_statis
                item['away_redcard_times'] = away_statis
            elif (statis_desc == "控球时间"):
                item['home_ballcontrol_time_ratio'] = home_statis
                item['away_ballcontrol_time_ratio'] = away_statis
            elif (statis_desc == "头球"):
                item['home_headballs'] = home_statis
                item['away_headballs'] = away_statis
            elif (statis_desc == "救球"):
                item['home_saveballs'] = home_statis
                item['away_saveballs'] = away_statis
        item['source_url'] = response.url
        yield item

        """
        for li_selt in response.css("div.jstj-l ul.zhudui li"):
            print li_selt.css("::attr(data)").extract_first().strip()

        for li_selt in response.css("div.jstj-l ul.kedui li"):
            print li_selt.css("::attr(data)").extract_first().strip()


        for jstj_row_selt in response.css("div.jstj-r div.jstj-r-con table tr"):
            td_list = jstj_row_selt.css("td")
            assert len(td_list) == 5

            print td_list[0].css("::text").extract_first().strip()

            print td_list[2].css("::text").extract_first().strip()

            print td_list[4].css("::text").extract_first().strip()
        """

    def group_tab_selt_process(self, response, group_tab_selt, match_season, match_category,
                               match_round, group_name, daily_unique_code):
        # print group_tab_selt.extract_first()
        match_tab_name_list = []
        for match_tab_name in group_tab_selt.css("thead tr th::text").extract():
            match_tab_name_list.append(match_tab_name.strip())
        assert len(match_tab_name_list) == 7
        # print group_tab_selt.extract_first()
        match_rows_selt = group_tab_selt.css("tr")
        # print len(match_rows_selt)
        # print type(match_rows_selt)
        items = []
        for match_row_selt in match_rows_selt:
            td_list = match_row_selt.css("td")
            # print type(td_list)
            # print len(td_list)
            # for td in
            # print td_list.extract()[0]
            if len(td_list) > 0:
                item = MatchItem()

                item['start_time'] = td_list[0].css("::text").extract_first().strip()
                item['home'] = td_list[1].css("a::text").extract_first().strip()

                item['away'] = td_list[3].css("a::text").extract_first().strip()

                item['unique_code'] = item['start_time'] + "_" + item['home'] + "_" + item['away']

                # if item['unique_code'] == daily_unique_code: time not identity
                if item['home'] + "_" + item['away'] == daily_unique_code:
                    item['match_category'] = match_category
                    item['match_season'] = match_season
                    item['match_group'] = group_name
                    item['match_round'] = match_round
                    item['source_url'] = response.url
                    item['item_id'] = 'Match'
                    item['score'] = td_list[2].css("::text").extract_first().strip()
                    item['half_score'] = td_list[4].css("::text").extract_first().strip()
                    item['handicap'] = td_list[5].css("::text").extract_first().strip()
                    op_url = td_list[6].css("a.oyx::attr(href)").extract()[1]
                    op_url = response.urljoin(op_url)
                    match_data_id = re.search(r'http://fenxi.zgzcw.com/(\d+)/', op_url).group(1)
                    item['match_data_id'] = match_data_id
                    items.append(item)
                    break
                else:
                    continue

                # return
                # yield item

                # print "#########"
                # print item['match_id']

                """
                yield {match_tab_name_list[0]:td_list[0].css("::text").extract_first().strip(),
                match_tab_name_list[1]: td_list[1].css("a::text").extract_first().strip(),
                match_tab_name_list[2]: td_list[2].css("::text").extract_first().strip(),
                match_tab_name_list[3]: td_list[3].css("a::text").extract_first().strip(),
                match_tab_name_list[4]: td_list[4].css("::text").extract_first().strip(),
                match_tab_name_list[5]: td_list[5].css("::text").extract_first().strip(),
                match_tab_name_list[6]: td_list[6].css("a.oyx::attr(href)").extract()
                }
                """

                # for jx_url in td_list[6].css("a.oyx::attr(href)").extract():


        return tuple(items)

    def match_data_process(self, response, item):
        requests = []
        if item['score'] != "-:-":
            # pass

            op_url = "http://fenxi.zgzcw.com/" + item['match_data_id'] + "/bjop"
            op_url = response.urljoin(op_url)
            requests.append(scrapy.Request(op_url, callback=self.op_parse,
                                           meta={'proxy': proxy_addres, 'match_id': item['match_id']}))

        # yp_url = td_list[6].css("a.oyx::attr(href)").extract()[0]
        # yp_url = response.urljoin(yp_url)
        # yield scrapy.Request(yp_url, callback = self.yp_parse, meta = {'proxy':proxy_addres,'match_id':item['match_id']})
        #
        #
        # op_url_list = op_url.split("/")[:-1]
        # dx_url = "/".join(op_url_list)+"/dxdb"
        # yield scrapy.Request(dx_url, callback = self.dx_parse, meta = {'proxy':proxy_addres,'match_id':item['match_id']})
        #
        # op_url_list = op_url.split("/")[:-1]
        # tj_url = "/".join(op_url_list)+"/zrtj"
        # yield scrapy.Request(tj_url, callback = self.tj_parse, meta = {'proxy':proxy_addres,'match_id':item['match_id']})
        return tuple(requests)