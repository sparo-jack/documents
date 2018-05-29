use football_betting;


select * from football_match where match_category = '比甲' order by id desc;

select * from football_match where match_category = '挪超' order by id desc;

select * from football_match where match_category = '葡超' order by id desc;

select * from football_match where match_category = '瑞典超' order by id desc;

select * from football_match where match_category = '俄超' order by id desc;

select * from football_match where match_category = '澳超' order by id desc;

select * from football_match where match_category = '法国杯' order by id desc;

select * from football_match where match_category = '德国杯' order by id desc;

select * from football_match where match_category = '意大利杯' order by id desc;

select * from football_match where match_category = '国王杯' order by id desc;

select * from football_match where match_category = '英足总杯' order by id desc;

select * from football_match where match_category = '荷乙' order by id desc;

select * from football_match where match_category = '英甲' order by id desc;

select * from football_match where match_category = '日乙' order by id desc;

select * from football_match where match_category = '中超' order by id desc;

select * from football_match where match_category = '亚冠' order by id desc;





select * from football_match order by id desc;

select * from football_match where match_category = '法乙' order by id desc;
select * from football_match where match_category = '日职' order by id desc;
select count(*) from football_match where status = 'FINISHED';
select count(*) from football_match where status = 'UPDATING';
select * from europe_odds order by id desc;
select count(*) from europe_odds;