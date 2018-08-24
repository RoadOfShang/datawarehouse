
--------------------------------------------------------------------------------------------------------------------------------
-- 
-- 步骤：
-- 
-- 1、初始化
-- 
-- 2、增量数据
-- 
-- 3、计算新增数据
-- 
-- 4、历史数据闭链
-- 
-- 5、更新拉链表
-- 

-- 建立历史表

drop table if exists test.t_hyf_score;
create table if not exists test.t_hyf_score(
customer_id          bigint         comment '客户id',
cert_id              string          comment '身份证号',
hy_score             int            comment '火眼分',
zhima_score          int            comment '芝麻分',
cocktail_score       int            comment '鸡尾酒分',
grade_level          string          comment '彩虹等级',
status               string          comment '当前状态',
version              int            comment '版本',
start_date           string          comment '开始时间'
)
partitioned by(end_date string comment '结束时间')
stored as orcfile
;

-- 初始化数据

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

insert overwrite table test.t_hyf_score partition(end_date)

select 

t.customer_id,
t.cert_id,
t.apply_source_score as hy_score,
t.zhima_score,
t.cocktail_score,
t.color as grade_level,
'C' as status,
1 as version,
substr(t.create_time,1,10) as start_date,
'3000-12-31' as end_date

from bd_src.fk_hyf_customer t 
where t.p_day_id = '2018-05-01'
and t.system_source = '7'
and t.apply_source_score is not null
and t.color is not null 
;


-- 更新sql
-- 定义变量 

set etl_date = '2018-05-02';
set end_date = '3000-12-31';


-- 计算临时表
-- t_hyf_cur

drop table if exists test.t_hyf_cur;
create table test.t_hyf_cur as 

select 

t.customer_id,
t.cert_id,
t.apply_source_score as hy_score,
t.zhima_score,
t.cocktail_score,
t.color as grade_level,
substr(t.create_time,1,10) as create_date
-- md5(concat(coalesce(cast(t.apply_source_score as int),''),coalesce(t.zhima_score,''),coalesce(t.cocktail_score,''),coalesce(t.color,''))) as md5_str 

from bd_src.fk_hyf_customer t
where t.p_day_id = ${hiveconf:etl_date}
and t.system_source = '7'
and t.apply_source_score is not null
and t.color is not null
;

-- t_hyf_his 

drop table if exists test.t_hyf_his;
create table test.t_hyf_his as 

select 

t.customer_id,
t.cert_id,
t.hy_score,
t.zhima_score,
t.cocktail_score,
t.grade_level,
t.status,
t.version,
t.start_date,
t.end_date
-- md5(concat(coalesce(t.hy_score,''),coalesce(t.zhima_score,''),coalesce(t.cocktail_score,''),coalesce(t.grade_level,''))) as md5_str

from test.t_hyf_score t
where t.end_date = ${hiveconf:end_date}
;


-- 插入增量数据

from test.t_hyf_cur c full outer join test.t_hyf_his h on c.customer_id = h.customer_id

-- 计算闭链数据
insert overwrite table test.t_hyf_score partition(end_date)

select 

h.customer_id,
h.cert_id,
h.hy_score,
h.zhima_score,
h.cocktail_score,
h.grade_level,
'H' as status,
h.version,
h.start_date,
${hiveconf:etl_date} as end_date

where h.customer_id is not null and c.customer_id is null and (cast(c.hy_score as double) = cast(h.hy_score as double) and cast(c.zhima_score as int) = cast(h.zhima_score as int) and cast(c.cocktail_score as int) = cast(h.cocktail_score as int) and c.grade_level = h.grade_level)

-- 计算最新值
insert overwrite table test.t_hyf_score partition(end_date)

select 

case when c.customer_id is not null then c.customer_id else h.customer_id end as customer_id,
case when c.customer_id is not null then c.cert_id else h.cert_id end as cert_id,
case when c.customer_id is not null then c.hy_score else h.hy_score end as hy_score,
case when c.customer_id is not null then c.zhima_score else h.zhima_score end as zhima_score,
case when c.customer_id is not null then c.cocktail_score else h.cocktail_score end as cocktail_score,
case when c.customer_id is not null then c.grade_level else h.grade_level end as grade_level,

'C' as status,

case when c.customer_id is not null and h.customer_id is null then 1 
when c.customer_id is not null and h.customer_id is not null and (cast(c.hy_score as double) != cast(h.hy_score as double) or cast(c.zhima_score as int) != cast(h.zhima_score as int) or cast(c.cocktail_score as int) != cast(h.cocktail_score as int) or c.grade_level != h.grade_level) then h.version + 1
else h.version
end version,

case when h.customer_id is not null and (cast(c.hy_score as double) = cast(h.hy_score as double) and cast(c.zhima_score as int) = cast(h.zhima_score as int) and cast(c.cocktail_score as int) = cast(h.cocktail_score as int) and c.grade_level = h.grade_level) then h.start_date
when h.customer_id is not null and c.customer_id is null then h.start_date
else c.create_date
end as start_date,

${hiveconf:end_date} as end_date
;
