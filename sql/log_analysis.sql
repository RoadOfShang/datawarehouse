
-- 不要破坏日志的json格式

select a.customer_id customer_id,b.customer_id,a.customer_flag_display customer_flag,a.ps_display project_status_display,a.detail_name detail_name,a.detail_desc detail_desc, 
a.charge_price_display as before_charge_price,b.charge_price_display as after_charge_price ,a.order_center_price_display before_order_center_price,b.order_center_price_display after_order_center_price,a.created_at
,a.detail_id,b.detail_id
from (select
id as customer_id,
get_json_object(log_data, '$.before.customer_flag_display') as customer_flag_display, -- 封装标记
get_json_object(log_data, '$.ps_display') as ps_display,  -- 项目状态(where 过滤已开跑)
get_json_object(c.col,'$.id') as detail_id,     --计费明细id
get_json_object(c.col,'$.detail_name') as detail_name,    --仓名称
get_json_object(c.col,'$.detail_desc')  as detail_desc,  -- 里程段，如60-90km 或者按趟
get_json_object(c.col,'$.charge_price_display') as charge_price_display,
get_json_object(c.col,'$.order_center_price_display')as order_center_price_display,
created_at
from fact_beeper.fact_beeper_beeper_logs
lateral view explode(split(regexp_replace(regexp_extract(get_json_object(log_data, '$.before.details'),'^\\[(.+)\\]$',1),'\\}\\,\\{', '\\}\\|\\|\\{'),'\\|\\|')) c
where key = 'diff_customer_fengzhuang_info' and p_day >'2018-01-01' and get_json_object(log_data, '$.ps_display') = "已开跑") as a join 
(select
id as customer_id,
get_json_object(log_data, '$.after.customer_flag_display') as customer_flag_display, -- 封装标记
get_json_object(log_data, '$.ps_display') as ps_display,  -- 项目状态(where 过滤已开跑)
get_json_object(c.col,'$.id') as detail_id,     --计费明细id
get_json_object(c.col,'$.detail_name') as detail_name,    --仓名称
get_json_object(c.col,'$.detail_desc')  as detail_desc,  -- 里程段，如60-90km 或者按趟
get_json_object(c.col,'$.charge_price_display') as charge_price_display,
get_json_object(c.col,'$.order_center_price_display')as order_center_price_display
from fact_beeper.fact_beeper_beeper_logs
lateral view explode(split(regexp_replace(regexp_extract(get_json_object(log_data, '$.after.details'),'^\\[(.+)\\]$',1),'\\}\\,\\{', '\\}\\|\\|\\{'),'\\|\\|')) c
where key= 'diff_customer_fengzhuang_info' and p_day >'2018-01-01' and get_json_object(log_data, '$.ps_display') = "已开跑") as b on a.detail_id = b.detail_id where (a.charge_price_display !=b.charge_price_display or a.order_center_price_display !=b.order_center_price_display) 
and a.customer_id=15099 and a.created_at='2018-01-31 11:36:55'