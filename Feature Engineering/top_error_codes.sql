select error_code, count(*) as ct
from public.rawpii_sms_kafka
where (date_updated) >= (timestamp '2021-05-01') AND (date_updated) <= (timestamp '2021-05-31')
group by 1
order by ct desc
limit 25;
