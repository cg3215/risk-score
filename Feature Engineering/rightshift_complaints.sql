with _7726 as (
  select count(*) as _7726, account_sid
  from public.carriercomplaints
  where date(date_time_processed) >= (TIMESTAMP '2021-05-01')
  group by 2
),
complaints as (
  SELECT accountsid as account_sid,
  	(case
    	when (array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint%' ESCAPE '^' then 'Carrier Complaint'
    	when (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_%' ESCAPE '^' then 'Consumer Complaint'
    end) AS issuetype,
  	COUNT(*) AS ct
  FROM public.rawpii_zendesk_tickets_2017  AS zendesk_tickets_2017
  LEFT JOIN public.zendesk_groups  AS zendesk_groups ON zendesk_groups.id = zendesk_tickets_2017.group_id
  WHERE
  (zendesk_tickets_2017.created_at at time zone 'US/Pacific'  >= TIMESTAMP '2021-05-01')
  and (zendesk_tickets_2017.created_at at time zone 'US/Pacific'  <= TIMESTAMP '2021-05-31')
  AND ((((zendesk_tickets_2017.updated_at at time zone 'US/Pacific' ) >= ((DATE_ADD('week', -89, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))
    AND (zendesk_tickets_2017.updated_at at time zone 'US/Pacific' ) < ((DATE_ADD('week', 90, DATE_ADD('week', -89, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)))))))))) AND
   (((array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint^_^_sms/voice^_^_cannabis^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint^_^_sms/voice^_^_harassment^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint^_^_sms/voice^_^_other^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint^_^_political^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint^_^_sms/voice^_^_spam^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_cannabis^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_harassment^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_other^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_political^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_spam^_abo%' ESCAPE '^'))
  GROUP BY 1,2
  ORDER BY 3 DESC
),
attempted_sends as (
  select count(*) as attempts, error_code, account_sid
   from public.rawpii_sms_kafka as fraud_sms_kafka
   where (NOT COALESCE(
                bitwise_and(fraud_sms_kafka.flags, bitwise_shift_left(1, 0, 32)) >=
                1, FALSE))
   AND (fraud_sms_kafka.date_created ) >= (TIMESTAMP '2021-05-01')
   AND (fraud_sms_kafka.date_created ) <= (TIMESTAMP '2021-05-31')
   AND (fraud_sms_kafka.data_load_date ) >= (TIMESTAMP '2021-05-01')
   AND (fraud_sms_kafka.data_load_date ) <= (TIMESTAMP '2021-05-31')
   group by 3,2
)

select account_sid, _7726, complaints_carrier, complaints_consumer, (coalesce(_7726,0)+coalesce(complaints_carrier,0)+coalesce(complaints_consumer,0))/(denom*1.00000000) as complaint_rate from (
  SELECT a.account_sid, _7726, complaints_carrier, complaints_consumer, sum(attempts) as denom
  from attempted_sends a
  left join (select account_sid, sum(ct) as complaints_carrier from complaints where issuetype='Carrier Complaint' group by 1) as complaints_carrier on complaints_carrier.account_sid=a.account_sid
  left join (select account_sid, sum(ct) as complaints_consumer from complaints where issuetype='Consumer Complaint' group by 1) as complaints_consumer on complaints_consumer.account_sid=a.account_sid
  left join _7726 on _7726.account_sid=a.account_sid
--   where a.error_code=0
  group by 1,2,3,4
)
group by 1,2,3,4,5
order by 3 desc

---------

with _7726 as (
  select count(*) as _7726, DATE_FORMAT(date_time_processed, '%Y-%m') as wk
  from public.carriercomplaints
  where date(date_time_processed) >= (TIMESTAMP '2021-05-01')
  group by 2
),
complaints as (
  SELECT
  	DATE_FORMAT(zendesk_tickets_2017.created_at at time zone 'US/Pacific', '%Y-%m') AS wk,
  	case
    	when (array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint%' ESCAPE '^' then 'Carrier Complaint'
    	when (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_%' ESCAPE '^' then 'Consumer Complaint'
    end AS issuetype,
  	COUNT(*) AS ct
  FROM public.rawpii_zendesk_tickets_2017  AS zendesk_tickets_2017
  LEFT JOIN public.zendesk_groups  AS zendesk_groups ON zendesk_groups.id = zendesk_tickets_2017.group_id

  WHERE (zendesk_tickets_2017.created_at at time zone 'US/Pacific'  >= TIMESTAMP '2021-05-01')
  AND ((((zendesk_tickets_2017.updated_at at time zone 'US/Pacific' ) >= ((DATE_ADD('week', -89, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))
    AND (zendesk_tickets_2017.updated_at at time zone 'US/Pacific' ) < ((DATE_ADD('week', 90, DATE_ADD('week', -89, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))))))
  AND (((array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint^_^_sms/voice^_^_cannabis^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint^_^_sms/voice^_^_harassment^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint^_^_sms/voice^_^_other^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint^_^_political^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%carrier^_complaint^_^_sms/voice^_^_spam^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_cannabis^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_harassment^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_other^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_political^_abo%' ESCAPE '^'
    OR (array_join(zendesk_tickets_2017.tags,',')) LIKE '%consumer^_^_spam^_abo%' ESCAPE '^'))
  GROUP BY 1,2
  ORDER BY 3 DESC
),
attempted_sends as (
  select count(*) as attempts, error_code, DATE_FORMAT(fraud_sms_kafka.date_created, '%Y-%m') as wk
   from public.rawpii_sms_kafka as fraud_sms_kafka
   where (NOT COALESCE(
                bitwise_and(fraud_sms_kafka.flags, bitwise_shift_left(1, 0, 32)) >=
                1, FALSE))
   AND (fraud_sms_kafka.date_created ) >= (TIMESTAMP '2021-05-01')
   AND (fraud_sms_kafka.data_load_date ) >= (TIMESTAMP '2021-05-01')
   group by 3,2
)

select wk, _7726, complaints_carrier, complaints_consumer, (coalesce(_7726,0)+coalesce(complaints_carrier,0)+coalesce(complaints_consumer,0))/(denom*1.00000000) as complaint_rate from (
  SELECT a.wk, _7726, complaints_carrier, complaints_consumer, sum(attempts) as denom
  from attempted_sends a
  left join (select wk, sum(ct) as complaints_carrier from complaints where issuetype='Carrier Complaint' group by 1) as complaints_carrier on complaints_carrier.wk=a.wk
  left join (select wk, sum(ct) as complaints_consumer from complaints where issuetype='Consumer Complaint' group by 1) as complaints_consumer on complaints_consumer.wk=a.wk
  left join _7726 on _7726.wk=a.wk
  where a.error_code=0
  group by 1,2,3,4
)
group by 1,2,3,4,5


select * from public.rawpii_zendesk_tickets_2017 limit 20