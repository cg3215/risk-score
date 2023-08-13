-- Account level Rightshift stats
-- May 2021

with optouts as (
  SELECT 
    COUNT(*) AS optouts, account_sid
  FROM public.sms_internal_blacklist  AS sms_internal_blacklist
  WHERE (CASE WHEN sms_internal_blacklist.allowed = 0 THEN True ELSE False END)
  AND ((CASE WHEN sms_internal_blacklist.twilio_number LIKE 'MG%' THEN 'messaging service' ELSE 'phone number' END) = 'phone number')
  AND (sms_internal_blacklist.date_updated) >= (TIMESTAMP '2021-05-01')
  AND (sms_internal_blacklist.date_updated) <= (TIMESTAMP '2021-05-31')
  AND account_sid in (
        select account_sid from public.account_flags_flat where is_twilio_employee_account = false
    )  group by 2
),
_7726 as (
  select count(*) as _7726, account_sid
  from public.carriercomplaints
  where date(date_time_processed) >= (TIMESTAMP '2021-05-01') and date(date_time_processed) <= (TIMESTAMP '2021-5-31')
        and account_sid in (
            select account_sid from public.account_flags_flat where is_twilio_employee_account = false
        )
  group by 2
),
attempted_sends as (
  select count(*) as attempted, account_sid
   from public.rawpii_sms_kafka as fraud_sms_kafka
   where (fraud_sms_kafka.date_created ) >= (TIMESTAMP '2021-05-01') and (fraud_sms_kafka.date_created ) <= (TIMESTAMP '2021-05-31')
   AND (fraud_sms_kafka.data_load_date ) >= (TIMESTAMP '2021-05-01') and (fraud_sms_kafka.date_created ) <= (TIMESTAMP '2021-05-31')
   AND (NOT COALESCE(bitwise_and(fraud_sms_kafka.flags,bitwise_shift_left(1, 0,32)) >= 1 , FALSE))
   group by 2
),
delivered_sends as (
  select count(*) as delivered, account_sid
   from public.rawpii_sms_kafka as fraud_sms_kafka
   where (fraud_sms_kafka.date_created ) >= (TIMESTAMP '2021-05-01') and (fraud_sms_kafka.date_created ) <= (TIMESTAMP '2021-05-31')
   AND (fraud_sms_kafka.data_load_date ) >= (TIMESTAMP '2021-05-01') and (fraud_sms_kafka.date_created ) <= (TIMESTAMP '2021-05-31')
   AND (NOT COALESCE(bitwise_and(fraud_sms_kafka.flags,bitwise_shift_left(1, 0,32)) >= 1 , FALSE))
   and error_code = 0
   group by 2
)
select a.*, d.delivered, optouts, _7726
from attempted_sends a
left join delivered_sends d on d.account_sid = a.account_sid
left join optouts on optouts.account_sid = a.account_sid
left join _7726 on _7726.account_sid = a.account_sid
-- optouts: divide by delivered
-- 7726: divide by attempted
-- Export as attenpted_delivered_unwanted_xwalk.csv
------------------------------------------------------------------------------
-- Generating features
with errors as (
      select account_sid,
      sum (case when ((error_code>=30003) and (error_code<=30006)) or (error_code=21614) then 1 else 0 end) as unreachable,
      sum (case when error_code=30007 then 1 else 0 end) as _30007,
      sum (case when error_code=30008 then 1 else 0 end) as _30008,
      sum (case when error_code=21610 then 1 else 0 end) as _21610,
--       sum (case when from_cc = 'US' then 1 else 0 end) as from_US,
--       sum (case when from_cc = 'CA' then 1 else 0 end) as from_CA,
--       sum (case when from_cc = 'GB' then 1 else 0 end) as from_GB,
--       sum (case when from_cc = 'AU' then 1 else 0 end) as from_AU,
--       sum (case when from_cc = 'FR' then 1 else 0 end) as from_FR,
--       sum (case when to_cc = 'US' then 1 else 0 end) as to_US,
--       sum (case when to_cc = 'CA' then 1 else 0 end) as to_CA,
--       sum (case when to_cc = 'GB' then 1 else 0 end) as to_GB,
--       sum (case when to_cc = 'AU' then 1 else 0 end) as to_AU,
--       sum (case when to_cc = 'IN' then 1 else 0 end) as to_IN,
      sum (case when from_type = 'LC' then 1 else 0 end) as is_LC,
      sum (case when from_type = 'SC' then 1 else 0 end) as is_SC,
      sum (case when from_type = 'AL' then 1 else 0 end) as is_AL
       from public.rawpii_sms_kafka as fraud_sms_kafka
       where (fraud_sms_kafka.date_created ) >= (TIMESTAMP '2021-05-01') and (fraud_sms_kafka.date_created ) <= (TIMESTAMP '2021-05-31')
       AND (fraud_sms_kafka.data_load_date ) >= (TIMESTAMP '2021-05-01') and (fraud_sms_kafka.date_created ) <= (TIMESTAMP '2021-05-31')
       AND (NOT COALESCE(bitwise_and(fraud_sms_kafka.flags,bitwise_shift_left(1, 0,32)) >= 1 , FALSE))
       group by 1
       -- Export as account_sid_ct_error_xwalk_v2.csv
)

with aup_msgs as (
  with earliest_suspension as (
    select sid, suspension_time from 
    (
      WITH fraud_last_suspension_time AS (
        WITH timestamps AS (
            SELECT
              date_created
              , event_name
              , details
              , rae.account_sid
              , user_source
              , credential_type
              , ROW_NUMBER() OVER (PARTITION BY rae.account_sid ORDER BY date_created DESC) AS suspension_number
            FROM public.rawpii_audit_events rae
            left join public.compromised_credential_events cce on cce.account_sid=rae.account_sid
            WHERE
              rae.event_name IN ('Account suspended')
              AND       1=1 -- no filter on 'fraud_last_suspension_time.audit_event_filter'
          )
          SELECT *
          FROM timestamps
          WHERE suspension_number=1
      )
      SELECT
      	fraud_last_suspension_time.account_sid  AS sid,
      	DATE_FORMAT(fraud_last_suspension_time.date_created , '%Y-%m-%d') AS suspension_time,
      	row_number() OVER (PARTITION BY cast(fraud_last_suspension_time.account_sid as varchar) ORDER BY fraud_last_suspension_time.date_created  ASC) AS rnk
      FROM fraud_last_suspension_time
      WHERE (fraud_last_suspension_time.date_created >= timestamp '2021-05-01')
      AND (fraud_last_suspension_time.date_created <= timestamp '2021-05-31')
      AND ((((fraud_last_suspension_time.details IS NOT NULL)
                AND fraud_last_suspension_time.details LIKE '%category: AUP%'
                or (
                fraud_last_suspension_time.details NOT LIKE '%parent_suspended%'
                AND fraud_last_suspension_time.details NOT LIKE '%suspended_by_parent%'
                AND fraud_last_suspension_time.details NOT LIKE '%Account suspended VIA suspension trigger%'
                AND fraud_last_suspension_time.details NOT LIKE '%category: FRAUD%'
                AND fraud_last_suspension_time.details NOT LIKE '%category: CUSTOMER REQUEST%'
                AND fraud_last_suspension_time.details != 'Account suspended'))))
      ORDER BY 1 DESC
    ) where rnk=1 and suspension_time >= '2021-05-01' and suspension_time <= '2021-05-31'
  ),
  msgs as (
    select account_sid, date(date_created) as d, count(*) as ct
    from public.rawpii_sms_kafka as fraud_sms_kafka
    where account_sid in (select sid from earliest_suspension)
    AND (NOT COALESCE(bitwise_and(fraud_sms_kafka.flags,bitwise_shift_left(1, 0,32)) >= 1 , FALSE))
    AND (fraud_sms_kafka.error_code  = 0)
    and (fraud_sms_kafka.date_created ) >= (TIMESTAMP '2021-04-24')
    AND (fraud_sms_kafka.data_load_date ) >= (TIMESTAMP '2021-04-24')
    group by 1,2
  )
select account_sid, sum(ct) as aup from msgs
left join earliest_suspension on earliest_suspension.sid=msgs.account_sid
where d between (date(suspension_time) - interval '7' day) and date(suspension_time)
group by 1
),
-- Export as account_aup_xwalk.csv
------------------------------------------------------------------
with fraud_msgs as (
  with earliest_suspension as (
    select sid, suspension_time from 
    (
      WITH fraud_last_suspension_time AS (
        WITH timestamps AS (
            SELECT
              date_created
              , event_name
              , details
              , rae.account_sid
              , user_source
              , credential_type
              , ROW_NUMBER() OVER (PARTITION BY rae.account_sid ORDER BY date_created DESC) AS suspension_number
            FROM public.rawpii_audit_events rae
            left join public.compromised_credential_events cce on cce.account_sid=rae.account_sid
            WHERE
              rae.event_name IN ('Account suspended')
              AND       1=1 -- no filter on 'fraud_last_suspension_time.audit_event_filter'
          )
          SELECT *
          FROM timestamps
          WHERE suspension_number=1
      )
      SELECT
      	fraud_last_suspension_time.account_sid  AS sid,
      	DATE_FORMAT(fraud_last_suspension_time.date_created , '%Y-%m-%d') AS suspension_time,
      	row_number() OVER (PARTITION BY cast(fraud_last_suspension_time.account_sid as varchar) ORDER BY fraud_last_suspension_time.date_created  ASC) AS rnk
      FROM fraud_last_suspension_time
      WHERE (fraud_last_suspension_time.date_created >= timestamp '2021-05-01') and (fraud_last_suspension_time.date_created <= timestamp '2021-05-31')
      AND ((((fraud_last_suspension_time.details IS NOT NULL)
                AND fraud_last_suspension_time.details LIKE '%category: FRAUD%'
                or (
                fraud_last_suspension_time.details NOT LIKE '%parent_suspended%'
                AND fraud_last_suspension_time.details NOT LIKE '%suspended_by_parent%'
                AND fraud_last_suspension_time.details NOT LIKE '%Account suspended VIA suspension trigger%'
                AND fraud_last_suspension_time.details NOT LIKE '%category: AUP%'
                AND fraud_last_suspension_time.details NOT LIKE '%category: CUSTOMER REQUEST%'
                AND fraud_last_suspension_time.details != 'Account suspended'))))
      ORDER BY 1 DESC
    ) where rnk=1 and suspension_time >= '2021-05-01' and suspension_time <= '2021-05-31'
  ),
  msgs as (
    select account_sid, date(date_created) as d, count(*) as ct from public.rawpii_sms_kafka as fraud_sms_kafka
    where account_sid in (select sid from earliest_suspension)
    AND (NOT COALESCE(bitwise_and(fraud_sms_kafka.flags,bitwise_shift_left(1, 0,32)) >= 1 , FALSE)) 
    AND (fraud_sms_kafka.error_code  = 0)
    and (fraud_sms_kafka.date_created ) >= (TIMESTAMP '2020-05-01')
    AND (fraud_sms_kafka.data_load_date ) >= (TIMESTAMP '2020-05-01')
    group by 1,2
  )
select account_sid, sum(ct) as fraud from msgs
left join earliest_suspension on earliest_suspension.sid=msgs.account_sid
where d between (date(suspension_time) - interval '7' day) and date(suspension_time)
group by 1
)
select account_sid,
(case when account_sid in (
    select sid from xwalk
  ) then 1 else 0 end) as cannabis,
fraud from fraud_msgs
-- Does not include ams fraud
-- Export as account_fraud_cannabis_xwalk.csv
)
