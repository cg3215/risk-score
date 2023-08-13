-- Project: SMS Unwanted Communications
-- Author: Elena Gou
-- Date: 06/15/2021

-- Objectives: 1. aggregate public.rawpii_sms_kafka to account-week level
-- 2. Join data with 7726 records and opt-outs
-- 3. Join with customer features

-- create table risk_analytics.unwanted_sms_eg as (
create table risk_analytics.unwanted_sms_eg_v2 as (
    with rawpii_sms as (
      select account_sid,
             DATE_FORMAT(DATE_TRUNC('DAY',DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(sent_date) % 7) - 1 + 7, 7)),sent_date)), '%Y-%m-%d') as wk,
             count(*) as _total,
      sum (case when ((error_code>=30003) and (error_code<=30006)) or (error_code=21614) then 1 else 0 end) as unreachable,
      sum (case when error_code=30007 then 1 else 0 end) as _30007,
      sum (case when error_code=30002 then 1 else 0 end) as _30002,
      sum (case when error_code=21610 then 1 else 0 end) as _21610
  from risk_analytics.rawpii_sms_052021_trimmed_eg
  group by 1, 2
    ),
    _7726s as(
        select account_sid,
               DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day',(0 - MOD((DAY_OF_WEEK(date_time_processed) % 7) - 1 + 7, 7)),date_time_processed)), '%Y-%m-%d') as wk,
               count(*) as _7726
        from public.carriercomplaints
        where date(date_time_processed) > (TIMESTAMP '2021-05-02') and date(date_time_processed) < (TIMESTAMP '2021-5-31')
        group by 1, 2
        ),
    optouts as (
        select account_sid,
               count(*) as optout,
               DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(sms_internal_blacklist.date_updated) % 7) - 1 + 7, 7)),sms_internal_blacklist.date_updated)), '%Y-%m-%d') as wk
        from public.sms_internal_blacklist
        where (sms_internal_blacklist.allowed = 0)
          and ((case when sms_internal_blacklist.twilio_number like 'MG%' then 'messaging service' else 'phone number' end) =
               'phone number')
          and (sms_internal_blacklist.date_updated) > (TIMESTAMP '2021-05-02')
          and (sms_internal_blacklist.date_updated) < (TIMESTAMP '2021-05-31')
        group by 1, 3
        )
        select rawpii_sms.*,
--                coalesce(_7726s._7726, 0) as _7726, coalesce(optouts.optout,0) as _optouts,
--                 rawpii_sms.unreachable/(rawpii_sms._total*1.0000) as unreachable_perc,
--                 rawpii_sms._30007/(rawpii_sms._total*1.0000) as _30007_perc,
--                 rawpii_sms._30002/(rawpii_sms._total*1.0000) as _30002_perc,
--                 rawpii_sms._21610/(rawpii_sms._total*1.0000) as _21610_perc,
--                 coalesce(_7726s._7726,0)/(rawpii_sms._total*1.0000) as _7726_perc,
--                 coalesce(optouts.optout,0)/(rawpii_sms._total*1.0000) as optouts_perc
                    coalesce(_7726s._7726, 0) as _7726, coalesce(optouts.optout,0) as _optouts,
                    100*rawpii_sms.unreachable/(rawpii_sms._total*1.00000000) as unreachable_perc,
                    100*rawpii_sms._30007/(rawpii_sms._total*1.00000000) as _30007_perc,
                    100*rawpii_sms._30002/(rawpii_sms._total*1.00000000) as _30002_perc,
                    100*rawpii_sms._21610/(rawpii_sms._total*1.00000000) as _21610_perc,
                    100*coalesce(_7726s._7726,0)/(rawpii_sms._total*1.00000000) as _7726_perc,
                    100*coalesce(optouts.optout,0)/(rawpii_sms._total*1.00000000) as optouts_perc
        from rawpii_sms
        left join _7726s on (rawpii_sms.account_sid = _7726s.account_sid) and (rawpii_sms.wk = _7726s.wk)
        left join optouts on (rawpii_sms.account_sid = optouts.account_sid) and (rawpii_sms.wk = optouts.wk)
)
select * from risk_analytics.unwanted_sms_eg_v2 limit 2
-- join with independent variables

-- select u.account_sid, u.wk, count(*)
select u.*, r.from_cc, r.from_type, r.to_cc, r.status, a.*
from risk_analytics.unwanted_sms_eg u
inner join risk_analytics.rawpii_sms_customer_traits_v2_eg r on (u.account_sid = r.account_sid) and (u.wk = r.wk)
inner join risk_analytics.feature_construction_rawpii_accounts_v2_eg a on (u.account_sid = a.sid)
-- group by u.account_sid, u.wk
-- having count(*) >1
-- Sanity Check: observations unique on account-week level

-- Result exported to df0624.csv

-- select u.*, r.from_cc, r.from_type, r.to_cc, r.status, a.*
select u.*, r.ct, r.from_cc, r.from_type, r.to_cc, r.status, a.*
from risk_analytics.unwanted_sms_eg u
inner join risk_analytics.rawpii_sms_customer_traits_v3_eg r on (u.account_sid = r.account_sid) and (u.wk = r.wk)
inner join risk_analytics.feature_construction_rawpii_accounts_v2_eg a on (u.account_sid = a.sid)
-- Result exported to df0707.csv

select u.*, r.ct, r.from_cc, r.from_type, r.to_cc, r.status, a.*
from risk_analytics.unwanted_sms_eg_v2 u
inner join risk_analytics.rawpii_sms_customer_traits_v3_eg r on (u.account_sid = r.account_sid) and (u.wk = r.wk)
inner join risk_analytics.feature_construction_rawpii_accounts_v2_eg a on (u.account_sid = a.sid)
-- Result exported to df0712.csv



