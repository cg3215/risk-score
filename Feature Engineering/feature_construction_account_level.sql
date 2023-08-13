-- Project: SMS Unwanted Communications
-- Author: Elena Gou
-- Date: 07/13/2021

-- Objectives: Aggregate all features to account level
-- create table risk_analytics.unwanted_sms_account_level_eg as (
create table risk_analytics.unwanted_sms_account_level_eg_v3 as (
    with rawpii_sms as (
      select account_sid,
             count(*) as _total,
      sum (case when ((error_code>=30003) and (error_code<=30006)) or (error_code=21614) then 1 else 0 end) as unreachable,
      sum (case when error_code=30007 then 1 else 0 end) as _30007,
      sum (case when error_code=30008 then 1 else 0 end) as _30008,
      sum (case when error_code=21610 then 1 else 0 end) as _21610,
      sum (case when from_cc = 'US' then 1 else 0 end) as from_US,
      sum (case when from_cc = 'CA' then 1 else 0 end) as from_CA,
      sum (case when from_cc = 'GB' then 1 else 0 end) as from_GB,
      sum (case when from_cc = 'AU' then 1 else 0 end) as from_AU,
      sum (case when from_cc = 'FR' then 1 else 0 end) as from_FR,
      sum (case when to_cc = 'US' then 1 else 0 end) as to_US,
      sum (case when to_cc = 'CA' then 1 else 0 end) as to_CA,
      sum (case when to_cc = 'GB' then 1 else 0 end) as to_GB,
      sum (case when to_cc = 'AU' then 1 else 0 end) as to_AU,
      sum (case when to_cc = 'IN' then 1 else 0 end) as to_IN,
      sum (case when from_type = 'LC' then 1 else 0 end) as is_LC,
      sum (case when from_type = 'SC' then 1 else 0 end) as is_SC,
      sum (case when from_type = 'AL' then 1 else 0 end) as is_AL,
      sum (case when status = 'DELIVERED' then 1 else 0 end) as delivered,
      sum (case when status = 'UNDELIVERED' then 1 else 0 end) as undelivered,
--       sum (case when status = 'RECEIVED' then 1 else 0 end) as received,
      sum (case when status = 'SENT' then 1 else 0 end) as sent
--       sum (case when status = 'FAILED' then 1 else 0 end) as failed

      from risk_analytics.rawpii_sms_052021_trimmed_eg
      where status != 'RECEIVED' and
        account_sid in (
            select account_sid from public.account_flags_flat where is_twilio_employee_account = false
        )
      group by 1
    ),
    _7726s as(
        select account_sid,
               count(*) as _7726
        from public.carriercomplaints
        where date(date_time_processed) > (TIMESTAMP '2021-05-02') and date(date_time_processed) < (TIMESTAMP '2021-5-31')
        and account_sid in (
            select account_sid from public.account_flags_flat where is_twilio_employee_account = false
        )
        group by 1
        ),
    optouts as (
        select account_sid,
               count(*) as optout
        from public.sms_internal_blacklist
        where (sms_internal_blacklist.allowed = 0)
          and ((case when sms_internal_blacklist.twilio_number like 'MG%' then 'messaging service' else 'phone number' end) =
               'phone number')
          and (sms_internal_blacklist.date_updated) > (TIMESTAMP '2021-05-02')
          and (sms_internal_blacklist.date_updated) < (TIMESTAMP '2021-05-31')
          and account_sid in (
                select account_sid from public.account_flags_flat where is_twilio_employee_account = false
            )
        group by 1
        )
        select rawpii_sms.*,
                    coalesce(_7726s._7726, 0) as _7726, coalesce(optouts.optout,0) as _optouts,
                    100*rawpii_sms.unreachable/(rawpii_sms._total*1.00000000) as unreachable_perc,
                    100*rawpii_sms._30007/(rawpii_sms._total*1.00000000) as _30007_perc,
                    100*rawpii_sms._30008/(rawpii_sms._total*1.00000000) as _30008_perc,
                    100*rawpii_sms._21610/(rawpii_sms._total*1.00000000) as _21610_perc,
                    100*coalesce(_7726s._7726,0)/(rawpii_sms._total*1.00000000) as _7726_perc,
                    100*coalesce(optouts.optout,0)/(rawpii_sms._total*1.00000000) as optouts_perc,
                    100*rawpii_sms.from_US/(rawpii_sms._total*1.00000000) as from_US_perc,
                    100*rawpii_sms.from_CA/(rawpii_sms._total*1.00000000) as from_CA_perc,
                    100*rawpii_sms.from_GB/(rawpii_sms._total*1.00000000) as from_GB_perc,
                    100*rawpii_sms.from_AU/(rawpii_sms._total*1.00000000) as from_AU_perc,
                    100*rawpii_sms.from_FR/(rawpii_sms._total*1.00000000) as from_FR_perc,
                    100*rawpii_sms.to_US/(rawpii_sms._total*1.00000000) as to_US_perc,
                    100*rawpii_sms.to_CA/(rawpii_sms._total*1.00000000) as to_CA_perc,
                    100*rawpii_sms.to_GB/(rawpii_sms._total*1.00000000) as to_GB_perc,
                    100*rawpii_sms.to_AU/(rawpii_sms._total*1.00000000) as to_AU_perc,
                    100*rawpii_sms.to_IN/(rawpii_sms._total*1.00000000) as to_IN_perc,
                    100*rawpii_sms.is_LC/(rawpii_sms._total*1.00000000) as is_LC_perc,
                    100*rawpii_sms.is_SC/(rawpii_sms._total*1.00000000) as is_SC_perc,
                    100*rawpii_sms.is_AL/(rawpii_sms._total*1.00000000) as is_AL_perc,
                    100*rawpii_sms.delivered/(rawpii_sms._total*1.00000000) as delivered_perc,
                    100*rawpii_sms.undelivered/(rawpii_sms._total*1.00000000) as undelivered_perc,
                    100*rawpii_sms.sent/(rawpii_sms._total*1.00000000) as sent_perc
--                     100*rawpii_sms.failed/(rawpii_sms._total*1.00000000) as failed_perc,
--                     100*rawpii_sms.received/(rawpii_sms._total*1.00000000) as received_perc

        from rawpii_sms
        left join _7726s on (rawpii_sms.account_sid = _7726s.account_sid)
        left join optouts on (rawpii_sms.account_sid = optouts.account_sid)
)

select u.*, a.*
from risk_analytics.unwanted_sms_account_level_eg u
inner join risk_analytics.feature_construction_rawpii_accounts_v2_eg a on (u.account_sid = a.sid)
-- Exclude employee accounts
where u.account_sid in (
    select account_sid from public.account_flags_flat where is_twilio_employee_account = false
    )
-- Export as df0713.csv

-- unwanted_sms_account_level_eg_v2
-- removed failed and received, add 30008
select u.*, a.*
from risk_analytics.unwanted_sms_account_level_eg_v2 u
inner join risk_analytics.feature_construction_rawpii_accounts_v2_eg a on (u.account_sid = a.sid)
-- Exclude employee accounts
where u.account_sid in (
    select account_sid from public.account_flags_flat where is_twilio_employee_account = false
    )
-- Export as df0714.csv

select u.*, a.*
from risk_analytics.unwanted_sms_account_level_eg_v3 u
inner join risk_analytics.feature_construction_rawpii_accounts_v2_eg a on (u.account_sid = a.sid)
-- Exclude employee accounts
where u.account_sid in (
    select account_sid from public.account_flags_flat where is_twilio_employee_account = false
    )
-- Export as df0716.csv


-- Generate testing data
create table risk_analytics.testing_062021_eg_v2 as (
    with rawpii_sms as (
      select account_sid,
             count(*) as _total,
      sum (case when ((error_code>=30003) and (error_code<=30006)) or (error_code=21614) then 1 else 0 end) as unreachable,
      sum (case when error_code=30007 then 1 else 0 end) as _30007,
      sum (case when error_code=30008 then 1 else 0 end) as _30008,
      sum (case when error_code=21610 then 1 else 0 end) as _21610,
      sum (case when from_cc = 'US' then 1 else 0 end) as from_US,
      sum (case when from_cc = 'CA' then 1 else 0 end) as from_CA,
      sum (case when from_cc = 'GB' then 1 else 0 end) as from_GB,
      sum (case when from_cc = 'AU' then 1 else 0 end) as from_AU,
      sum (case when from_cc = 'FR' then 1 else 0 end) as from_FR,
      sum (case when to_cc = 'US' then 1 else 0 end) as to_US,
      sum (case when to_cc = 'CA' then 1 else 0 end) as to_CA,
      sum (case when to_cc = 'GB' then 1 else 0 end) as to_GB,
      sum (case when to_cc = 'AU' then 1 else 0 end) as to_AU,
      sum (case when to_cc = 'IN' then 1 else 0 end) as to_IN,
      sum (case when from_type = 'LC' then 1 else 0 end) as is_LC,
      sum (case when from_type = 'SC' then 1 else 0 end) as is_SC,
      sum (case when from_type = 'AL' then 1 else 0 end) as is_AL,
      sum (case when status = 'DELIVERED' then 1 else 0 end) as delivered,
      sum (case when status = 'UNDELIVERED' then 1 else 0 end) as undelivered,
--       sum (case when status = 'RECEIVED' then 1 else 0 end) as received,
      sum (case when status = 'SENT' then 1 else 0 end) as sent
--       sum (case when status = 'FAILED' then 1 else 0 end) as failed

      from risk_analytics.rawpii_sms_062021_trimmed_eg
      where status != 'RECEIVED' and
    account_sid in (
        select account_sid from public.account_flags_flat where is_twilio_employee_account = false
    )
      group by 1
    ),
    _7726s as(
        select account_sid,
               count(*) as _7726
        from public.carriercomplaints
        where date(date_time_processed) > (TIMESTAMP '2021-06-02') and date(date_time_processed) < (TIMESTAMP '2021-6-30') and
        account_sid in (
            select account_sid from public.account_flags_flat where is_twilio_employee_account = false
        )
        group by 1
        ),
    optouts as (
        select account_sid,
               count(*) as optout
        from public.sms_internal_blacklist
        where (sms_internal_blacklist.allowed = 0)
          and ((case when sms_internal_blacklist.twilio_number like 'MG%' then 'messaging service' else 'phone number' end) =
               'phone number')
          and (sms_internal_blacklist.date_updated) > (TIMESTAMP '2021-06-02')
          and (sms_internal_blacklist.date_updated) < (TIMESTAMP '2021-06-30') and
        account_sid in (
            select account_sid from public.account_flags_flat where is_twilio_employee_account = false
        )
        group by 1
        )
        select rawpii_sms.*,
                    coalesce(_7726s._7726, 0) as _7726, coalesce(optouts.optout,0) as _optouts,
                    100*rawpii_sms.unreachable/(rawpii_sms._total*1.00000000) as unreachable_perc,
                    100*rawpii_sms._30007/(rawpii_sms._total*1.00000000) as _30007_perc,
                    100*rawpii_sms._30008/(rawpii_sms._total*1.00000000) as _30008_perc,
                    100*rawpii_sms._21610/(rawpii_sms._total*1.00000000) as _21610_perc,
                    100*coalesce(_7726s._7726,0)/(rawpii_sms._total*1.00000000) as _7726_perc,
                    100*coalesce(optouts.optout,0)/(rawpii_sms._total*1.00000000) as optouts_perc,
                    100*rawpii_sms.from_US/(rawpii_sms._total*1.00000000) as from_US_perc,
                    100*rawpii_sms.from_CA/(rawpii_sms._total*1.00000000) as from_CA_perc,
                    100*rawpii_sms.from_GB/(rawpii_sms._total*1.00000000) as from_GB_perc,
                    100*rawpii_sms.from_AU/(rawpii_sms._total*1.00000000) as from_AU_perc,
                    100*rawpii_sms.from_FR/(rawpii_sms._total*1.00000000) as from_FR_perc,
                    100*rawpii_sms.to_US/(rawpii_sms._total*1.00000000) as to_US_perc,
                    100*rawpii_sms.to_CA/(rawpii_sms._total*1.00000000) as to_CA_perc,
                    100*rawpii_sms.to_GB/(rawpii_sms._total*1.00000000) as to_GB_perc,
                    100*rawpii_sms.to_AU/(rawpii_sms._total*1.00000000) as to_AU_perc,
                    100*rawpii_sms.to_IN/(rawpii_sms._total*1.00000000) as to_IN_perc,
                    100*rawpii_sms.is_LC/(rawpii_sms._total*1.00000000) as is_LC_perc,
                    100*rawpii_sms.is_SC/(rawpii_sms._total*1.00000000) as is_SC_perc,
                    100*rawpii_sms.is_AL/(rawpii_sms._total*1.00000000) as is_AL_perc,
                    100*rawpii_sms.delivered/(rawpii_sms._total*1.00000000) as delivered_perc,
                    100*rawpii_sms.undelivered/(rawpii_sms._total*1.00000000) as undelivered_perc,
                    100*rawpii_sms.sent/(rawpii_sms._total*1.00000000) as sent_perc
--                     100*rawpii_sms.failed/(rawpii_sms._total*1.00000000) as failed_perc,
--                     100*rawpii_sms.received/(rawpii_sms._total*1.00000000) as received_perc

        from rawpii_sms
        left join _7726s on (rawpii_sms.account_sid = _7726s.account_sid)
        left join optouts on (rawpii_sms.account_sid = optouts.account_sid)
)

select * from risk_analytics.testing_062021_eg_v2 where account_sid = 'AC7663b0f32ca3c48e9159fd9eb959f325'
select * from risk_analytics.rawpii_sms_062021_trimmed_eg where account_sid = 'AC7663b0f32ca3c48e9159fd9eb959f325'
select * from public.sms_internal_blacklist where account_sid = 'AC7663b0f32ca3c48e9159fd9eb959f325' and
(sms_internal_blacklist.date_updated) > (TIMESTAMP '2021-06-02')
          and (sms_internal_blacklist.date_updated) < (TIMESTAMP '2021-06-30')

select u.*, a.*
from risk_analytics.testing_062021_eg_v2 u
inner join risk_analytics.feature_construction_rawpii_accounts_v2_eg a on (u.account_sid = a.sid)
-- Export as June_2021_v2.csv

-- Generate sid, friendly_name xwalk with duplicates
select sid,
       friendly_name,
       (case when friendly_name LIKE '%First Twilio%' then 1 else 0 end) as default_name
from public.rawpii_accounts
-- Export as sid_first_twilio_xwalk.csv