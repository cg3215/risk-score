-- Project: SMS Unwanted Communications
-- Author: Elena Gou
-- Date: 06/15/2021
-- Objectives: 1. Pinpoint which variables in rawpii_sms_kafka to be kept for the analysis
-- 2. Use the dataset to construct customer features (from and to country and from category) and aggregate data to account-weekly level
select error_code, count(*) from public.rawpii_sms_kafka where status = 'FAILED' group by error_code order by count(*) desc limit 20;
select error_code from public.rawpii_sms_kafka where sid = 'SM3675f3ecc0844be4dd8eee295a7436da'

-- relevant variables (account-daily level)
create table risk_analytics.rawpii_sms_052021_trimmed_eg as (
    select account_sid, sid, status, sent_date, error_code, from_type, from_cc, to_cc
    from public.rawpii_sms_kafka
    where date(sent_date) > (TIMESTAMP '2021-05-02') and date(sent_date) < (TIMESTAMP '2021-5-31')
    );

-- testing data
create table risk_analytics.rawpii_sms_062021_trimmed_eg as (
    select account_sid, sid, status, sent_date, error_code, from_type, from_cc, to_cc
    from public.rawpii_sms_kafka
    where date(sent_date) > (TIMESTAMP '2021-06-02') and date(sent_date) < (TIMESTAMP '2021-06-30')
    );

-- country (account level)
select from_cc, count(*) as ct_from_cc, count(*)/sum(1.00 * count(*)) over () as perc_from_cc
from risk_analytics.rawpii_sms_052021_trimmed_eg
group by 1
order by 2 desc;
-- Top 5 countries: US, null, CA, GB, AU, DE
-- 6826221045 from US, 87.66% of all messages in May 2021
-- Decision: Generate binary variable US and non-US

select to_cc, count(*) as ct_to_cc, count(*)/sum(1.00 * count(*)) over () as perc_to_cc
from risk_analytics.rawpii_sms_052021_trimmed_eg
group by 1
order by 2 desc;
-- Top 5 countries: US, CA, null, IN, GB, BR
-- 6160910644 from US, 78% of all messages in May 2021
-- Decision: Generate binary variable US and non-US

-- From/To country combination
select from_cc, to_cc, count(*) as ct_from_cc, count(*)/sum(1.00 * count(*)) over () as perc
from risk_analytics.rawpii_sms_052021_trimmed_eg
group by 1,2
order by 3 desc;
-- Top Combinations: US-US, null-null, CA-CA, US-BR, US-CO, US-IN
-- 6150784380 from and to US, 78% of all messages in May 2021

-- from_type and from_category (account level)
select from_type, count(*) as ct_from_type, count(*)/sum(1.00 * count(*)) over () as perc
from risk_analytics.rawpii_sms_052021_trimmed_eg
group by 1
order by 2 desc;
-- Top from phone number type: Long-Code (4540986161, 58%), Short-Code (2624940124, 33%), Other
-- Decision: generate 2 binary variables is_lc, is_sc

select from_category, count(*) as ct_from_category, count(*)/sum(1.00 * count(*)) over () as perc
from risk_analytics.rawpii_sms_052021_trimmed_eg
group by 1
order by 2 desc;
-- Decision: Discard variable as most categories are null

-- status (account level)
select status, count(*) as ct_status, count(*)/sum(1.00 * count(*)) over () as perc_status
from risk_analytics.rawpii_sms_052021_trimmed_eg
group by 1
order by 2 desc;
-- Decision: include all statuses

-- error_code (account level)
select error_code, count(*) as ct_error_code, count(*)/sum(1.0000 * count(*)) over () as perc_error_code
from risk_analytics.rawpii_sms_052021_trimmed_eg
where error_code != 0
group by 1
order by 2 desc;
-- Cutoff point: 1% of all the errors

-- feature construction


create table risk_analytics.rawpii_sms_from_cc_eg as (
    select account_sid, wk, from_cc from(
            select account_sid, wk, from_cc, row_number() over(partition by account_sid, wk order by ct desc) rank
            from (
                select account_sid,
                DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(sent_date) % 7) - 1 + 7, 7)), sent_date)), '%Y-%m-%d') as wk,
                from_cc,
                count(*) as ct

                from risk_analytics.rawpii_sms_052021_trimmed_eg
                where from_cc is not null
                group by 1,2,3
                order by 1,2, ct desc
                 )
            )
        where rank = 1);

create table risk_analytics.rawpii_sms_to_cc_eg as (
    select account_sid, wk, to_cc from(
            select account_sid, wk, to_cc, row_number() over(partition by account_sid, wk order by ct desc) rank
            from (
                select account_sid,
                DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(sent_date) % 7) - 1 + 7, 7)), sent_date)), '%Y-%m-%d') as wk,
                to_cc,
                count(*) as ct
                from risk_analytics.rawpii_sms_052021_trimmed_eg
                where to_cc is not null
                group by 1,2,3
                order by 1,2, ct desc
                 )
            )
    where rank = 1);

create table risk_analytics.rawpii_sms_from_type_eg as (
    select account_sid, wk, from_type from(
            select account_sid, wk, from_type, row_number() over(partition by account_sid, wk order by ct desc) rank
            from (
                select account_sid,
                DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(sent_date) % 7) - 1 + 7, 7)), sent_date)), '%Y-%m-%d') as wk,
                from_type,
                count(*) as ct

                from risk_analytics.rawpii_sms_052021_trimmed_eg
                where from_type is not null
                group by 1,2,3
                order by 1,2, ct desc
                 )
         )
    where rank = 1);

-- create table risk_analytics.rawpii_sms_customer_traits_eg as(
--     select fc.*, tc.to_cc, ft.from_type
--     from risk_analytics.rawpii_sms_from_cc_eg fc
--     left join risk_analytics.rawpii_sms_to_cc_eg tc on (fc.account_sid=tc.account_sid) and (fc.wk=tc.wk)
--     left join risk_analytics.rawpii_sms_from_type_eg ft on (fc.account_sid=ft.account_sid) and (fc.wk=ft.wk)
--     );
create table risk_analytics.rawpii_sms_status_eg as (
    select account_sid, wk, status from(
            select account_sid, wk, status, row_number() over(partition by account_sid, wk order by ct desc) rank
            from (
                select account_sid,
                DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(sent_date) % 7) - 1 + 7, 7)), sent_date)), '%Y-%m-%d') as wk,
                status,
                count(*) as ct

                from risk_analytics.rawpii_sms_052021_trimmed_eg
                where status is not null
                group by 1,2,3
                order by 1,2, ct desc
                 )
         )
    where rank = 1);

-- select status, count(*) from risk_analytics.rawpii_sms_status_eg group by status order by count(*) desc

create table risk_analytics.rawpii_sms_customer_traits_v2_eg as(
    select fc.*, tc.to_cc, ft.from_type, s.status
    from risk_analytics.rawpii_sms_from_cc_eg fc
    left join risk_analytics.rawpii_sms_to_cc_eg tc on (fc.account_sid=tc.account_sid) and (fc.wk=tc.wk)
    left join risk_analytics.rawpii_sms_from_type_eg ft on (fc.account_sid=ft.account_sid) and (fc.wk=ft.wk)
    left join risk_analytics.rawpii_sms_status_eg s on (fc.account_sid=s.account_sid) and (fc.wk=s.wk)
    );

-- test if the dataset was created correctly - each account should have no more than 5 observations
select account_sid, count(*)  from risk_analytics.rawpii_sms_from_type_eg group by account_sid;
-- drop table risk_analytics.rawpii_sms_customer_traits_eg

-- select * from risk_analytics.rawpii_sms_customer_traits_eg limit 2

-- Add # of Message per week
create table risk_analytics.rawpii_sms_volume_per_wk as(
    select account_sid,
    DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(sent_date) % 7) - 1 + 7, 7)), sent_date)), '%Y-%m-%d') as wk,
    count(*) as ct
    from risk_analytics.rawpii_sms_052021_trimmed_eg
    group by 1,2
)
-- select * from risk_analytics.rawpii_sms_volume_per_wk limit 5

create table risk_analytics.rawpii_sms_customer_traits_v3_eg as(
    select fc.*, tc.to_cc, ft.from_type, p.ct, s.status
    from risk_analytics.rawpii_sms_from_cc_eg fc
    left join risk_analytics.rawpii_sms_to_cc_eg tc on (fc.account_sid=tc.account_sid) and (fc.wk=tc.wk)
    left join risk_analytics.rawpii_sms_from_type_eg ft on (fc.account_sid=ft.account_sid) and (fc.wk=ft.wk)
    left join risk_analytics.rawpii_sms_status_eg s on (fc.account_sid=s.account_sid) and (fc.wk=s.wk)
    left join risk_analytics.rawpii_sms_volume_per_wk p on (fc.account_sid=p.account_sid) and (fc.wk=p.wk)
    );