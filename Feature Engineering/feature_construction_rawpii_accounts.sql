-- Project: SMS Unwanted Communications
-- Author: Elena Gou
-- Date: 06/21/2021
-- Objectives: Generate the following features
-- 1. ISV or non-ISV
-- 2. Number of sub-accounts
-- 3. Parent account or not
-- 4. Account age as of the end of May 2021

-- All account_18_id in rawpii_dim_customer_sales appear in rawpii_sfdc_accounts
select count(distinct account_18_id) from public.rawpii_dim_customer_sales where account_18_id in (
    select distinct account_18_id from public.rawpii_sfdc_accounts
    );

-- ISV or non-ISV - account level
-- Method 1: use Salesforce data
-- create table risk_analytics.accountsid_isv_crosswalk_eg as (
--     select rdcs.account_sid, rdcs.account_18_id, (case when rsa.isv_account = TRUE then 'ISV' else 'non-ISV' end) as isv
--     from public.rawpii_dim_customer_sales rdcs
--     left join public.rawpii_sfdc_accounts rsa on rdcs.account_18_id = rsa.account_18_id
-- );

-- Method 2: Use internal list from the Risk Analytics team for consistency
create table risk_analytics.accountsid_isv_crosswalk_v2_eg as (
    select rdcs.account_sid, rdcs.account_18_id, (case when rdcs.account_18_id in (
                                select id from c_xwalk
        ) then 1 else 0 end) as isv
    from public.rawpii_dim_customer_sales rdcs
    left join public.rawpii_sfdc_accounts rsa on rdcs.account_18_id = rsa.account_18_id
    where rdcs.account_sid IS NOT NULL and rdcs.account_sid != 'ACtestKL'
);

-- No duplicates
select count(*) from risk_analytics.accountsid_isv_crosswalk_v2_eg group by account_18_id, account_sid having count(*) > 1;

-- No NULL sid
select parent_account_sid, sid, live_account_sid from public.rawpii_accounts where sid IS NULL;

-- age cutoff points: 11, 26, 47 (months)
select distinct age from (
        select sid, age, ntile(100) over (order by age) as perc
        from (
             select parent_account_sid, sid, date_diff('month', date(date_created), date('2021-05-31')) as age
            from public.rawpii_accounts
            where date_created < TIMESTAMP '2021-05-31'
                 ) sub
                  )
where perc = 0 or perc = 25 or perc = 50 or perc = 75
order by age desc

-- number of sub-accounts cutoff points: 0, 1, > 1
select distinct num_sub from (
        select id, num_sub, ntile(100) over (order by num_sub) as perc
        from (
             select (case when parent_account_sid is not null then parent_account_sid else sid end) as id,
            count(distinct sid) - 1 as num_sub
            from public.rawpii_accounts
            where date_created < TIMESTAMP '2021-05-31'
            group by 1
            ) sub
        where sub.num_sub > 0
                  )
where perc = 0 or perc = 25 or perc = 50 or perc = 75
order by num_sub desc

-- Account Age, is_parent, num_sub-account - account level
create table risk_analytics.feature_construction_rawpii_accounts_v2_eg as (
select parent_account_sid, sid, a.id as concat_id, age, parent, num_sub, coalesce(isv,0) as isv,
       (case when age <= 11 then '0-11' when age > 11 and age <= 26 then '11-26' when age > 47 then '> 47' else '26-47' end) as age_cat,
       (case when num_sub = 0 then '0' when num_sub = 1 then '1' else '> 1' end) as num_sub_cat
from (
              select parent_account_sid, sid,
                     (case when parent_account_sid is not null then parent_account_sid else sid end) as id,
                     date_diff('month', date(date_created), date('2021-05-31')) as age,
              (case when parent_account_sid IS NULL then 0 else 1 end) as parent
              from public.rawpii_accounts
              where date_created < TIMESTAMP '2021-05-31'
                  ) a
left join (
select (case when parent_account_sid is not null then parent_account_sid else sid end) as id,
            count(distinct sid) - 1 as num_sub
            from public.rawpii_accounts
            where date_created < TIMESTAMP '2021-05-31'
            group by 1
) s on s.id = a.id
left join risk_analytics.accountsid_isv_crosswalk_v2_eg r on r.account_sid = a.id);

-- drop table risk_analytics.feature_construction_rawpii_accounts_v2_eg
-- select * from risk_analytics.feature_construction_rawpii_accounts_v2_eg where isv =1
drop table risk_analytics.sid_fam_size_xwalk_eg
-- Add family size feature
create table risk_analytics.sid_fam_size_xwalk_eg as (
    select f.sid, coalesce(f.num_sub,0) +1 as fam_size
                from risk_analytics.feature_construction_rawpii_accounts_v2_eg f
                left join risk_analytics.feature_construction_rawpii_accounts_v2_eg q
                on f.sid = q.parent_account_sid
)
-- export as sid_fam_size_xwalk.csv

select sid,age,parent,isv from risk_analytics.feature_construction_rawpii_accounts_v2_eg
-- Export as account_age_xwalk.csv
