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
     'AC0432a3ef606168d8ad1312c43e4bc0d8',
    'AC05a870077166689500e156e8085d608e',
    'AC0c02b7d4659c878d5a22d2c6313c1c57',
    'AC15fb72388c1ca6ce4c95d4b60fc3f3eb',
    'AC17f5b1d22c69d67f60b5e0b1b694e3fd',
    'AC18c419fa1fc6d7e0aa723ea1677abf5b',
    'AC1ace5431c7357fc56cb51e50ba0b23e7',
    'AC1de9175287cc98914f6289b15732fc01',
    'AC20c9b4bee3bdcbce7d936d43f89db110',
    'AC2aa489a927354ad5793363326e73d214',
    'AC309ae47febd38e560fe0fba5a1075c98',
    'AC333793c7f8be6df9f9b8de8f0ab682bd',
    'AC3932aa6d17e1bb13eddc6ba6012d4ee2',
    'AC42c6102c1a72d04805b44968fb8fe113',
    'AC4bf285af2dab39d9a1549fed79e88431',
    'AC4e2459c0903841f0dfc3cbadfaa04e8e',
    'AC5789c1fb99a64f3575526e0e7425aa28',
    'AC58b7c0e74a900affa751028d55cfca27',
    'AC5c1d0d5d6b3e44c1e5d63fff01dd3db0',
    'AC5d84615656bee97eebabd7b5a4e3285a',
    'AC5fd22b23415c04d13305c17a8c5de894',
    'AC626abc32ab907037a01d29936653a6a5',
    'AC7316b88e1c9eaecead06c1c2fa74af77',
    'AC7762350e38fa10c114f89021c721fbf7',
    'AC7bda6c19fd07b25e67d02116950263c0',
    'AC81f76452943114e8ec82d89eec90ea5d',
    'AC83fce667a7617dd3d5d668b5c8291108',
    'AC8446f729928288cc1b5e8de117c354ed',
    'AC934020f7be8ad491b0bc93fd5dc4855b',
    'AC94222fb77376e346fcd9b5ce674fda93',
    'AC98c055f01abec984e5fa5b1ed322677a',
    'ACa13ca6f8c436cdb47221a583cfad0b36',
    'ACa27ddd861ee7752b8719a1920b7b009c',
    'ACae3952b79d00f7872c337929df160741',
    'ACaffe005722b54beca3ce12e4d14e8baa',
    'ACb4572da22958a0fe382e6b0186d90d22',
    'ACc13d683804cb3773a11d1786ebe29dfb',
    'ACc82a810bc0bea6ef5ef0c43f407f5868',
    'ACd42cac064e948ac416f7be8a73f74f78',
    'ACd7140f554ae264819f3f73440f864d98',
    'ACdcaabcfd2a6409ced6a29c4c71c61f81',
    'ACe5b97e502d22cf517804720a8a017f6f',
    'ACe8bbce9c3b6125d4979ec793071b34ed',
    'ACeaa9b5f33c98f91c7620097dfca0efe3',
    'ACee62c4c4a5da27d3107f02657605b299',
    'ACfd6cb471820e97b79cdf3488617a4d40',
    'ACff553b941af13486ffb88664c4f4d4cf',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'ACb8d01b252ae2b66a866015b06e8446ef',
    'ACb8d01b252ae2b66a866015b06e8446ef',
    'AC5aba2c93dc9643f78e9c405f18493fc8',
    'ACbdb3fc75f31f759272c8d3cf41673392',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC49f809cc218fefe39d6ea74aa121ca12',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC28cc68b5089550456a5504b3739c2a02',
    'AC047ad5a3b099a40d56919a5b3bad31f5',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC303d9006b8a1a9f06ce052fcee09b286',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC59af251235a438296ebf11ac3bbee1e9',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'ACa136a8a683fca9e9b4b7daf85aeb8f13',
    'ACeb349566162dc79dc0edf39a59d5ccab',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'ACb8d01b252ae2b66a866015b06e8446ef',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC24f3e7dbba3d017e27cb52e422713820',
    'AC047ad5a3b099a40d56919a5b3bad31f5',
    'AC431e3340a59c85698052248802d58dca',
    'AC59af251235a438296ebf11ac3bbee1e9',
    'AC303d9006b8a1a9f06ce052fcee09b286',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'ACb8d01b252ae2b66a866015b06e8446ef',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC330d48bf657095357264f36d67f0c3c9',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC49f809cc218fefe39d6ea74aa121ca12',
    'ACa136a8a683fca9e9b4b7daf85aeb8f13',
    'AC431e3340a59c85698052248802d58dca',
    'ACb8d01b252ae2b66a866015b06e8446ef',
    'AC6c74b91270254c22b3d65c9c5fe598f8',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'ACb8d01b252ae2b66a866015b06e8446ef',
    'ACb8d01b252ae2b66a866015b06e8446ef',
    'AC047ad5a3b099a40d56919a5b3bad31f5',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'ACb8d01b252ae2b66a866015b06e8446ef',
    'AC303d9006b8a1a9f06ce052fcee09b286',
    'AC00db4c875a3ba66acee9e7d4b5044606',
    'AC59af251235a438296ebf11ac3bbee1e9',
    'AC93a8304478d035be02dc3c99a8ead9f8',
    'AC97be61027e944360d96dac6a3cd15e8e',
    'AC4194cdb66e2d8e50d3da987f809c0d54',
    'AC4d8ad370603b47ecb1635945f5fd0cff',
    'AC94389608e352cc500390ac6ad01811ed',
    'ACb096e037d8ee6d445708479fecc1895d',
    'AC55906193c71ed902419c47c1094c64f8',
    'AC3b7b3e3e8c6b7d87e8bd4e27233ef8ae',
    'AC5a4fe5d71776b60c3ff7012ba1b3019d',
    'AC9842f70addde77a1efd340b1307f2a00',
    'AC6ae5f6a000ed426004ecec1633aa4dc7',
    'ACc592be33a75d9cb2f5c95893313be721',
    'AC3cadcf30a0976010fc4d429d046ff1df',
    'AC7eda3773a75c36dd329ac6fc11343322',
    'ACa0d9d92fba8f59a04f1c64c309092b13',
    'ACcad54a3d59a4661f94e5a0e0f055aa51',
    'ACf34886cf2089f2cabfd189de301a2330',
    'ACf69f24a78083ca90174aab9b1e94048e',
    'ACd4cecac51266a68edd23195aba0024c3',
    'AC0785450e36c799c956a101f9021e9333',
    'AC855c8f832abde8532d64c4a834bdef86',
    'AC637059728c1a0e35623e8c4f00434eda',
    'AC92a8dd1f3d065b4fd8bac30d283d0765',
    'ACdc495a8b4e694c8faf587ba871943a4f',
    'AC3b789d5140cb1ef5e607baf52c5029b6',
    'AC6e90ca64ed616688178811d552a29404',
    'AC5654cbdec33f42bf36db482287aa6a82',
    'ACd02a05b29439e881d8d413a9bde602d6',
    'AC3c4739a2137c3cd1d5d8bccf83e45db4',
    'ACac594fab32d43da8ed3bfd510e987b80',
    'AC3b4a43b07e0ad52df362fb0e850a36d9',
    'AC62c8d44ecc472b83e91a9172467be116',
    'ACfa463d87f8df3b3dae32af0ec365ebc4',
    'ACbf3d0036ce2b2216f40bf23e166e543b'
  ) then 1 else 0 end) as cannabis,
fraud from fraud_msgs
-- Does not include ams fraud
-- Export as account_fraud_cannabis_xwalk.csv
)
