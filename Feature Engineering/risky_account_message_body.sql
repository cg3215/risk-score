-- Get information on top messages for each risky account
select * from (
                  select sms.account_sid
                       , bodies.body
                       , row_number() over (partition by sms.account_sid order by count(distinct sms.sid) desc) as rnk
                       , count(distinct sms.sid)
                  from public.rawpii_sms_kafka as sms
                  left join public.rawpii_sms_with_bodies_kafka as bodies on bodies.sid = sms.sid
                  where sms.account_sid in (
                        'AC7cf0b316f1f99eb5802eafbb55a74e97', 
                        ...
                        'ACc0df26dd5626d891d203210ee400e5a7')
                    AND (NOT COALESCE(bitwise_and(sms.flags, bitwise_shift_left(1, 0, 32)) >= 1, FALSE))
                    AND (sms.error_code = 0)
                    and (sms.date_created) >= (TIMESTAMP '2021-05-01')
                    AND (sms.data_load_date) >= (TIMESTAMP '2021-05-01')
                    and (sms.date_created) < (TIMESTAMP '2021-06-01')
                    AND (sms.data_load_date) < (TIMESTAMP '2021-06-01')
                  group by 1, 2
              ) where rnk=1