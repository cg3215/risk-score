# File Description

[1. rightshift.sql (May 2021 data)](https://code.hq.twilio.com/twilio/ct-risk-analytics/blob/eg_dev/egou/Feature%20Engineering/rightshift.sql)

Use public.sms_internal_blacklist to generate count of opt-outs

Utilize public.carrier_complaints to calculate 7726 count

Use public.rawpii_sms_kafka to count attempted sends, delivered sends, error code 30003-30006 and 21614 (unreachable), 30007, 30008, 21610, sent from Long Code, Short Code, and Alphanumeric 

Join public.rawpii_audit_events and public.compromised_credential_events on account_sid to create fraud and (against) AUP (Acceptable User Policy) message counts

Generate account level binary variable indicating if an account is considered a cannabis account or not


[2. june_rightshift.sql](https://code.hq.twilio.com/twilio/ct-risk-analytics/blob/eg_dev/egou/Feature%20Engineering/june_rightshift.sql)

Same as above but with June 2021 data


[3. feature_construction_rawpii_accounts.sql](https://code.hq.twilio.com/twilio/ct-risk-analytics/blob/eg_dev/egou/Feature%20Engineering/feature_construction_rawpii_accounts.sql)

Generate account level age, has parent or not, is ISV or not, and family size information
