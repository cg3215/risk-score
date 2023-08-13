# File Description

1. rightshift.sql (May 2021 data)

Use public.sms_internal_blacklist to generate a count of opt-outs and public.carrier_complaints to calculate the quantity of spam

2. feature_construction_sms_level.sql

Generate message level features including error_code rates, phone number type, message origination country and termination country, delivery rates

3. feature_construction_account_level.sql

Generate account level features including age, has parent or not, is ISV or not, is cannabis related or not, and family size information

4. risky_account_message_body.sql
   
Query to view sample high risk messages

5. feature_construction_rawpii_accounts.sql
   
Generate additional account level metrics from Salesforce data
