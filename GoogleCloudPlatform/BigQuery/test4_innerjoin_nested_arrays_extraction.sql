#step 1 : create dataset for respecive date using classic web ui or console
bq mk `docomomail-dcef6:ai_events_test`

#step 2 : upload data into data set using classic web ui or console
bq load ai_events_test.sample3 20180601.txt imei:string,AdID:string,ts:timestamp,one_hr_before_ts:timestamp

#step 3 : Inner join query to extract valid record by matching id, AdID and with in the time ranges.
SELECT A.event_date,A.event_timestamp,A.event_name,A.device.mobile_os_hardware_model,A.device.advertising_id,
ARRAY(SELECT event_params.key FROM UNNEST(event_params) AS event_params) AS event_param_key,
(SELECT ARRAY_AGG(event_params.value.string_value IGNORE NULLS) FROM UNNEST(event_params) AS event_params) AS event_param_value
FROM `docomomail-dcef6.analytics_165653046.events_20181218` A INNER JOIN `docomomail-dcef6.ai_events_test.sample3` B
ON B.AdID = A.device.advertising_id
WHERE CAST(TIMESTAMP_MICROS(A.event_timestamp) as TIMESTAMP) BETWEEN B.hour_before AND B.time

#step 4 : export query results as csv file through web ui or external link through console

#setp 5 : drop the dataset and delete all tables
bq rm -r ai_events_test
