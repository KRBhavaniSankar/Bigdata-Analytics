#step 1 : create dataset for respecive date using classic web ui or console
bq mk `project-name:dataset_id`

#step 2 : upload data into data set using classic web ui or console
bq load dataset_id.sample3 20180601.txt imei:string,AdID:string,ts:timestamp,one_hr_before_ts:timestamp

#step 3 : Inner join query to extract valid record by matching id, AdID and with in the time ranges.
SELECT A.event_date,A.event_timestamp,A.event_name,A.device.mobile_os_hardware_model,A.device.advertising_id,
ARRAY(SELECT event_params.key FROM UNNEST(event_params) AS event_params) AS event_param_key,
<<<<<<< HEAD:GoogleCloudPlatform/BigQuery/test4_innerjoin_nested_arrays_extraction.sql
(SELECT ARRAY_AGG(event_params.value.string_value IGNORE NULLS) FROM UNNEST(event_params) AS event_params) AS event_param_value
FROM `project-name.analytics_165653046.events_20181218` A INNER JOIN `project-name.dataset_id.sample3` B
=======
(SELECT ARRAY_AGG(event_params.value.string_value IGNORE NULLS) FROM UNNEST(event_params) AS event_params) AS event_param_value,B.*
FROM `project-name.dataset_id.events_20181218` A INNER JOIN `project-name.dataset_id.twohour_20181218_2` B
>>>>>>> inner join with nested array data using timestamp operations:GoogleCloudPlatform/BigQuery/test5_innerjoin_nested_arrays_tsa_operation.sql
ON B.AdID = A.device.advertising_id
WHERE CAST(TIMESTAMP_MICROS(A.event_timestamp) as TIMESTAMP) BETWEEN B.hour_before AND B.time

#step 4 : export query results as csv file through web ui or external link through console

#setp 5 : drop the dataset and delete all tables
bq rm -r dataset_id
