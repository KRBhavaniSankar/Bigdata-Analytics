SELECT event_date,event_timestamp,event_name,device.mobile_os_hardware_model,device.advertising_id,
ARRAY(SELECT event_params.key FROM UNNEST(event_params) AS event_params) AS event_param_key,
(SELECT ARRAY_AGG(event_params.value.string_value IGNORE NULLS) FROM UNNEST(event_params) AS event_params) AS event_param_value
FROM `project-name.analytics_165653046.events_20181218`
LIMIT 100
