SELECT A.event_date,A.event_timestamp,A.event_name,A.device.mobile_os_hardware_model,A.device.advertising_id,
(SELECT event_prm.key FROM UNNEST(A.event_params) as event_prm) AS event_params_key,
(SELECT event_prm.value.string_value FROM UNNEST(A.event_params) as event_prm) AS event_params_value_string
FROM `docomomail-dcef6.analytics_165653046.events_20181218` A INNER JOIN `docomomail-dcef6.ai_events_test.sample` B
ON B.AdID = A.device.advertising_id
WHERE CAST(TIMESTAMP_MICROS(A.event_timestamp) as TIMESTAMP) BETWEEN B.hour_before AND B.time
