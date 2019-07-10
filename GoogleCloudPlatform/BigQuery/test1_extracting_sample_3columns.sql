SELECT A.*,B.event_date,B.event_timestamp,B.event_name
FROM `docomomail-dcef6.ai_events_test.tb20181218` A INNER JOIN `docomomail-dcef6.analytics_165653046.events_20181218` B
ON A.AdID = B.device.advertising_id
WHERE CAST(B.event_date as TIMESTAMP) BETWEEN A.hour_before AND A.time
