ASSERT(SELECT MAX(Datetime_diff(close_date, created_date, DAY)) < 2
FROM `${austin_311_service_requests}`) AS 'Max Time-to-respond exceeded';