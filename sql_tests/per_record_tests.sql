SELECT
IF(
  -- The test
  status IN ('Closed', 'Open', 'Work In Progress', 'New', 'Resolved', 'Duplicate (open)', 'Duplicate (closed)'),
  -- The pass message (per record)
  CONCAT('status ', status, ' is valid'),
  -- The error message
  ERROR(CONCAT('Status ', status, ' is not valid at record with unique_key=', unique_key))
) as status_is_correct,
IF(
  -- if status is final
  status IN ('Closed', 'Resolved', 'Duplicate (closed)'),
  IF (
    -- it should have a close_date
    close_date IS NOT NULL,
    'Closed date is valid',
    ERROR(CONCAT('Record with unique_key ', unique_key, ' should have close date since status is closed. Status = ', status, '. Closed Date: ', close_date))
  ),
  IF(
    -- if not, it shouldn't have a close date
    close_date IS NULL,
    'Close date is null - valid',
    ERROR(CONCAT('Record with unique_key ', unique_key, ' should NOT have close date since status is not closed. Status = ', status, '. Closed Date: ', close_date))
  )
) as closed_has_close_date
FROM `${austin_311_service_requests}`;

