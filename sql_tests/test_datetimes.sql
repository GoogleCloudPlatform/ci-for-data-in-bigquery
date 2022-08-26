SELECT
IF
  (
    -- The test
    DATETIME_DIFF(shipped_at, created_at, SECOND) >= 0,

    -- The pass message (per record)
    'Shipped date is after created date',

    -- The error message
    ERROR(CONCAT('Shipped date ', shipped_at, ' is before created date', created_at, ' at record with unique_key=', order_id)) )
FROM
  `${the_look_ecom_copy.orders}`
WHERE
  shipped_at IS NOT NULL AND created_at IS NOT NULL;
