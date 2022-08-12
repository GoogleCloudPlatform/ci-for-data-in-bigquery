SELECT
IF
  (
    -- The test
    status IN ('Shipped',
      'Complete',
      'Returned',
      'Cancelled',
      'Processing'),
    -- The pass message (per record)
    CONCAT('status ', status, ' is valid'),
    -- The error message
    ERROR(CONCAT('Status ', status, ' is not valid at record with unique_key=', order_id)) ) AS status_is_correct
FROM
  `${thelook_ecommerce.orders}`;