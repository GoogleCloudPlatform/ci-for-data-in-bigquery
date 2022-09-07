#!/usr/bin/env sh

# [START setup_status_test]
cat <<EOF > sql_tests/status_is_valid.sql
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
  \`\${the_look_ecom_copy.orders}\`;
EOF
# [END setup_status_test]


# [START setup_datetime_tests]
cat <<EOF > sql_tests/test_datetimes.sql
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
  \`\${the_look_ecom_copy.orders}\`
WHERE
  shipped_at IS NOT NULL AND created_at IS NOT NULL;
EOF
# [END setup_datetime_tests]

# [START setup_no_missing_joins_1]
cat <<EOF > sql_tests/no_missing_joins.sql
ASSERT
  ((
    SELECT
      COUNT(*)
    FROM (
      SELECT
        *
      FROM
        \`\${the_look_ecom_copy.orders}\` AS o
      LEFT JOIN
        \`\${the_look_ecom_copy.users}\` AS u
      ON
        u.id = o.user_id
      WHERE
        u.id IS NULL)) = 0) AS "orders table has a non-existing user_id";
EOF
# [END setup_no_missing_joins_1]

# [START setup_no_missing_joins_2]
cat <<EOF >> sql_tests/no_missing_joins.sql
ASSERT
  ((
    SELECT
      COUNT(*)
    FROM (
      SELECT
        *
      FROM
        \`\${the_look_ecom_copy.order_items}\` AS oi
      LEFT JOIN
        \`\${the_look_ecom_copy.orders}\` AS o
      ON
        o.order_id = oi.order_id
      WHERE
        o.order_id IS NULL)) = 0) AS "order_items has an non-existing order_id";
EOF
# [END setup_no_missing_joins_2]
