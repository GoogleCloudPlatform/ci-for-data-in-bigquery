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

# [START setup_no_missing_users_test]
cat <<EOF > sql_tests/no_missing_users_in_orders_table.sql
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
        u.id IS NULL)) = 0) AS "No invalid user_id in orders table";
EOF
# [END setup_no_missing_users_test]

# [START setup_no_missing_orders_test]
cat <<EOF > sql_tests/no_missing_orders_in_order_items_table.sql
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
        o.order_id IS NULL)) = 0) AS "No invalid order_id in orders table";
EOF
# [END setup_no_missing_orders_test]
