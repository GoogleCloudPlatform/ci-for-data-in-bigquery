ASSERT
  ((
    SELECT
      COUNT(*)
    FROM (
      SELECT
        *
      FROM
        `${the_look_ecom_copy.orders}` AS o
      LEFT JOIN
        `${the_look_ecom_copy.users}` AS u
      ON
        u.id = o.user_id
      WHERE
        u.id IS NULL)) = 0) AS "orders table has a non-existing user_id";
ASSERT
  ((
    SELECT
      COUNT(*)
    FROM (
      SELECT
        *
      FROM
        `${the_look_ecom_copy.order_items}` AS oi
      LEFT JOIN
        `${the_look_ecom_copy.orders}` AS o
      ON
        o.order_id = oi.order_id
      WHERE
        o.order_id IS NULL)) = 0) AS "order_items has an non-existing order_id";
