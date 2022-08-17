ASSERT
  ((
    SELECT
      COUNT(*)
    FROM (
      SELECT
        *
      FROM
        `${thelook_ecommerce.order_items}` AS oi
      LEFT JOIN
        `${thelook_ecommerce.orders}` AS o
      ON
        o.order_id = oi.order_id
      WHERE
        o.order_id IS NULL)) = 0) AS "No invalid oder_id in orders table";