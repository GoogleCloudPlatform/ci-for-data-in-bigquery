ASSERT
  ((
    SELECT
      COUNT(*)
    FROM (
      SELECT
        *
      FROM
        `thelook_ecommerce.orders` AS o
      LEFT JOIN
        `thelook_ecommerce.users` AS u
      ON
        u.id = o.user_id
      WHERE
        o.user_id IS NULL)) = 0) AS "No invalid user_id in orders table";