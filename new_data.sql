-- [START insert_data]
INSERT INTO `devenv1.clone_20220816140511_orders` VALUES (125080, 100001, 'Shiped', 'M', '2022-08-01T10:12:14', NULL, '2022-07-31T10:12:14', NULL, 5);
INSERT INTO `devenv1.clone_20220816140511_order_items` VALUES (180462, 125080, 100001, 29120, 457588, 'Shipped', '2022-08-01T10:12:14', '2022-07-31T10:12:14', NULL, NULL, 78.0);
-- [END insert_data]

-- [START update_data]
UPDATE `devenv1.clone_20220819104829_orders`
    SET
        user_id = 100000,
        status = 'Shipped',
        shipped_at = '2022-08-02T10:12:14'
    WHERE
        order_id = 125080;
UPDATE `devenv1.clone_20220819104829_order_items`
    SET
        user_id = 100000,
        shipped_at = '2022-08-02T10:12:14'
    WHERE
        order_id = 125080;
-- [END update_data]
