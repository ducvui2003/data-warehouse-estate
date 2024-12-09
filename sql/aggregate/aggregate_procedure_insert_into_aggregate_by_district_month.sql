DROP PROCEDURE IF EXISTS insert_into_aggregate_by_district_month;
DELIMITER
$$

CREATE PROCEDURE `insert_into_aggregate_by_district_month`()
BEGIN
    -- Chèn dữ liệu vào bảng aggregate_by_district_month
    INSERT INTO `estate_warehouse`.`aggregate_by_district_month`
    (district_sk, year_month_sk, total_properties, avg_bedroom, avg_bathroom, min_bedroom, max_bedroom, min_bathroom,
     max_bathroom, avg_price, avg_area, avg_price_per_sqm)
    SELECT d.sk                                                                                        AS district_sk,
           d_month.sk                                                                                  AS year_month_sk,
           COUNT(e.sk)                                                                                 AS total_properties, -- Tổng số bất động sản
           ROUND(AVG(CAST(e.bedroom AS DECIMAL)))                                                      AS avg_bedroom,      -- Trung bình số phòng ngủ
           ROUND(AVG(CAST(e.bathroom AS DECIMAL)))                                                     AS avg_bathroom,     -- Trung bình số phòng tắm
           MIN(CAST(CASE WHEN e.bedroom REGEXP '^[0-9]+$' THEN e.bedroom ELSE NULL END AS UNSIGNED))   AS min_bedroom,      -- Số phòng ngủ ít nhất
           MAX(CAST(CASE WHEN e.bedroom REGEXP '^[0-9]+$' THEN e.bedroom ELSE NULL END AS UNSIGNED))   AS max_bedroom,      -- Số phòng ngủ nhiều nhất
           MIN(CAST(CASE WHEN e.bathroom REGEXP '^[0-9]+$' THEN e.bathroom ELSE NULL END AS UNSIGNED)) AS min_bathroom,     -- Số phòng tắm ít nhất
           MAX(CAST(CASE WHEN e.bathroom REGEXP '^[0-9]+$' THEN e.bathroom ELSE NULL END AS UNSIGNED)) AS max_bathroom,     -- Số phòng tắm nhiều nhất
           ROUND(AVG(CAST(e.price AS DECIMAL)))                                                        AS avg_price,        -- Giá bất động sản trung bình
           ROUND(AVG(CAST(e.area AS DECIMAL)))                                                         AS avg_area,         -- Trung bình diện tích
           ROUND(AVG(CAST(e.price AS DECIMAL)) / NULLIF(AVG(CAST(e.area AS DECIMAL)), 0),
                 2)                                                                                    AS avg_price_per_sqm -- Giá bất động sản trung bình trên một mét vuông
    FROM `estate_warehouse`.`estate` e
             JOIN `estate_warehouse`.`dim_districts` d ON e.ward_sk = d.sk
             JOIN `estate_warehouse`.`dim_provinces` p ON d.province_sk = p.sk
             JOIN `estate_warehouse`.`dim_month` d_month ON e.start_date = d_month.date_sk_start
    WHERE p.name = 'Hồ Chí Minh' -- Lấy theo khu vực tỉnh Hồ chí minh
    GROUP BY d.sk, d_month.sk;
END $$

DELIMITER ;

CALL insert_into_aggregate_by_district_month();


SELECT d.sk                                                                                        AS district_sk,
       d_month.sk                                                                                  AS year_month_sk,
       COUNT(e.sk)                                                                                 AS total_properties, -- Tổng số bất động sản
       ROUND(AVG(CAST(e.bedroom AS DECIMAL)))                                                      AS avg_bedroom,      -- Trung bình số phòng ngủ
       ROUND(AVG(CAST(e.bathroom AS DECIMAL)))                                                     AS avg_bathroom,     -- Trung bình số phòng tắm
       MIN(CAST(CASE WHEN e.bedroom REGEXP '^[0-9]+$' THEN e.bedroom ELSE NULL END AS UNSIGNED))   AS min_bedroom,      -- Số phòng ngủ ít nhất
       MAX(CAST(CASE WHEN e.bedroom REGEXP '^[0-9]+$' THEN e.bedroom ELSE NULL END AS UNSIGNED))   AS max_bedroom,      -- Số phòng ngủ nhiều nhất
       MIN(CAST(CASE WHEN e.bathroom REGEXP '^[0-9]+$' THEN e.bathroom ELSE NULL END AS UNSIGNED)) AS min_bathroom,     -- Số phòng tắm ít nhất
       MAX(CAST(CASE WHEN e.bathroom REGEXP '^[0-9]+$' THEN e.bathroom ELSE NULL END AS UNSIGNED)) AS max_bathroom,     -- Số phòng tắm nhiều nhất
       ROUND(AVG(CAST(e.price AS DECIMAL)))                                                        AS avg_price,        -- Giá bất động sản trung bình
       ROUND(AVG(CAST(e.area AS DECIMAL)))                                                         AS avg_area,         -- Trung bình diện tích
       ROUND(AVG(CAST(e.price AS DECIMAL)) / NULLIF(AVG(CAST(e.area AS DECIMAL)), 0),
             2)                                                                                    AS avg_price_per_sqm -- Giá bất động sản trung bình trên một mét vuông
FROM `estate_warehouse`.`estate` e
         JOIN `estate_warehouse`.`dim_districts` d ON e.ward_sk = d.sk
         JOIN `estate_warehouse`.`dim_provinces` p ON d.province_sk = p.sk

         JOIN `estate_warehouse`.`dim_month` d_month ON e.start_date = d_month.date_sk_start
WHERE p.name = 'Hồ Chí Minh' -- Lấy theo khu vực tỉnh Hồ chí minh
GROUP BY d.sk, d_month.sk;


SELECT *
FROM `estate_warehouse`.`estate` e
         JOIN estate_warehouse.dim_wards dw ON e.ward_sk = dw.sk
         JOIN `estate_warehouse`.`dim_districts` d ON dw.district_sk = d.sk
         JOIN `estate_warehouse`.`dim_provinces` p ON d.province_sk = p.sk
         JOIN estate_warehouse.dim_date dd on dd.sk = e.start_date
         JOIN `estate_warehouse`.`dim_month` d_month ON dd.sk = d_month.date_sk_start