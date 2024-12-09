# Procedure chuyển đổi dữ liệu từ estate_daily_temp_batdongsan_com_vn sang estate_daily_batdongsan_com_vn
DROP PROCEDURE IF EXISTS transform_batdongsan_com_vn;
DELIMITER //
CREATE PROCEDURE transform_batdongsan_com_vn()
BEGIN
    START TRANSACTION;

    # 1. xóa các record không hợp lệ trong
    # estate_staging.estate_daily_temp_batdongsan_com_vn
    DELETE
    FROM estate_staging.estate_daily_temp_muaban_net
    WHERE subject IS NULL
       OR `area` IS NULL
       or `area` = ""
       or `price` LIKE 'Thỏa thuận'
       or `price` = ""
       OR `price` is NULL
       OR address IS NULL
       or address = "";

    # 2. Tạo bảng tạm estate.temp_date
    DROP TEMPORARY TABLE IF EXISTS estate_staging.temp_date;
    CREATE TEMPORARY TABLE estate_staging.temp_date
    (
        sk         BIGINT PRIMARY KEY,
        create_at  INT,
        start_date INT,
        end_date   INT
    );

    # 3. Chuyển đổi các field date string trong
    # estate_staging.estate_daily_temp_batdongsan_com_vn
    # sang id của estate_staging.dim_date
    INSERT INTO estate_staging.temp_date (sk, create_at, start_date, end_date)
    SELECT temp.sk,
           MAX(CASE WHEN dd.full_date = STR_TO_DATE(temp.create_at, '%d/%m/%Y') THEN dd.sk END)  AS create_at,
           MAX(CASE WHEN dd.full_date = STR_TO_DATE(temp.start_date, '%d/%m/%Y') THEN dd.sk END) AS start_date,
           MAX(CASE WHEN dd.full_date = STR_TO_DATE(temp.end_date, '%d/%m/%Y') THEN dd.sk END)   AS end_date
    FROM estate_staging.estate_daily_temp_batdongsan_com_vn temp
             JOIN dim_date dd ON STR_TO_DATE(dd.full_date, '%Y-%m-%d')
        IN (
            STR_TO_DATE(temp.start_date, '%d/%m/%Y'),
            STR_TO_DATE(temp.create_at, '%d/%m/%Y'),
            STR_TO_DATE(temp.end_date, '%d/%m/%Y')
                                     )
    GROUP BY temp.sk;


    # 3. Tạo bảng tạm temp_address
    DROP TEMPORARY TABLE IF EXISTS estate_staging.temp_address;
    CREATE TEMPORARY TABLE estate_staging.temp_address
    (
        sk      BIGINT PRIMARY KEY,
        ward_id VARCHAR(20),
        detail  VARCHAR(255)
    );

    # 4.  Chuyển đổi address string trong
    # estate_staging.estate_daily_temp_batdongsan_com_vn
    # sang ward_id trong dim_ward và address_detail
    INSERT INTO estate_staging.temp_address (sk, ward_id, detail)
    SELECT temp.sk,
           w.sk,
           get_detail_from_address(temp.address) AS short_address
    FROM estate_staging.estate_daily_temp_batdongsan_com_vn temp
             JOIN estate_staging.dim_wards w ON
        w.full_name = get_ward_from_address(temp.address) OR
        w.name = get_ward_from_address(temp.address)
             JOIN estate_staging.dim_districts d ON
        d.full_name = get_district_from_address(temp.address) OR
        d.name = get_district_from_address(temp.address)
             JOIN estate_staging.dim_provinces p ON
        p.full_name = get_province_from_address(temp.address) OR
        p.name = get_province_from_address(temp.address)
    WHERE w.district_sk = d.sk
      AND d.province_sk = p.sk;

    # 5. Chuyển đổi các giá trị từ
    # estate_staging.estate_daily_temp_batdongsan_com_vn
    # sang estate_staging.estate_daily_batdongsan_com_vn
    # không thêm các record có giá trị price và area <= 0
    INSERT INTO estate_staging.estate_daily_batdongsan_com_vn
    (sk,
     nk,
     subject,
     ward_sk,
     address_detail,
     price,
     area,
     description,
     floors,
     orientation,
     bedroom,
     bathroom,
     legal,
     url,
     seller_name,
     seller_email,
     start_date,
     end_date,
     create_at)
    SELECT edt.sk                                                                    AS sk,
           edt.nk                                                                    AS nk,
           edt.subject                                                               AS subject,
           ta.ward_id                                                                AS ward_sk,
           ta.detail                                                                 AS address_detail,
           JSON_EXTRACT(transform_price(edt.area, edt.price), '$.price_total_in_ty') AS price,
           JSON_EXTRACT(transform_price(edt.area, edt.price), '$.area_transform')    AS area,
           edt.description                                                           as description,
           transform_room(edt.floors)                                                as floors,
           edt.orientation                                                           as orientation,
           transform_room(edt.bedroom)                                               as bedroom,
           transform_room(edt.bathroom)                                              as bathroom,
           edt.legal                                                                 as legal,
           edt.url                                                                   as url,
           edt.full_name                                                             as seller_name,
           edt.email                                                                 as seller_email,
           td.start_date                                                             as start_date,
           td.end_date                                                               as end_date,
           td.create_at                                                              as create_at
    FROM estate_staging.estate_daily_temp_batdongsan_com_vn edt
             JOIN temp_date td ON edt.sk = td.sk
             JOIN temp_address ta ON edt.sk = ta.sk
    WHERE JSON_EXTRACT(transform_price(edt.area, edt.price), '$.price_total_in_ty') > 0
      AND JSON_EXTRACT(transform_price(edt.area, edt.price), '$.area_transform') > 0;

    # 6. Đếm số record trong estate_staging.estate_daily_batdongsan_com_vn
    SELECT COUNT(es.nk) as count_row
    FROM estate_staging.estate_daily_batdongsan_com_vn es;
    COMMIT;
END;
DELIMITER ;

#Test
# TRUNCATE TABLE estate_daily_batdongsan_com_vn; # Dòng này sẽ chạy bên phần load file lên temp
CALL transform_batdongsan_com_vn();
