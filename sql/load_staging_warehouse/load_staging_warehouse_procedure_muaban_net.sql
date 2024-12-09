DROP PROCEDURE IF EXISTS load_staging_warehouse_muaban_net;
DELIMITER //
CREATE PROCEDURE load_staging_warehouse_muaban_net()
BEGIN
    START TRANSACTION;
-- Tạo bảng estate_staging.temp_estate_update chứa dữ liệu cần cập nhập
    DROP TEMPORARY TABLE IF EXISTS estate_staging.temp_estate_update;
    CREATE TEMPORARY TABLE estate_staging.temp_estate_update
    (
        sk              bigint primary key,
        nk              varchar(100),
        subject         varchar(255) null,
        price           double       null,
        area            double       null,
        ward_sk         varchar(20)  null,
        address_detail  varchar(255) null,
        description     longtext     null,
        orientation     varchar(255) null,
        floors          int          null,
        bedroom         int          null,
        bathroom        int          null,
        images          json         null,
        legal           varchar(255) null,
        url             varchar(255) null,
        start_date      int          null,
        end_date        int          null,
        create_at       int          null,
        seller_fullname varchar(255) null,
        seller_phone    varchar(50)  null,
        seller_email    varchar(255) null
    );

    -- Xác định các row cần cập nhập trong estate_staging.estate_daily_batdongsan_com_vn
-- và insert vào bảng tạm estate_staging.temp_estate_update
    INSERT INTO estate_staging.temp_estate_update
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
     images,
     seller_fullname,
     create_at)
    SELECT fact.sk,
           daily.nk,
           daily.subject,
           daily.ward_sk,
           daily.address_detail,
           daily.price,
           daily.area,
           daily.description,
           daily.floors,
           daily.orientation,
           daily.bedroom,
           daily.bathroom,
           daily.legal,
           daily.url,
           daily.images,
           daily.seller_name,
           daily.create_at
    FROM estate_warehouse.estate fact
             JOIN estate_staging.estate_daily_muaban_net daily
                  ON fact.nk = daily.nk
    WHERE fact.active = TRUE
      AND fact.dt_expired = '9999-12-31 00:00:00'
      AND (
        fact.subject <> daily.subject
            OR fact.ward_sk <> daily.ward_sk
            OR fact.address_detail <> daily.address_detail
            OR fact.price <> daily.price
            OR fact.area <> daily.area
            OR fact.description <> daily.description
            OR fact.floors <> daily.floors
            OR fact.orientation <> daily.orientation
            OR fact.bedroom <> daily.bedroom
            OR fact.bathroom <> daily.bathroom
            OR fact.legal <> daily.legal
            OR fact.url <> daily.url
            OR fact.images <> daily.images
            OR fact.seller_fullname <> daily.seller_name
            OR fact.create_at <> daily.create_at
        );


-- update các row hết hạn trong estate_warehouse.estate
    UPDATE estate_warehouse.estate fact
        JOIN estate_staging.temp_estate_update temp
        ON fact.sk = temp.sk
    SET fact.active     = FALSE,
        fact.dt_expired = NOW()
    WHERE fact.nk = temp.nk
      AND fact.active = TRUE;

    -- Thêm row trong estate_staging.temp_estate_update vào estate_warehouse.estate
-- với active = TRUE và dt_expired = '9999-12-31 00:00:00'
    INSERT INTO estate_warehouse.estate
    (nk,
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
     images,
     seller_fullname,
     seller_email,
     start_date,
     end_date,
     create_at,
     active,
     dt_created,
     dt_expired)
    SELECT temp.nk,
           temp.subject,
           temp.ward_sk,
           temp.address_detail,
           temp.price,
           temp.area,
           temp.description,
           temp.floors,
           temp.orientation,
           temp.bedroom,
           temp.bathroom,
           temp.legal,
           temp.url,
           temp.images,
           temp.seller_fullname,
           temp.seller_email,
           temp.start_date,
           temp.end_date,
           temp.create_at,
           TRUE,
           NOW(),
           '9999-12-31 00:00:00'
    FROM estate_staging.temp_estate_update temp;


    -- Tạo bảng estate_staging.temp_estate_insert chứa các
-- row chưa có trong estate_staging.estate_daily_batdongsan_com_vn
    DROP TEMPORARY TABLE IF EXISTS estate_staging.temp_estate_insert;
    CREATE TEMPORARY TABLE estate_staging.temp_estate_insert
    (
        nk              varchar(100) primary key,
        subject         varchar(255) null,
        price           double       null,
        area            double       null,
        ward_sk         varchar(20)  null,
        address_detail  varchar(255) null,
        description     longtext     null,
        orientation     varchar(255) null,
        floors          int          null,
        bedroom         int          null,
        bathroom        int          null,
        images          json         null,
        legal           varchar(255) null,
        url             varchar(255) null,
        start_date      int          null,
        end_date        int          null,
        create_at       int          null,
        seller_fullname varchar(255) null,
        seller_phone    varchar(50)  null,
        seller_email    varchar(255) null
    );

    -- insert các row chưa có trong estate_staging.estate_daily_batdongsan_com_vn
-- vào estate_warehouse.estate
    INSERT INTO estate_staging.temp_estate_insert
    (nk,
     subject,
     ward_sk, address_detail, price, area,
     description, floors, orientation, bedroom, bathroom, legal, url,
     images,
     seller_fullname, seller_email, start_date, end_date, create_at)
    SELECT DISTINCT daily.nk,
                    daily.subject,
                    daily.ward_sk,
                    daily.address_detail,
                    daily.price,
                    daily.area,
                    daily.description,
                    daily.floors,
                    daily.orientation,
                    daily.bedroom,
                    daily.bathroom,
                    daily.legal,
                    daily.url,
                    daily.images,
                    daily.seller_name,
                    daily.seller_email,
                    daily.start_date,
                    daily.end_date,
                    daily.create_at
    FROM estate_staging.estate_daily_batdongsan_com_vn daily
    WHERE not exists (select 1 from estate_warehouse.estate where nk = daily.nk);

    -- Thêm row trong estate_staging.temp_estate_insert vào estate_warehouse.estate
-- với active = TRUE và dt_expired = '9999-12-31 00:00:00'
    INSERT INTO estate_warehouse.estate (nk, subject, ward_sk, address_detail, price, area,
                                         description, floors, orientation, bedroom, bathroom, legal, url,
                                         images,
                                         seller_fullname, seller_email, start_date, end_date, create_at,
                                         active,
                                         dt_created, dt_expired)
    SELECT temp.nk,
           temp.subject,
           temp.ward_sk,
           temp.address_detail,
           temp.price,
           temp.area,
           temp.description,
           temp.floors,
           temp.orientation,
           temp.bedroom,
           temp.bathroom,
           temp.legal,
           temp.url,
           temp.images,
           temp.seller_fullname,
           temp.seller_email,
           temp.start_date,
           temp.end_date,
           temp.create_at,
           TRUE,
           NOW(),
           '9999-12-31 00:00:00'
    FROM estate_staging.temp_estate_insert temp;
    COMMIT;
END;
DELIMITER ;

CALL load_staging_warehouse_muaban_net();