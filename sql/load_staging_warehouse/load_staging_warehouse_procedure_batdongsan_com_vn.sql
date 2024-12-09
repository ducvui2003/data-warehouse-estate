# Load data từ bảng daily của db staging vào bảng fact trong db warehouse
# Thực hiện phân biệt dữ liệu cần cập nhập và dữ liệu cần thêm mới

DROP PROCEDURE IF EXISTS load_staging_warehouse_batdongsan_com_vn;
DELIMITER //
CREATE PROCEDURE load_staging_warehouse_batdongsan_com_vn()
BEGIN
    # 1. Ngày chưa hết hạn
    DECLARE dt_not_expired DATETIME default '9999-12-31 00:00:00';
    START TRANSACTION;
    # 2. Tạo bảng tạm estate_staging.temp_estate_update chứa dữ liệu cần cập nhập
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

    # 3. Xác định các row cần cập nhập trong estate_staging.estate_daily_batdongsan_com_vn
    # và insert vào bảng tạm estate_staging.temp_estate_update
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
     seller_email,
     start_date,
     end_date,
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
           daily.seller_email,
           daily.start_date,
           daily.end_date,
           daily.create_at
    FROM estate_warehouse.estate fact
             JOIN estate_staging.estate_daily_batdongsan_com_vn daily
                  ON fact.nk = daily.nk
    WHERE fact.active = TRUE
      AND fact.dt_expired = dt_not_expired
      AND (
        -- Kiểm tra các trạng thái thay đổi của dữ liệu của staging vs warehouse
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
            OR fact.seller_email <> daily.seller_email
            OR fact.start_date <> daily.start_date
            OR fact.end_date <> daily.end_date
            OR fact.create_at <> daily.create_at
        );

    # 4. Cập nhập các row hết hạn trong estate_warehouse.fact
    UPDATE estate_warehouse.estate fact
        JOIN estate_staging.temp_estate_update temp
        ON fact.sk = temp.sk
    SET fact.active     = FALSE,
        fact.dt_expired = NOW()
    WHERE fact.nk = temp.nk
      AND fact.active = TRUE;

    # 5. Thêm row trong estate_staging.temp_estate_update vào estate_warehouse.estate
    # với active = TRUE và dt_expired = '9999-12-31 00:00:00'
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
           dt_not_expired
    FROM estate_staging.temp_estate_update temp;


    # 5. Tạo bảng estate_staging.temp_estate_insert chứa các
    # row chưa có trong estate_staging.estate_daily_batdongsan_com_vn
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

    # 6.Thêm các row chưa có trong estate_staging.estate_daily_batdongsan_com_vn
    # vào estate_warehouse.estate
    INSERT INTO estate_staging.temp_estate_insert
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
     create_at)
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

    # 7. Thêm row trong estate_staging.temp_estate_insert vào estate_warehouse.estate
    # với active = TRUE và dt_expired = '9999-12-31 00:00:00'
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
           dt_not_expired
    FROM estate_staging.temp_estate_insert temp;


    # 8. Đếm số record trong estate_staging.estate_daily_batdongsan_com_vn
    SELECT COUNT(es.nk) as count_row
    FROM estate_staging.estate_daily_batdongsan_com_vn es;
    COMMIT;
END;
DELIMITER ;

CALL load_staging_warehouse_batdongsan_com_vn();