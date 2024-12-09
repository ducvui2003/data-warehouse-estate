# Function chuyển đổi dữ liệu giá và diện tích
DROP FUNCTION IF EXISTS transform_price;
DELIMITER //
CREATE FUNCTION transform_price(
    area VARCHAR(255),
    price VARCHAR(255)
) RETURNS JSON
    DETERMINISTIC
BEGIN
    # 1. Khởi tạo các biến
    # - area_transform: diện tích đã chuyển đổi (m2)
    # - price_total: giá trị tổng (tỷ)
    # - billion_value: giá trị tỷ
    # - million_value: giá trị triệu
    DECLARE area_transform DECIMAL(15, 2);
    DECLARE price_total DECIMAL(20, 3);
    DECLARE billion_value DECIMAL(20, 2);
    DECLARE million_value DECIMAL(20, 2);

    # 2. Lấy ra giá trị của diện tích lưu vào area_transform
    SET area_transform = CAST(REPLACE(SUBSTRING(area, 1, LOCATE(' ', area) - 1), ',', '.') AS DECIMAL(15, 2));

    # 3. Kiểm tra price_input == 'Thỏa thuận'
    IF price LIKE 'Thỏa thuận' THEN
        # 3.1 Đặt giá trị price_total = NULL
        SET price_total = NULL;

    # 4. Giá chứa dấu ngăn cách "/"
    ELSEIF price LIKE '%/%' THEN
        # 4.1 Trích xuất giá trị tổng lưu vào price_total
        SET price_total = ROUND(CAST(REPLACE(SUBSTRING(price, 1, LOCATE(' ', price) - 1), ',', '.') AS DECIMAL(15, 2)) *
                                area_transform / 1000, 3);

    # 5. Giá chứa 'tỷ' và 'triệu'
    ELSEIF price LIKE '%tỷ%' AND price LIKE '%triệu%' THEN

        # 5.1 Trích xuất giá trị hàng tỷ lưu vào billion_value
        SET billion_value = CAST(REPLACE(SUBSTRING(price, 1, LOCATE(' tỷ', price) - 1), ',', '.') AS DECIMAL(20, 2));
        # 5.2 Trích xuất giá trị triệu vào million_value
        SET million_value = CAST(REPLACE(
                SUBSTRING(price, LOCATE(' tỷ', price) + 4, LOCATE(' triệu', price) - LOCATE(' tỷ', price) - 4), ',',
                '.') AS DECIMAL(20, 2)) / 1000;
        # 5.3 Tính giá trị tổng của 5.1 và 5.2 và lưu vào total_price
        SET price_total = ROUND(billion_value + million_value, 3);

    # 6. Giá chứa 'tỷ'
    ELSEIF price LIKE '%tỷ' THEN
        # 6.1 Trích xuất giá trị tỷ lưu vào price_total
        SET price_total =
                ROUND(CAST(REPLACE(SUBSTRING(price, 1, LOCATE(' ', price) - 1), ',', '.') AS DECIMAL(20, 2)), 3);

    # 7. Giá chứa 'triệu'
    ELSEIF price LIKE '%triệu' THEN
        # 7.1 Trích xuất giá trị triệu và lưu vào price_total
        SET price_total =
                ROUND(CAST(REPLACE(SUBSTRING(price, 1, LOCATE(' ', price) - 1), ',', '.') AS DECIMAL(20, 2)) / 1000, 3);
    ELSE

        SET price_total = ROUND(CAST(REPLACE(price, ',', '.') AS DECIMAL(20, 2)) / 1000000000, 3);
    END IF;

    # 8. Trả về kết quả gồm price_total và area_transform
    RETURN JSON_OBJECT(
            'price_total_in_ty', IFNULL(price_total, -1),
            'area_transform', IFNULL(area_transform, -1)
           );
END //
DELIMITER ;

# Test
SELECT transform_price('61 m²', '60 triệu/m²');
# SELECT JSON_EXTRACT(TransformPrice('100 m2', '1 tỷ 200 triệu'),'$.area_transform');
# SELECT TransformPrice('100 m2', '1 tỷ 200 triệu');
# SELECT TransformRoom('3 phòng ngủ');
