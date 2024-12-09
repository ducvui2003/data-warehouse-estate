# Function chuyển đổi dữ liệu giá và diện tích
DROP FUNCTION IF EXISTS TransformPrice;
DELIMITER //
CREATE FUNCTION TransformPrice(
    area VARCHAR(255),
    price VARCHAR(255)
) RETURNS JSON
    DETERMINISTIC
BEGIN
    DECLARE area_transform DECIMAL(15, 2);
    DECLARE price_total DECIMAL(20, 3);
    DECLARE billion_value DECIMAL(20, 2);
    DECLARE million_value DECIMAL(20, 2);

    -- Convert area to decimal, check if area is NULL or empty
    IF area IS NULL OR TRIM(area) = '' THEN
        RETURN JSON_OBJECT('price_total', NULL, 'area_transform', NULL);
    END IF;

    SET area_transform = CAST(REPLACE(SUBSTRING(area, 1, LOCATE(' ', area) - 1), ',', '.') AS DECIMAL(15, 2));

    -- Calculate price_total based on price format
    IF price LIKE 'Thỏa thuận' THEN
        SET price_total = NULL;

    ELSEIF price LIKE '%/%' THEN
        -- Price per square meter (e.g., "60 triệu/m²")
        SET price_total = ROUND(CAST(REPLACE(SUBSTRING(price, 1, LOCATE(' ', price) - 1), ',', '.') AS DECIMAL(15, 2)) *
                                area_transform / 1000, 3);

    ELSEIF price LIKE '%tỷ%' AND price LIKE '%triệu%' THEN
        -- Handle cases like "1 tỷ 200 triệu"
        SET billion_value = CAST(REPLACE(SUBSTRING(price, 1, LOCATE(' tỷ', price) - 1), ',', '.') AS DECIMAL(20, 2));
        SET million_value = CAST(REPLACE(
                SUBSTRING(price, LOCATE(' tỷ', price) + 4, LOCATE(' triệu', price) - LOCATE(' tỷ', price) - 4), ',',
                '.') AS DECIMAL(20, 2)) / 1000;
        SET price_total = ROUND(billion_value + million_value, 3);

    ELSEIF price LIKE '%tỷ' THEN
        -- Handle cases like "1 tỷ"
        SET price_total =
                ROUND(CAST(REPLACE(SUBSTRING(price, 1, LOCATE(' ', price) - 1), ',', '.') AS DECIMAL(20, 2)), 3);

    ELSEIF price LIKE '%triệu' THEN
        -- Handle cases like "200 triệu"
        SET price_total =
                ROUND(CAST(REPLACE(SUBSTRING(price, 1, LOCATE(' ', price) - 1), ',', '.') AS DECIMAL(20, 2)) / 1000, 3);

    ELSE
        -- Default to direct decimal conversion
        SET price_total = ROUND(CAST(REPLACE(price, ',', '.') AS DECIMAL(20, 2)) / 1000000000, 3);
    END IF;

    -- Return only price_total_in_ty and area_transform
    RETURN JSON_OBJECT(
            'price_total_in_ty', IFNULL(price_total, NULL),
            'area_transform', IFNULL(area_transform, NULL)
           );
END //
DELIMITER ;

# Test
# SELECT TransformPrice('61 m²', '60 triệu/m²');
# SELECT JSON_EXTRACT(TransformPrice('100 m2', '1 tỷ 200 triệu'),'$.area_transform');
# SELECT TransformPrice('100 m2', '1 tỷ 200 triệu');
# SELECT TransformRoom('3 phòng ngủ');

# Function chuyển đổi dữ liệu phòng ngủ, phòng tắm, tầng
DROP FUNCTION IF EXISTS TransformRoom;
DELIMITER //
CREATE function TransformRoom(value varchar(255)) returns int
    deterministic
BEGIN
     DECLARE room INT;
    DECLARE first_space INT;

    -- Initialize the result
    SET room = 0;

    -- Check for NULL or empty input
    IF value IS NULL OR value = '' THEN
        RETURN room;
    END IF;

    -- Find the position of the first space
    SET first_space = LOCATE(' ', value);

    -- If a space is found, process the substring
    IF first_space > 0 THEN
        SET room = CAST(REPLACE(SUBSTRING(value, 1, first_space - 1), ',', '.') AS UNSIGNED);
    ELSE
        -- No space found; cast the entire string
        SET room = CAST(REPLACE(value, ',', '.') AS UNSIGNED);
    END IF;

    RETURN room;
END;
DELIMITER ;


# Function lấy province từ address
DROP FUNCTION IF EXISTS GetProvinceFromAddress;
DELIMITER //

CREATE FUNCTION GetProvinceFromAddress(
    input_string TEXT
) RETURNS TEXT
    DETERMINISTIC
BEGIN
    return TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(input_string, ',', -1), ',', 1));
END;
DELIMITER ;

# Test
SELECT GetProvinceFromAddress('Dự án Masteri Thảo Điền, Đường Xa Lộ Hà Nội, Phường Thảo Điền, Quận 2, Hồ Chí Minh');


# Function lấy district từ address
DROP FUNCTION IF EXISTS GetDistrictFromAddress;
DELIMITER //

CREATE FUNCTION GetDistrictFromAddress(
    input_string TEXT
) RETURNS TEXT
    DETERMINISTIC
BEGIN
    return TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(input_string, ',', -2), ',', 1));
END;
DELIMITER ;

# Test
# SELECT GetDistrictFromAddress('Dự án Masteri Thảo Điền, Đường Xa Lộ Hà Nội, Phường Thảo Điền, Quận 2, Hồ Chí Minh');


# Function lấy ward từ address
DROP FUNCTION IF EXISTS GetWardFromAddress;
DELIMITER //

CREATE FUNCTION GetWardFromAddress(
    input_string TEXT
) RETURNS TEXT
    DETERMINISTIC
BEGIN
    return TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(input_string, ',', -3), ',', 1));
END;
DELIMITER ;

# Test
# SELECT GetWardFromAddress('Dự án Masteri Thảo Điền, Đường Xa Lộ Hà Nội, Phường Thảo Điền, Quận 2, Hồ Chí Minh');


# Function lấy detail từ address
DROP FUNCTION IF EXISTS GetDetailFromAddress;
DELIMITER //

CREATE FUNCTION GetDetailFromAddress(
    input_string TEXT
) RETURNS TEXT
    DETERMINISTIC
BEGIN
    return TRIM(SUBSTRING_INDEX(input_string, ',',
                                LENGTH(input_string) - LENGTH(REPLACE(input_string, ',', '')) - 2));
END;
DELIMITER ;

# Test
# SELECT GetDetailFromAddress('Dự án Masteri Thảo Điền, Đường Xa Lộ Hà Nội, Phường Thảo Điền, Quận 2, Hồ Chí Minh');

