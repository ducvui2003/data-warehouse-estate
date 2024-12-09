# Function lấy district từ address
DROP FUNCTION IF EXISTS get_district_from_address;
DELIMITER //

CREATE FUNCTION get_district_from_address(
    input_string TEXT
) RETURNS TEXT
    DETERMINISTIC
BEGIN
    return TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(input_string, ',', -2), ',', 1));
END;
DELIMITER ;


# Test
# SELECT GetDistrictFromAddress('Dự án Masteri Thảo Điền, Đường Xa Lộ Hà Nội, Phường Thảo Điền, Quận 2, Hồ Chí Minh');