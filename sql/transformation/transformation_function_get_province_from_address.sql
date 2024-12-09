# Function lấy province từ address
DROP FUNCTION IF EXISTS get_province_from_address;
DELIMITER //

CREATE FUNCTION get_province_from_address(
    input_string TEXT
) RETURNS TEXT
    DETERMINISTIC
BEGIN
    return TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(input_string, ',', -1), ',', 1));
END;
DELIMITER ;

# Test
SELECT get_province_from_address('Dự án Masteri Thảo Điền, Đường Xa Lộ Hà Nội, Phường Thảo Điền, Quận 2, Hồ Chí Minh');