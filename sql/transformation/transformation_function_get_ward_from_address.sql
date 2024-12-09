# Function lấy ward từ address
DROP FUNCTION IF EXISTS get_ward_from_address;
DELIMITER //

CREATE FUNCTION get_ward_from_address(
    input_string TEXT
) RETURNS TEXT
    DETERMINISTIC
BEGIN
    return TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(input_string, ',', -3), ',', 1));
END;
DELIMITER ;

# Test
# SELECT GetWardFromAddress('Dự án Masteri Thảo Điền, Đường Xa Lộ Hà Nội, Phường Thảo Điền, Quận 2, Hồ Chí Minh');