# Function lấy detail từ address
DROP FUNCTION IF EXISTS get_detail_from_address;
DELIMITER //

CREATE FUNCTION get_detail_from_address(
    input_string TEXT
) RETURNS TEXT
    DETERMINISTIC
BEGIN
    return TRIM(SUBSTRING_INDEX(input_string, ',',
                                LENGTH(input_string) - LENGTH(REPLACE(input_string, ',', '')) - 2));
END;
DELIMITER ;