# Function chuyển đổi dữ liệu phòng ngủ, phòng tắm, tầng
DROP FUNCTION IF EXISTS transform_room;
DELIMITER //
CREATE function transform_room(value varchar(255)) returns int
    deterministic
BEGIN
    # 1. Khai báo biến room =0 và first_space
    DECLARE room INT DEFAULT 0;
    DECLARE first_space INT DEFAULT 0;

    # 2. Kiểm tra value = NULL hoặc = ''
    IF value IS NULL OR value = '' THEN
        # 2.1 Trả về giá trị room = 0
        RETURN room;
    END IF;

    # 3. Lấy ra index của dấu cách đầu tiên
    SET first_space = LOCATE(' ', value);

    # 4. Kiểm tra first_space > 0
    IF first_space > 0 THEN
        # 4.1 Trích xuất giá trị trước frist_space và chuyển sang số và set vào room
        SET room = CAST(REPLACE(SUBSTRING(value, 1, first_space - 1), ',', '.') AS UNSIGNED);
    END IF;

    # 5. Trả về giá trị room
    RETURN room;
END;
DELIMITER ;

# Test
SELECT transform_room('3 phòng ngủ');