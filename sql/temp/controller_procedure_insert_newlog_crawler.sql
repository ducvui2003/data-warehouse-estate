use estate_controller;
DROP PROCEDURE IF EXISTS insert_new_log_crawler;
DELIMITER //
CREATE PROCEDURE
    insert_new_log_crawler()
BEGIN
    INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status, create_at)
    VALUES (1, NOW(), NULL, ' ', ' ', 0, 'FILE_PENDING', NOW()),
           (2, NOW(), NULL, ' ', ' ', 0, 'FILE_PENDING', NOW());
END //
DELIMITER ;
