use estate_controller;
DROP PROCEDURE IF EXISTS insert_log_crawler;
DELIMITER //
Create PROCEDURE insert_log_crawler(_config_id INT,
                                    _file_name VARCHAR(200),
                                    _error_file_name VARCHAR(200), _count_row INT,
                                    _status VARCHAR(200)
)
BEGIN
    UPDATE logs
    SET time_end        = NOW(),
        file_name       = _file_name,
        error_file_name = _error_file_name,
        count_row       = _count_row,
        is_delete       = 1
    WHERE status = 'FILE_PROCESSING'
      AND config_id = _config_id;

    if _status = 'FILE_ERROR' THEN
        INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                          create_at)
        VALUES (_config_id, NOW(), NULL, ' ', ' ', 0, _status, NOW());
    ELSEIF _status = 'STAGING_PENDING' THEN
        INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                          create_at)
        VALUES (_config_id, NOW(), NULL, ' ', ' ', 0, _status, NOW());
    END IF;
END //
DELIMITER ;