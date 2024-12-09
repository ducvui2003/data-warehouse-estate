use estate_controller;
DROP PROCEDURE IF EXISTS insert_log_staging;
DELIMITER //
Create PROCEDURE insert_log_staging(_config_id INT,
                                      _count_row INT,
                                      _error_file_name VARCHAR(200),
                                      _status VARCHAR(200)
)
BEGIN
    UPDATE logs
    SET time_end        = NOW(),
        file_name       = NULL,
        error_file_name = _error_file_name,
        count_row       = _count_row,
        is_delete       = 1
    WHERE status = 'STAGING_PENDING'
      AND config_id = _config_id;

    if _status = 'STAGING_ERROR' THEN
        INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                          create_at)
        VALUES (_config_id, NOW(), NULL, ' ', _error_file_name, 0, _status, NOW());
    ELSEIF _status = 'TRANSFORM_PENDING' THEN
        INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                          create_at)
        VALUES (_config_id, NOW(), NULL, ' ', ' ', _count_row, _status, NOW());
    END IF;
END //
DELIMITER ;
