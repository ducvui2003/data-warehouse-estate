DROP PROCEDURE IF EXISTS get_log_crawler;
CREATE PROCEDURE get_log_crawler()
BEGIN
    DECLARE _id INT DEFAULT 0;
    DECLARE _config_id INT DEFAULT 0;
#     Check if there is a FILE_PROCESSING status
    IF !EXISTS(SELECT 'FILE_PROCESSING'
               FROM logs
               WHERE status = 'FILE_PROCESSING'
                 AND is_delete = 0
               LIMIT 1) THEN

        SELECT logs.id, logs.config_id
        INTO _id, _config_id
        FROM logs
                 join configs on logs.config_id = configs.id AND configs.active = 1
        WHERE (logs.status = 'FILE_PENDING' OR logs.status = 'FILE_ERROR')
          AND is_delete = 0
          AND DATE(logs.create_at) = CURRENT_DATE -- Uncomment if filtering by date
        #     GROUP BY logs.id, logs.config_id
        ORDER BY CASE
                     WHEN logs.status = 'FILE_PENDING' THEN 1
                     WHEN logs.status = 'FILE_ERROR' THEN 2
                     END
        LIMIT 1;

        IF _config_id != 0 THEN

            UPDATE logs
            SET logs.is_delete = 1
            WHERE logs.config_id = _config_id;
            #               AND (logs.status = 'FILE_PENDING' OR logs.status = 'FILE_ERROR')
#               AND DATE(logs.create_at) = CURRENT_DATE;

            INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                              create_at)
            VALUES (_config_id, NOW(), NULL, ' ', ' ', 0, 'FILE_PROCESSING', NOW());

        END IF;


        SELECT configs.id,
               configs.email,
               configs.data_dir_path,
               configs.error_dir_path,
               configs.file_extension,
               configs.file_format,
               configs.prefix,
               configs.prefix_error,
               resources.scenario,
               resources.base_url,
               resources.limit_page,
               resources.paging_pattern,
               resources.source_page,
               resources.purpose,
               resources.navigate_scenario
        FROM resources
                 JOIN configs ON resources.id = configs.resource_id
                 JOIN logs ON logs.config_id = configs.id
        WHERE configs.id = _config_id
          AND logs.id = _id
          AND active = 1;
    END IF;
END;
DeLIMITER ;

DROP PROCEDURE IF EXISTS insert_log_crawler;
CREATE PROCEDURE insert_log_crawler(_config_id INT,
                                    _file_name VARCHAR(200), _error_file_name VARCHAR(200), _count_row INT,
                                    _status VARCHAR(200))
BEGIN
    UPDATE logs
    SET time_end        = NOW(),
        file_name       = _file_name,
        error_file_name = _error_file_name,
        count_row       = _count_row,
        is_delete       = 1
    WHERE status = 'FILE_PROCESSING'
      AND is_delete = 0
      AND config_id = _config_id;

    if _status = 'FILE_ERROR' THEN
        INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                          create_at)
        VALUES (_config_id, NOW(), NULL, ' ', ' ', 0, _status, NOW());
    ELSE
        INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                          create_at)
        VALUES (_config_id, NOW(), NULL, ' ', ' ', 0, _status, NOW());
    END IF;
END;
DeLIMITER ;

CALL get_log_crawler();

-- STAGING_PENDING nếu crawl thành công
-- FILE_ERROR nếu thất bại
CALL insert_log_crawler(1, 'file_name', '', 10, 'STAGING_PENDING');