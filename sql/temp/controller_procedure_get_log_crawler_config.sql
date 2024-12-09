use estate_controller;
DROP PROCEDURE IF EXISTS get_log_crawler_config;
DELIMITER //
Create PROCEDURE get_log_crawler_config()
BEGIN
    DECLARE ex INT DEFAULT 0;
    DECLARE _config_id INT DEFAULT 0;
    SELECT COUNT(*) AS count, logs.config_id
    INTO ex, _config_id
    FROM logs
             join configs on logs.config_id = configs.id AND configs.active = 1
    WHERE (logs.status = 'FILE_PENDING' OR logs.status = 'FILE_ERROR')
-- AND DATE(logs.create_at) = CURRENT_DATE  -- Uncomment if filtering by date
    GROUP BY logs.status, logs.config_id
    ORDER BY CASE
                 WHEN logs.status = 'FILE_PENDING' THEN 1
                 WHEN logs.status = 'FILE_ERROR' THEN 2
                 END
    LIMIT 1;

    IF ex != 0 THEN
        SELECT JSON_UNQUOTE(JSON_EXTRACT(configs.header_procedure, '$.get_log_crawler'))
        FROM configs
        WHERE configs.id = _config_id
        LIMIT 1;
    END IF;
end //
DELIMITER ;

#call get_log_crawler_config();