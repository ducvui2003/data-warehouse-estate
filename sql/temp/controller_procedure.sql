use estate_controller;
DELIMITER //

CREATE PROCEDURE get_database_config(IN dbName varchar(200))
BEGIN
    SELECT config_databases.*
    FROM type_databases
             JOIN config_databases
                  ON type_databases.id = config_databases.type_database_id
    WHERE type_databases.type_name = dbName;
END //

# CREATE PROCEDURE get_database_config_by_name(IN dbName varchar(200))
# BEGIN
#     DECLARE header_staging JSON;
#     SELECT JSON_EXTRACT(configs.header_procedure, '$.get_database_config')
#     INTO header_staging
#     FROM type_databases
#              JOIN config_databases ON type_databases.id = config_databases.type_database_id
#              JOIN configs ON config_databases.id = configs.config_database_id
#     WHERE type_databases.type_name = dbName
#     LIMIT 1;
#     SELECT header_staging;
# END
//

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

call get_log_crawler_config();

CREATE PROCEDURE get_log_crawler()
BEGIN
    DECLARE ex INT DEFAULT 0;
    DECLARE _config_id INT DEFAULT 0;
    DECLARE _configs JSON DEFAULT '{}';
    DECLARE _resource JSON DEFAULT '{}';
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
    SELECT configs.id,
           configs.email,
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
           resources.purpose
    FROM resources
             JOIN configs ON resources.id = configs.resource_id
    WHERE configs.id = _config_id
      AND active = 1;
END //


CREATE PROCEDURE insert_log_crawler(_config_id INT,
                                    _file_name VARCHAR(200), _error_file_name VARCHAR(200), _count_row INT,
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
    ELSE
        INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                          create_at)
        VALUES (_config_id, NOW(), NULL, ' ', ' ', 0, _status, NOW());
    END IF;
END
//

CREATE PROCEDURE get_dump_staging(_path VARCHAR(200))
BEGIN
    DECLARE _name VARCHAR(200);
    DECLARE _host VARCHAR(200);
    DECLARE _password VARCHAR(200);
    DECLARE _port INT;
    DECLARE _username VARCHAR(200); DECLARE _command VARCHAR(500);
    SELECT name, host, password, port, username
    INTO _name,_host,_password,_port,_username
    FROM config_databases
    WHERE name = 'estate_staging';

    SET _command = CONCAT(
            'mysqldump -h ', _host,
            ' -P ', _port,
            ' -u ', _username,
            ' -p ', _password,
            ' ', _name,
            ' estate_daily > ', _path, '/estate_staging.sql'
                   );

    -- Display the command (for debugging/logging purposes)
    SELECT _command;

    -- Note: MySQL cannot directly execute system commands from within a procedure,
    -- so you would need to run the generated command externally.

END //

call get_dump_staging('/home/duy/Downloads');

call get_log_crawler();