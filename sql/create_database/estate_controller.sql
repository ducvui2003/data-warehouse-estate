-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Server version:               8.0.40 - MySQL Community Server - GPL
-- Server OS:                    Linux
-- HeidiSQL Version:             12.8.0.6908
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- Dumping database structure for estate_controller
CREATE DATABASE IF NOT EXISTS `estate_controller` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `estate_controller`;

-- Dumping structure for table estate_controller.configs
CREATE TABLE IF NOT EXISTS `configs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `prefix` varchar(100) DEFAULT NULL,
  `file_format` varchar(100) DEFAULT NULL,
  `file_extension` varchar(100) DEFAULT NULL,
  `data_dir_path` varchar(255) DEFAULT NULL,
  `error_dir_path` varchar(200) DEFAULT NULL,
  `active` bit(1) DEFAULT b'0',
  `config_database_id` bigint DEFAULT NULL,
  `resource_id` bigint DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `configs_config_databases_id_fk` (`config_database_id`),
  KEY `resource_id` (`resource_id`),
  CONSTRAINT `configs_config_databases_id_fk` FOREIGN KEY (`config_database_id`) REFERENCES `config_databases` (`id`),
  CONSTRAINT `configs_ibfk_1` FOREIGN KEY (`resource_id`) REFERENCES `resources` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Dumping data for table estate_controller.configs: ~2 rows (approximately)
INSERT INTO `configs` (`id`, `prefix`, `file_format`, `file_extension`, `data_dir_path`, `error_dir_path`, `active`, `config_database_id`, `resource_id`, `email`) VALUES
	(1, 'source_1_', '%Y_%m_%d__%H_%M', 'csv', 'D:\\university\\data_warehouse\\host\\data', 'D:\\university\\data_warehouse\\host\\error', b'1', 1, 1, '21130320@st.hcmuef.edu.vn'),
	(2, 'source_2_', '%Y_%m_%d__%H_%M', 'csv', 'D:\\university\\data_warehouse\\host\\data', 'D:\\university\\data_warehouse\\host\\error', b'1', 1, 2, '21130320@st.hcmuef.edu.vn');

-- Dumping structure for table estate_controller.config_databases
CREATE TABLE IF NOT EXISTS `config_databases` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `type_database_id` bigint DEFAULT NULL,
  `host` varchar(100) DEFAULT NULL,
  `port` int DEFAULT NULL,
  `username` varchar(100) DEFAULT NULL,
  `password` varchar(100) DEFAULT NULL,
  `rdbms_type` enum('MYSQL','MONGODB','POSTGRESQL') DEFAULT NULL,
  `add_ons` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `config_databases_type_databases_id_fk` (`type_database_id`),
  CONSTRAINT `config_databases_type_databases_id_fk` FOREIGN KEY (`type_database_id`) REFERENCES `type_databases` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Dumping data for table estate_controller.config_databases: ~2 rows (approximately)
INSERT INTO `config_databases` (`id`, `name`, `type_database_id`, `host`, `port`, `username`, `password`, `rdbms_type`, `add_ons`) VALUES
	(1, 'estate_staging', 1, '127.0.0.1', 3306, 'estate_root', '1234', 'MYSQL', NULL),
	(2, 'estate_warehouse', 2, '127.0.0.1', 3306, 'estate_root', '1234', 'MYSQL', NULL);

-- Dumping structure for procedure estate_controller.get_database_config
DELIMITER //
CREATE PROCEDURE `get_database_config`(IN dbName varchar(200))
BEGIN
#     3.trả về toàn bộ thông tin của config_databases đó
    SELECT config_databases.*
#         1.Thực hiện liên kết bảng type_databases và config_databases
    FROM type_databases
             JOIN config_databases
                  ON type_databases.id = config_databases.type_database_id
#         2.thực hiện tìm kiếm loại database phù hợp với tham số truyền vào
    WHERE type_databases.type_name = dbName;
END//
DELIMITER ;

-- Dumping structure for procedure estate_controller.get_log_crawler
DELIMITER //
CREATE PROCEDURE `get_log_crawler`()
BEGIN
    #     1.1 tạo biến _id và _config_id để lưu lại log id và config id của log đó
    DECLARE _id INT DEFAULT 0;
    DECLARE _config_id INT DEFAULT 0;
#     1.2 thực hiện kiểm tra trong log có tồn tại ít nhất 1 dòng là FILE_PROCESSING
    IF !EXISTS(SELECT 'FILE_PROCESSING'
               FROM logs
               WHERE status = 'FILE_PROCESSING'
                 AND is_delete = 0
               LIMIT 1) THEN
        SELECT logs.id, logs.config_id
        #         1.4.1 không thay đổi giá trị 2 biến
#        1.4.2 thực hiện việc gán giá trị log.id và log.config_id vào biến _id, _config_id
        INTO _id, _config_id
#         1.3 thực hiện join bảng logs và bảng config
        FROM logs
                 join configs on logs.config_id = configs.id
#         1.4 tìm kiếm 1 dòng log có status là FILE_PENDING hay FILE_ERROR chưa bị xóa của ngày hôm đó và của thông tin còn hoạt động
        WHERE (logs.status = 'FILE_PENDING' OR logs.status = 'FILE_ERROR')
          AND is_delete = 0
          AND DATE(logs.create_at) = CURRENT_DATE
          And configs.active = 1
        #     GROUP BY logs.id, logs.config_id
        ORDER BY CASE
                     WHEN logs.status = 'FILE_PENDING' THEN 1
                     WHEN logs.status = 'FILE_ERROR' THEN 2
                     END
        LIMIT 1;
#         1.5 Kiểm tra nếu config_id lấy được khác không(tức tìm được dòng có trạng thái hợp lệ)
        IF _config_id != 0 THEN
#             1.6 thiết lập trạng thái bị xóa cho dòng log đó
            UPDATE logs
            SET logs.is_delete = 1
            WHERE logs.id = _id;
            #               AND (logs.status = 'FILE_PENDING' OR logs.status = 'FILE_ERROR')
#               AND DATE(logs.create_at) = CURRENT_DATE;
# 1.7 thêm 1 dòng log mới có config_id, thời gian bắt đầu và thời giang tạo ngay lúc thêm và có trạng thái "FILE_PROCESSING"
            INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                              create_at)
            VALUES (_config_id, NOW(), NULL, ' ', ' ', 0, 'FILE_PROCESSING', NOW());
        END IF;
#         1.8 Trả về các thông tin cần thiết cho việc crawl của _config_id lấy được
        SELECT configs.id,
               configs.email,
               configs.data_dir_path,
               configs.error_dir_path,
               configs.file_extension,
               configs.file_format,
               configs.prefix,
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
END//
DELIMITER ;

-- Dumping structure for procedure estate_controller.get_log_staging
DELIMITER //
CREATE PROCEDURE `get_log_staging`()
BEGIN
#     1.1 tạo biến _id và _config_id để lưu lại log id và config id của log đó
    DECLARE _id INT DEFAULT 0;
DECLARE _config_id INT DEFAULT 0;
#     1.2 thực hiện kiểm tra trong log có tồn tại ít nhất 1 dòng là STAGING_PROCESSING
IF !EXISTS(SELECT 'STAGING_PROCESSING'
               FROM logs
               WHERE status = 'STAGING_PROCESSING'
                 AND is_delete = 0
               LIMIT 1) THEN
SELECT logs.id, logs.config_id
#         1.4.1 không thay đổi giá trị 2 biến
#        1.4.2 thực hiện việc gán giá trị log.id và log.config_id vào biến _id, _config_id
INTO _id, _config_id
#         1.3 thực hiện join bảng logs và bảng config
FROM logs
         join configs on logs.config_id = configs.id
#         1.4 tìm kiếm 1 dòng log có status là STAGING_PENDING hay STAGING_ERROR chưa bị xóa của ngày hôm đó và của thông tin còn hoạt động
WHERE (logs.status = 'STAGING_PENDING' OR logs.status = 'STAGING_ERROR')
  AND is_delete = 0
  AND DATE(logs.create_at) = CURRENT_DATE
  And configs.active = 1
#     GROUP BY logs.id, logs.config_id
ORDER BY
    CASE
             WHEN logs.status = 'STAGING_PENDING' THEN 1
             WHEN logs.status = 'STAGING_ERROR' THEN 2
    END,
         logs.create_at ASC
LIMIT 1;
#         1.5 Kiểm tra nếu config_id lấy được khác không(tức tìm được dòng có trạng thái hợp lệ)
IF _config_id != 0 THEN
#             1.6 thiết lập trạng thái bị xóa cho dòng log đó
UPDATE logs
SET logs.is_delete = 1
WHERE logs.id = _id;
#               AND (logs.status = 'FILE_PENDING' OR logs.status = 'STAGING_ERROR')
#               AND DATE(logs.create_at) = CURRENT_DATE;
# 1.7 thêm 1 dòng log mới có config_id, thời gian bắt đầu và thời giang tạo ngay lúc thêm và có trạng thái "STAGING_PROCESSING"
INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                  create_at)
VALUES (_config_id, NOW(), NULL, '', ' ', 0, 'STAGING_PROCESSING', NOW());
END IF;
#         1.8 Trả về các thông tin cần thiết cho việc load của _config_id lấy được
SELECT configs.id,
       configs.email,
#        he dieu hanh khac thi sua
       CONCAT( configs.data_dir_path,'/',logs.file_name) as file_path,
       resources.id as resource_id,
       resources.name,
       configs.error_dir_path,
       configs.file_format,
       configs.prefix
FROM resources
         JOIN configs ON resources.id = configs.resource_id
         JOIN logs ON logs.config_id = configs.id

WHERE configs.id = _config_id
  AND logs.id = _id
  AND active = 1;
END IF;
END//
DELIMITER ;

-- Dumping structure for procedure estate_controller.get_log_to_loadfile
DELIMITER //
CREATE PROCEDURE `get_log_to_loadfile`()
BEGIN
    SELECT *
    FROM logs
    INNER JOIN configs ON logs.config_id = configs.id
    WHERE DATE(logs.create_at) = CURRENT_DATE
      AND (logs.status = 'STAGING_PENDING' OR logs.status = 'STAGING_ERROR') AND logs.is_delete = 0
    ORDER BY logs.create_at DESC
    LIMIT 1;
END//
DELIMITER ;

-- Dumping structure for procedure estate_controller.get_log_transform
DELIMITER //
CREATE PROCEDURE `get_log_transform`()
BEGIN
    # 1.1 tạo biến _id và _config_id để lưu lại log id và config id của log đó
    DECLARE _id INT DEFAULT 0;
    DECLARE _config_id INT DEFAULT 0;
    # 1.2 thực hiện kiểm tra trong log có tồn tại ít nhất 1 dòng là TRANSFORM_PROCESSING
    IF !EXISTS(SELECT 'TRANSFORM_PROCESSING'
               FROM logs
               WHERE status = 'TRANSFORM_PROCESSING'
                 AND is_delete = 0
               LIMIT 1) THEN
        # Không tồn tại dòng nào có status là TRANSFORM_PROCESSING
        SELECT logs.id, logs.config_id
        # 1.4.1 không thay đổi giá trị 2 biến
        # 1.4.2 thực hiện việc gán giá trị log.id và log.config_id vào biến _id, _config_id
        INTO _id, _config_id
        # 1.3 thực hiện join bảng logs và bảng config
        FROM logs
                 join configs on logs.config_id = configs.id
        # 1.4 tìm kiếm 1 dòng log có status là TRANSFORM_PENDING hay TRANSFORM_ERROR chưa bị xóa của ngày hôm đó và của thông tin còn hoạt động
        WHERE (logs.status = 'TRANSFORM_PENDING' OR logs.status = 'TRANSFORM_ERROR')
          AND is_delete = 0
          AND DATE(logs.create_at) = CURRENT_DATE
          And configs.active = 1
        #  Sắp xếp ưu tiên dòng có status là TRANSFORM_PENDING trước
        ORDER BY CASE
                     WHEN logs.status = 'TRANSFORM_PENDING' THEN 1
                     WHEN logs.status = 'TRANSFORM_ERROR' THEN 2
                     END
        LIMIT 1;
        # 1.5 Kiểm tra nếu config_id lấy được khác không(tức tìm được dòng có trạng thái hợp lệ)
        IF _config_id != 0 THEN
            # 1.6 thiết lập trạng thái bị xóa cho dòng log đó
            UPDATE logs
            SET logs.is_delete = 1
            WHERE logs.id = _id;
            # 1.7 thêm 1 dòng log mới có config_id, thời gian bắt đầu và thời giang tạo ngay lúc thêm và có trạng thái "TRANSFORM_PROCESSING"
            INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                              create_at)
            VALUES (_config_id, NOW(), NULL, ' ', ' ', 0, 'TRANSFORM_PROCESSING', NOW());
        END IF;
        # 1.8 Trả về các thông tin cần thiết cho việc crawl của _config_id lấy được
        SELECT configs.id,
               configs.error_dir_path,
               configs.file_format,
               configs.prefix,
               resources.name as source_name
        FROM resources
                 JOIN configs ON resources.id = configs.resource_id
                 JOIN logs ON logs.config_id = configs.id
        WHERE configs.id = _config_id
          AND logs.id = _id
          AND active = 1;
    END IF;
END//
DELIMITER ;

-- Dumping structure for procedure estate_controller.get_log_warehouse
DELIMITER //
CREATE PROCEDURE `get_log_warehouse`()
BEGIN
#     1.1 tạo biến _id và _config_id để lưu lại log id và config id của log đó
    DECLARE _id INT DEFAULT 0;
DECLARE _config_id INT DEFAULT 0;
#     1.2 thực hiện kiểm tra trong log có tồn tại ít nhất 1 dòng là WAREHOUSE_PROCESSING
IF !EXISTS(SELECT 'WAREHOUSE_PROCESSING'
               FROM logs
               WHERE status = 'WAREHOUSE_PROCESSING'
                 AND is_delete = 0
               LIMIT 1) THEN
SELECT logs.id, logs.config_id
#         1.4.1 không thay đổi giá trị 2 biến
#        1.4.2 thực hiện việc gán giá trị log.id và log.config_id vào biến _id, _config_id
INTO _id, _config_id
#         1.3 thực hiện join bảng logs và bảng config
FROM logs
         join configs on logs.config_id = configs.id
#         1.4 tìm kiếm 1 dòng log có status là WAREHOUSE_PENDING hay WAREHOUSE_ERROR chưa bị xóa của ngày hôm đó và của thông tin còn hoạt động
WHERE (logs.status = 'WAREHOUSE_PENDING' OR logs.status = 'WAREHOUSE_ERROR')
  AND is_delete = 0
  AND DATE(logs.create_at) = CURRENT_DATE
  And configs.active = 1
#     GROUP BY logs.id, logs.config_id
ORDER BY
    CASE
             WHEN logs.status = 'WAREHOUSE_PENDING' THEN 1
             WHEN logs.status = 'WAREHOUSE_ERROR' THEN 2
    END,
         logs.create_at ASC
LIMIT 1;
#         1.5 Kiểm tra nếu config_id lấy được khác không(tức tìm được dòng có trạng thái hợp lệ)
IF _config_id != 0 THEN
#             1.6 thiết lập trạng thái bị xóa cho dòng log đó
UPDATE logs
SET logs.is_delete = 1
WHERE logs.id = _id;
#               AND (logs.status = 'FILE_PENDING' OR logs.status = 'WAREHOUSE_ERROR')
#               AND DATE(logs.create_at) = CURRENT_DATE;
# 1.7 thêm 1 dòng log mới có config_id, thời gian bắt đầu và thời giang tạo ngay lúc thêm và có trạng thái "WAREHOUSE_PROCESSING"
INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                  create_at)
VALUES (_config_id, NOW(), NULL, '', ' ', 0, 'WAREHOUSE_PROCESSING', NOW());
END IF;
#         1.8 Trả về các thông tin cần thiết cho việc load của _config_id lấy được
SELECT configs.id,
       configs.email,
#        he dieu hanh khac thi sua
       CONCAT( configs.data_dir_path,'/',logs.file_name) as file_part,
       resources.name,
       configs.error_dir_path,
       configs.file_format,
       configs.prefix
FROM resources
         JOIN configs ON resources.id = configs.resource_id
         JOIN logs ON logs.config_id = configs.id

WHERE configs.id = _config_id
  AND logs.id = _id
  AND active = 1;
END IF;
END//
DELIMITER ;

-- Dumping structure for procedure estate_controller.get_script_load_file_by_source
DELIMITER //
CREATE PROCEDURE `get_script_load_file_by_source`(
    IN source ENUM ('muaban.net', 'batdongsan.com.vn'),
    IN file_path VARCHAR(255)
)
BEGIN
    -- Declare variables for the SQL statements
    DECLARE truncate_sql TEXT;
    DECLARE set_infile_on_sql TEXT;
    DECLARE load_file_sql TEXT;
    DECLARE set_infile_off_sql TEXT;

    -- Construct the SQL statements based on the source
    IF source = 'batdongsan.com.vn' THEN
        SET truncate_sql = 'TRUNCATE estate_daily_temp_batdongsan_com_vn;';
        SET set_infile_on_sql = 'SET GLOBAL local_infile = TRUE;';
        SET load_file_sql = CONCAT(
                'LOAD DATA LOCAL INFILE \'', file_path,
                '\' INTO TABLE estate_daily_temp_batdongsan_com_vn ',
                'FIELDS TERMINATED BY \',\' OPTIONALLY ENCLOSED BY \'"\' ',
                'LINES TERMINATED BY \'\n\' IGNORE 1 LINES ',
                '(nk, url, area, email, legal, price, floors, images, address, bedroom, subject, bathroom, end_date, ',
                'create_at, full_name, start_date, description, orientation);'
                            );
        SET set_infile_off_sql = 'SET GLOBAL local_infile = FALSE;';
    ELSEIF source = 'muaban.net' THEN
        SET truncate_sql = 'TRUNCATE estate_daily_temp_muaban_net;';
        SET set_infile_on_sql = 'SET GLOBAL local_infile = TRUE;';
        SET load_file_sql = CONCAT(
                'LOAD DATA LOCAL INFILE \'', file_path,
                '\' INTO TABLE estate_daily_temp_muaban_net ',
                'FIELDS TERMINATED BY \',\' OPTIONALLY ENCLOSED BY \'"\' ',
                'LINES TERMINATED BY \'\n\' IGNORE 1 LINES ',
                '(nk, url, area, legal, phone, price, floors, images, address, bedroom, subject, bathroom, create_at, ',
                'full_name, description, orientation);'
                            );
        SET set_infile_off_sql = 'SET GLOBAL local_infile = FALSE;';
    ELSE
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Invalid source provided. Supported sources are: "batdongsan.com.vn", "muaban.net".';
    END IF;

    -- Return the list of SQL statements as separate rows
    SELECT 'TRUNCATE' AS step, truncate_sql AS sql_statement
    UNION ALL
    SELECT 'SET_LOCAL_INFILE_ON', set_infile_on_sql
    UNION ALL
    SELECT 'LOAD_FILE', load_file_sql
    UNION ALL
    SELECT 'SET_LOCAL_INFILE_OFF', set_infile_off_sql;
END//
DELIMITER ;

-- Dumping structure for procedure estate_controller.insert_log_crawler
DELIMITER //
CREATE PROCEDURE `insert_log_crawler`(_config_id INT,
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
    ELSEIF _status = 'STAGING_PENDING' THEN
        INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                          create_at)
        VALUES (_config_id, NOW(), NULL, ' ', ' ', 0, _status, NOW());
    END IF;
END//
DELIMITER ;

-- Dumping structure for procedure estate_controller.insert_log_staging
DELIMITER //
CREATE PROCEDURE `insert_log_staging`(_config_id INT,
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
END//
DELIMITER ;

-- Dumping structure for procedure estate_controller.insert_log_transform
DELIMITER //
CREATE PROCEDURE `insert_log_transform`(_config_id INT,
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
    WHERE status = 'TRANSFORM_PROCESSING'
      AND config_id = _config_id;

    if _status = 'TRANSFORM_ERROR' THEN
        INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                          create_at)
        VALUES (_config_id, NOW(), NULL, ' ', _error_file_name, 0, _status, NOW());
    ELSEIF _status = 'TRANSFORM_PENDING' THEN
        INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status,
                          create_at)
        VALUES (_config_id, NOW(), NULL, ' ', ' ', _count_row, _status, NOW());
    END IF;
END//
DELIMITER ;

-- Dumping structure for procedure estate_controller.insert_new_log_crawler
DELIMITER //
CREATE PROCEDURE `insert_new_log_crawler`()
BEGIN
    INSERT INTO logs (config_id, time_start, time_end, file_name, error_file_name, count_row, status, create_at)
    VALUES (1, NOW(), NULL, ' ', ' ', 0, 'FILE_PENDING', NOW()),
           (2, NOW(), NULL, ' ', ' ', 0, 'FILE_PENDING', NOW());
END//
DELIMITER ;

-- Dumping structure for table estate_controller.logs
CREATE TABLE IF NOT EXISTS `logs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `config_id` bigint DEFAULT NULL,
  `status` enum('FILE_PENDING','FILE_PROCESSING','FILE_ERROR','STAGING_PENDING','STAGING_PROCESSING','STAGING_ERROR','TRANSFORM_PENDING','TRANSFORM_PROCESSING','TRANSFORM_ERROR','WAREHOUSE_PENDING','WAREHOUSE_PROCESSING','WAREHOUSE_ERROR','DATAMART_PENDING','DATAMART_PROCESSING','DATAMART_ERROR') NOT NULL,
  `time_start` datetime DEFAULT NULL,
  `time_end` datetime DEFAULT NULL,
  `file_name` varchar(200) DEFAULT NULL,
  `error_file_name` longtext,
  `count_row` int DEFAULT NULL,
  `create_at` datetime DEFAULT NULL,
  `is_delete` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `config_id_idx` (`config_id`),
  CONSTRAINT `logs_ibfk_1` FOREIGN KEY (`config_id`) REFERENCES `configs` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=118 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Dumping data for table estate_controller.logs: ~3 rows (approximately)
INSERT INTO `logs` (`id`, `config_id`, `status`, `time_start`, `time_end`, `file_name`, `error_file_name`, `count_row`, `create_at`, `is_delete`) VALUES
	(1, 1, 'FILE_PENDING', '2024-11-14 08:30:00', NULL, 'file1.csv', NULL, 100, '2024-12-07 00:00:00', 0),
	(2, 1, 'TRANSFORM_PENDING', '2024-11-21 08:30:00', NULL, NULL, NULL, 0, '2024-12-07 00:00:00', 1),
	(106, 1, 'TRANSFORM_PROCESSING', '2024-12-07 08:09:15', NULL, ' ', ' ', 0, '2024-12-07 08:09:15', 0),
	(107, 1, 'STAGING_PENDING', NULL, NULL, 'D:\\university\\data_warehouse\\sql\\data\\batdongsan_com_vn_data.csv', NULL, NULL, '2024-12-09 16:04:27', 0),
	(108, 1, 'FILE_PENDING', '2024-12-09 13:48:56', NULL, ' ', ' ', 0, '2024-12-09 13:48:56', 1),
	(109, 2, 'FILE_PENDING', '2024-12-09 13:48:56', NULL, ' ', ' ', 0, '2024-12-09 13:48:56', 1),
	(112, 1, 'FILE_PROCESSING', '2024-12-09 13:52:40', '2024-12-09 15:17:19', 'D:\\university\\data_warehouse\\host\\data\\source_1_2024_12_09__22_17.csv', NULL, 400, '2024-12-09 13:52:40', 1),
	(113, 1, 'STAGING_PENDING', '2024-12-09 15:17:19', NULL, ' ', ' ', 0, '2024-12-09 15:17:19', 1),
	(114, 2, 'FILE_PROCESSING', '2024-12-09 15:21:06', '2024-12-09 15:40:48', 'D:\\university\\data_warehouse\\host\\data\\source_2_2024_12_09__22_40.csv', NULL, 100, '2024-12-09 15:21:06', 1),
	(115, 2, 'STAGING_PENDING', '2024-12-09 15:40:48', NULL, ' ', ' ', 0, '2024-12-09 15:40:48', 1);

-- Dumping structure for table estate_controller.resources
CREATE TABLE IF NOT EXISTS `resources` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `base_url` varchar(255) DEFAULT NULL,
  `source_page` varchar(255) DEFAULT NULL,
  `paging_pattern` varchar(100) DEFAULT NULL,
  `limit_page` int DEFAULT NULL,
  `scenario` json DEFAULT NULL,
  `purpose` enum('BAN','CHO THUE') DEFAULT NULL,
  `navigate_scenario` json DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Dumping data for table estate_controller.resources: ~3 rows (approximately)
INSERT INTO `resources` (`id`, `name`, `base_url`, `source_page`, `paging_pattern`, `limit_page`, `scenario`, `purpose`, `navigate_scenario`) VALUES
	(1, 'batdongsan.com.vn', 'https://batdongsan.com.vn', 'nha-dat-ban', '/p', 20, '{"nk": {"method": "get_attribute", "quantity": 1, "selector": "//*[@id=\'product-detail-web\']", "attribute": "prid"}, "url": {"method": "url"}, "area": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'js__pr-short-info-item\')]/*[text()=\'Diện tích\']/following-sibling::*[1]"}, "email": {"method": "get_attribute", "quantity": 1, "selector": "//*[@id=\'email\']", "attribute": "data-email"}, "legal": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'re__pr-specs-content-item\')]/*[text()=\'Pháp lý\']/following-sibling::*[1]"}, "price": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'js__pr-short-info-item\')]/*[text()=\'Mức giá\']/following-sibling::*[1]"}, "avatar": {"method": "get_attribute", "quantity": 1, "selector": "//*[contains(@class, \'js__agent-contact-avatar\')]", "attribute": "src"}, "floors": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'re__pr-specs-content-item\')]/*[text()=\'Số tầng\']/following-sibling::*[1]"}, "images": {"method": "get_attribute", "quantity": null, "selector": "//*[contains(@class, \'slick-track\')]//img", "attribute": "src"}, "address": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'js__pr-address\')]"}, "bedroom": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'re__pr-specs-content-item\')]/*[text()=\'Số phòng ngủ\']/following-sibling::*[1]"}, "subject": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'pr-title\')]"}, "bathroom": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'re__pr-specs-content-item\')]/*[text()=\'Số toilet\']/following-sibling::*[1]"}, "end_date": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'js__pr-config-item\')]/*[text()=\'Ngày hết hạn\']/following-sibling::*[1]"}, "create_at": {"method": "time"}, "full_name": {"method": "get_attribute", "quantity": 1, "selector": "(//*[contains(@class, \'js_contact-name\')])[1]", "attribute": "title"}, "start_date": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'js__pr-config-item\')]/*[text()=\'Ngày đăng\']/following-sibling::*[1]"}, "description": {"method": "description", "selector": "//*[contains(@class, \'re__detail-content\')]"}, "orientation": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'re__pr-specs-content-item\')]/*[text()=\'Hướng nhà\']/following-sibling::*[1]"}}', 'BAN', '{"item": ".js__product-link-for-product-id", "list": ".js__card"}'),
	(2, 'muaban.net/bat-dong-san', 'https://muaban.net', 'ban-nha-dat-chung-cu', '?page=', 5, '{"nk": {"regex": "Mã tin:\\\\s*(\\\\d+)", "method": "text", "quantity": 1, "selector": "//*[contains(@class, \'sc-6orc5o-15 jiDXp\')]//*[@class=\'date\']"}, "url": {"method": "url"}, "area": {"method": "text", "quantity": 1, "selector": "//*[(text()=\'Diện tích đất\')]/following-sibling::*[1]"}, "legal": {"method": "text", "quantity": 1, "selector": "//*[(text()=\'Giấy tờ pháp lý\')]/following-sibling::*[1]"}, "phone": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'sc-lohvv8-15 fyGvhT\')]"}, "price": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'sc-6orc5o-15 jiDXp\')]//*[@class=\'price\']"}, "floors": {"method": "text", "quantity": 1, "selector": "//*[(text()=\'Tổng số tầng\')]/following-sibling::*[1]"}, "images": {"method": "get_attribute", "quantity": null, "selector": "//*[contains(@class, \'sc-6orc5o-3 ljaVcC\')]//img", "attribute": "src"}, "address": {"method": "description", "selector": "//*[contains(@class, \'sc-6orc5o-15 jiDXp\')]/div[contains(@class, \'address\')]"}, "bedroom": {"method": "text", "quantity": 1, "selector": "//*[(text()=\'Số phòng ngủ\')]/following-sibling::*[1]"}, "subject": {"method": "text", "quantity": 1, "selector": "//*[contains(@class, \'sc-6orc5o-15 jiDXp\')]/h1"}, "bathroom": {"method": "text", "quantity": 1, "selector": "//*[(text()=\'Số phòng vệ sinh\')]/following-sibling::*[1]"}, "create_at": {"method": "time"}, "full_name": {"method": "text", "quantity": 1, "selector": "//span[contains(@class, \'title\')]"}, "description": {"method": "description", "selector": "//*[contains(@class, \'sc-6orc5o-18 gdAVnx\')]"}, "orientation": {"method": "text", "quantity": 1, "selector": "//*[(text()=\'Hướng cửa chính\')]/following-sibling::*[1]"}}', 'BAN', '{"item": "a.title", "list": ".sc-q9qagu-4.iZrvBN"}');

-- Dumping structure for table estate_controller.type_databases
CREATE TABLE IF NOT EXISTS `type_databases` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `type_name` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Dumping data for table estate_controller.type_databases: ~2 rows (approximately)
INSERT INTO `type_databases` (`id`, `type_name`) VALUES
	(1, 'staging'),
	(2, 'warehouse');

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
