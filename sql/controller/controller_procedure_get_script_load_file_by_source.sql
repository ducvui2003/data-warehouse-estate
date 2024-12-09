DROP PROCEDURE IF EXISTS get_script_load_file_by_source;
DELIMITER //
CREATE PROCEDURE get_script_load_file_by_source(
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
END //

DELIMITER ;


CALL get_script_load_file_by_source("batdongsan.com.vn", "D:/university/data_warehouse/sql/batdongsan_com_vn_data.csv");

# Dòng này load file vào bảng estate_daily_temp_batdongsan_com_vn
truncate estate_daily_temp_batdongsan_com_vn;
SET GLOBAL local_infile = TRUE;
LOAD DATA LOCAL INFILE 'D:/university/data_warehouse/sql/data/batdongsan_com_vn_data.csv' INTO TABLE estate_daily_temp_batdongsan_com_vn FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '
' IGNORE 1 LINES (nk, url, area, email, legal, price, floors, images, address, bedroom, subject, bathroom, end_date,
                  create_at, full_name, start_date, description, orientation);
SET GLOBAL local_infile = FALSE;

CALL get_script_load_file_by_source("muaban_net", "D:/university/data_warehouse/sql/muaban_net_data.csv");

# Dòng này load file vào bảng estate_daily_temp_muaban_net
truncate estate_daily_temp_muaban_net;
SET GLOBAL local_infile = TRUE;
LOAD DATA LOCAL INFILE 'D:/university/data_warehouse/sql/muaban_net_data.csv' INTO TABLE estate_daily_temp_muaban_net FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '
' IGNORE 1 LINES (nk, url, area, legal, phone, price, floors, images, address, bedroom, subject, bathroom, create_at,
                  full_name, description, orientation);
SET GLOBAL local_infile = FALSE;

