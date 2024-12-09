use estate_staging;


-- Load file to table estate_daily_temp - estate_staging database
truncate estate_daily_temp_batdongsan_com_vn;

# Enable local_infile = True để có thể load data từ file vào staging
SHOW VARIABLES LIKE 'local_infile'; -- check load local infile
SET GLOBAL local_infile = TRUE;
LOAD DATA LOCAL INFILE 'C:/Users/ADMIN/Downloads/data_batdongsan_com_vn.csv '
    INTO TABLE estate_daily_temp_batdongsan_com_vn
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
-- subject csv => subject table,...
    (nk, subject, address, description, area, price, orientation, bathroom, floors, bedroom, legal, images, email, full_name, start_date, end_date, create_at, url);

# Disable local_infile = True để có thể load data từ file vào staging
SET GLOBAL local_infile = FALSE;

SELECT *
from estate_daily_temp_batdongsan_com_vn


