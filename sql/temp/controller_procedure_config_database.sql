use estate_controller;
DELIMITER //
# 1.get_database_config
CREATE PROCEDURE get_database_config(IN dbName varchar(200))
BEGIN
#     3.trả về toàn bộ thông tin của config_databases đó
    SELECT config_databases.*
#         1.Thực hiện liên kết bảng type_databases và config_databases
    FROM type_databases
             JOIN config_databases
                  ON type_databases.id = config_databases.type_database_id
#         2.thực hiện tìm kiếm loại database phù hợp với tham số truyền vào
    WHERE type_databases.type_name = dbName;
END //
DELIMITER ;