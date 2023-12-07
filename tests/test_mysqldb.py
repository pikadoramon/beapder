from beapder.db.mysqldb import MysqlDB

db = MysqlDB(
    ip="localhost", port=3306, db="beapder", user_name="beapder", user_pass="beapder123"
)

MysqlDB.from_url("mysql://beapder:beapder123@localhost:3306/beapder?charset=utf8mb4")
