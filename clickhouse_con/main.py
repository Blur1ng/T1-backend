from ch_con import ClickHouse_con
from ch_set import ClickHouseTable
from filedata import FileData
from settings import ORDER_BY

def main():
    all_data = FileData().get_data_from_dir() # { file_name:list[ dict ] }
    ch_con = ClickHouse_con()
    ch_con.connect()
    cursor = ch_con.cursor
    try:
        for file_name, columns in all_data.items():
            table_name_ = file_name
            ch = ClickHouseTable(table_name_, cursor)
            ch.create_table(columns[0], ORDER_BY)
            ch.insert_data(columns)
    finally:
        cursor.close()


if __name__ == "__main__":
    main()

