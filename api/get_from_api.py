from clickhouse_con.ch_classes import DataChange
from clickhouse_con.save_data import SaveData
from api.models.data_model import post_more_data



async def get_api_data(enter_data: post_more_data):
    change_data = await DataChange(enter_data.data).get_change_data()
    await SaveData({enter_data.table_name.strftime("%Y-%m-%d %H").replace("-","_").replace(" ","__").replace(":", "_"): 
                  change_data}).create_and_save()