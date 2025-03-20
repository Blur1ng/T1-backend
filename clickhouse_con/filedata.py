import os
from ch_classes import DataChange
import concurrent.futures


class FileData:
    def __init__(self):
        self.path = os.path.dirname(os.path.abspath(__file__))+"/server/"

    def get_data_from_dir(self) -> dict[str:list[dict]]:
        all_data = {}
        tasks = []
        for dir_name in os.listdir(path=self.path):
            if dir_name == ".DS_Store":
                continue

            next_dir = os.listdir(path=self.path+dir_name)
            if "CHECK" not in next_dir:
                continue

            for file in next_dir:
                if file == "CHECK":
                    continue
                table_name = file[:-4].replace(".", "_")
                tasks.append((dir_name, file, table_name))

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            future_start = {executor.submit(self._get_data_from_next_dir, dir_name, file): table_name for dir_name, file, table_name in tasks}#запуск всех потоков
            for future_now in concurrent.futures.as_completed(future_start): #ждут пока каждый поток закончится
                table_name = future_start[future_now]
                all_data[table_name] = future_now.result()

        return all_data
    def _get_data_from_next_dir(self, dir_name: str, file: str):
        data = []
        with open(self.path+dir_name+"/"+file, "r") as f:
            for line in f:
                data.append(eval(line))
        change_data = DataChange(data).get_change_data()
        return change_data

