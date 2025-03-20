from pathlib import Path
from ch_classes import DataChange
import concurrent.futures
import ast


class FileData:
    def __init__(self):
        self.path = Path(__file__).parent / "server"

    def get_data_from_dir(self) -> dict[str, list[dict]]:
        all_data = {}
        tasks = []
        
        for dir_path in self.path.iterdir():
            if not dir_path.is_dir() or dir_path.name == ".DS_Store":
                continue

            next_dir_files = list(dir_path.iterdir())
            next_dir_names = [f.name for f in next_dir_files]
            
            if "CHECK" not in next_dir_names:
                continue

            for file_path in next_dir_files:
                if file_path.name == "CHECK" or not file_path.is_file():
                    continue
                    
                table_name = file_path.stem.replace(".", "_")
                tasks.append((file_path, table_name))

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            future_start = {executor.submit(self._get_data_from_next_dir, file_path): table_name for file_path, table_name in tasks}
            
            for future_now in concurrent.futures.as_completed(future_start):
                table_name = future_start[future_now]
                all_data[table_name] = future_now.result()

        return all_data
        
    def _get_data_from_next_dir(self, file_path: Path):
        data = []
        try:
            with file_path.open("r") as f:
                for line in f:
                    data.append(ast.literal_eval(line))
                        
            change_data = DataChange(data).get_change_data()
            return change_data
        except Exception as e:
            print(f"Не удалось получить данные из {file_path}")
            raise