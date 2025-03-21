from pathlib import Path
from clickhouse_con.ch_classes import DataChange
import concurrent.futures
import ast
from clickhouse_con.settings import ch_settings
import asyncio
import logging
logger = logging.getLogger(__name__)


class FileData:
    def __init__(self):
        self.path = Path(__file__).parent / ch_settings.MAINDIR
    
    async def get_data_from_dir(self) -> dict:
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
                tasks.append((self._process_file(file_path), table_name))

        # Ждем завершения всех задач
        if tasks:
            coroutines, table_names = zip(*tasks)
            results = await asyncio.gather(*coroutines)

            for table_name, result in zip(table_names, results):
                all_data[table_name] = result

        return all_data
    
    async def _process_file(self, file_path: Path,):
        data = await self._read_file(file_path)
        
        change_data = await DataChange(data).get_change_data()
        return change_data
    
    async def _read_file(self, file_path: Path) -> list[dict]:
        data = []
        content = await asyncio.to_thread(self._read_file_sync, file_path)
        
        for line in content.splitlines():
            parsed_line = await asyncio.to_thread(ast.literal_eval, line)
            data.append(parsed_line)
                
        return data
    
    def _read_file_sync(self, file_path: Path) -> str:
        with file_path.open("r") as f:
            return f.read()
        