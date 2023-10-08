import bz2
import datetime
import multiprocessing
import os
import re
from multiprocessing import Queue
from pathlib import Path

import httpx
import pygrib
from bs4 import BeautifulSoup

from src.grib2_to_wgf4_converter import Grib2ToWGF4Converter
from src.grib_message import GribMessagePyGrib
from src.utils import gather_with_concurrency


class FailedToDownloadFile(Exception):
    pass


class OpenDataParser:
    def __init__(
            self,
            data_page: str = "https://opendata.dwd.de/weather/nwp/icon-d2/grib/00/tot_prec/",

    ):
        self.data_page = data_page
        self.httpx_client = httpx.AsyncClient()
        self.main_dir = Path(data_page.split('/')[-2])
        self.max_simultaneously_loading_files = 2
        self.converter_queue = Queue()
        self.loaded_files = set()
        self.convert_started = set()
        self.num_converter_workers = 12
        self.converter_workers = []

    @staticmethod
    def datetime_from_file_name(grib_filename: str) -> tuple[datetime.datetime, datetime.timedelta]:
        pattern = r'icon-d2_germany_regular-lat-lon_single-level_(\d{10})_(\d{3})_2d_tot_prec\.grib2\.bz2'

        match = re.search(pattern, grib_filename)
        datetime_str = match.group(1)
        timedelta_str = match.group(2)
        date_time = datetime.datetime.strptime(datetime_str, "%Y%m%d%H%M")
        time_delta = datetime.timedelta(hours=int(timedelta_str))

        return date_time, time_delta

    def dir_name_from_file_name(self, grib_filename: str):
        file_date, file_time_delta = self.datetime_from_file_name(grib_filename)
        name = file_date.strftime("%d.%m.%Y")
        name += "_{:02}:{:02}".format(file_time_delta.days * 24 + file_time_delta.seconds // 3600, (file_time_delta.seconds // 60) % 60)
        total_seconds = (file_date + file_time_delta).timestamp()
        name += f"_{int(total_seconds)}"
        return name

    async def download_grib(self, link: str):
        dir_name = self.dir_name_from_file_name(link)
        os.mkdir(self.main_dir / dir_name)
        resp = await self.httpx_client.get(self.data_page + link)
        if resp.status_code == 200:
            with open(self.main_dir / dir_name / "grib", 'wb') as f:
                f.write(bz2.decompress(resp.content))
        else:
            raise FailedToDownloadFile
        self.loaded_files.add(dir_name)
        self.manage_converts()

    def get_prev_file(self, time_d: str) -> str | None:
        for loaded in self.loaded_files:
            if loaded[11:13] == time_d:
                return loaded
        return None

    def manage_converts(self):
        # Смотрит какие файлы готовы к конвертации и запускает если надо
        for loaded_file in self.loaded_files:
            time_delta = loaded_file[11:13]
            # Первый файл не процессим
            if time_delta == '00':
                continue
            time_delta = int(time_delta) - 1
            # Если предыдущий не скачали не процессим
            prev_file = self.get_prev_file(f'{time_delta:02}')
            if not prev_file:
                continue

            if loaded_file not in self.convert_started:
                self.converter_queue.put((prev_file, loaded_file,))

    def start_workers(self):
        for _ in range(self.num_converter_workers):
            worker_process = multiprocessing.Process(target=self.converter_worker)
            worker_process.start()
            self.converter_workers.append(worker_process)

    def stop_workers(self):
        for _ in range(self.num_converter_workers):
            self.converter_queue.put(None)  # Signal to workers to exit

        for worker_process in self.converter_workers:
            worker_process.join()

    async def start_load_tasks(self):
        try:
            os.mkdir(self.main_dir)
        except FileExistsError:
            pass
        response = await self.httpx_client.get(self.data_page)
        soup = BeautifulSoup(response.text, 'html.parser')
        regular_lat_lon_links = [link['href'] for link in soup.find_all('a') if 'regular-lat-lon' in link['href']]
        tasks = []
        for link in regular_lat_lon_links:
            tasks.append(self.download_grib(link))
        await gather_with_concurrency(self.max_simultaneously_loading_files, *tasks)

    def remove_gribs(self):
        for loaded in self.loaded_files:
            os.remove(self.main_dir / loaded / "grib")

    async def run(self):
        self.start_workers()
        try:
            await self.start_load_tasks()
        except Exception:
            pass
        finally:
            self.stop_workers()

    def converter_worker(self):
        while True:
            message = self.converter_queue.get()
            if message is None:
                break
            file_start, file_end = message
            pgrb_start = pygrib.open(str(self.main_dir) + "/" + file_start + "/grib")
            pgrb_end = pygrib.open(str(self.main_dir) + "/" + file_end + "/grib")
            mes1 = GribMessagePyGrib(pgrb_start[1])
            mes2 = GribMessagePyGrib( pgrb_end[1])
            converter = Grib2ToWGF4Converter(mes1, mes2, output_filename=self.main_dir/file_end/"PRATE.wgf4")
            converter.to_wgf4()

            pgrb_start.close()
            pgrb_end.close()
