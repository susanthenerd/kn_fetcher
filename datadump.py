import aiohttp
import asyncio
import json
from tqdm.asyncio import tqdm
import backoff
import os
import signal
import logging
from aiofiles import open as aio_open

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Config:
    LIMIT = 50
    URL_BASE = "https://rcpc.kilonova.ro/api/submissions/get"
    FILE_NAME = 'data_dump_rcpc.json'
    CHECKPOINT_FILE = 'checkpoint_rcpc.txt'
    TIMEOUT = 10
    MAX_TRIES = 10
    MAX_TIME = 60
    CHUNK_SIZE = 1000


class GracefulShutdown:
    def __init__(self):
        self.is_shutting_down = False
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        logging.info("Received shutdown signal. Exiting gracefully...")
        self.is_shutting_down = True

    def should_shut_down(self):
        return self.is_shutting_down


class CheckpointManager:
    def __init__(self, file_name, checkpoint_file):
        self.file_name = file_name
        self.checkpoint_file = checkpoint_file

    async def save(self, offset, all_submissions, unique_users, problems_details):
        async with aio_open(self.file_name, 'w', encoding='utf-8') as f:
            await f.write(json.dumps({
                "submissions": all_submissions,
                "users": unique_users,
                "problems": problems_details
            }, ensure_ascii=False, indent=4))
        async with aio_open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            await f.write(str(offset))

    def load(self):
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                return int(f.read().strip())
        return 0


class DataAggregator:
    def __init__(self, config, checkpoint_manager, shutdown_handler):
        self.config = config
        self.checkpoint_manager = checkpoint_manager
        self.shutdown_handler = shutdown_handler
        self.progress_bar = None

    async def fetch_with_retry(self, session, url):
        @backoff.on_exception(backoff.expo,
                              aiohttp.ClientError,
                              max_tries=self.config.MAX_TRIES,
                              max_time=self.config.MAX_TIME,
                              on_backoff=lambda details: logging.warning(
                                  "Backing off {wait:0.1f} seconds after {tries} tries calling function with args {"
                                  "args} and kwargs {kwargs}".format(
                                      **details)),
                              on_giveup=lambda e: logging.error(f"Operation failed with error: {e}"))
        async def _fetch():
            async with session.get(url, timeout=self.config.TIMEOUT) as response:
                response.raise_for_status()
                return await response.json()

        return await _fetch()

    async def aggregate_data(self, session, url_base, contest_id=None, problem_id=None):
        offset = self.checkpoint_manager.load()
        all_submissions, unique_users, problems_details = [], {}, {}

        if offset > 0 and os.path.exists(self.config.FILE_NAME):
            async with aio_open(self.config.FILE_NAME, 'r', encoding='utf-8') as f:
                data = json.loads(await f.read())
                all_submissions = data.get('submissions', [])
                unique_users = data.get('users', {})
                problems_details = data.get('problems', {})

        url_params = f"ascending=false&limit={self.config.LIMIT}&ordering=id"
        if contest_id:
            url_params += f"&contest_id={contest_id}"
        if problem_id:
            url_params += f"&problem_id={problem_id}"

        self.progress_bar = tqdm(total=None, desc="Retrieving data", initial=offset)
        while not self.shutdown_handler.should_shut_down():
            url = f"{url_base}?{url_params}&offset={offset}"
            try:
                data = await self.fetch_with_retry(session, url)

                if not data["data"]["submissions"]:
                    break

                all_submissions.extend(data["data"]["submissions"])
                unique_users.update(data["data"]["users"])
                problems_details.update(data["data"]["problems"])
                self.progress_bar.update(len(data["data"]["submissions"]))
                offset += self.config.LIMIT

                # Save in chunks
                if len(all_submissions) % self.config.CHUNK_SIZE == 0:
                    await self.checkpoint_manager.save(offset, all_submissions, unique_users, problems_details)
            except Exception as e:
                logging.exception(f"An error occurred: {e}")
                break

        self.progress_bar.close()
        if self.shutdown_handler.should_shut_down():
            await self.checkpoint_manager.save(offset, all_submissions, unique_users, problems_details)

    async def run(self):
        async with aiohttp.ClientSession() as session:
            await self.aggregate_data(session, self.config.URL_BASE)


async def main():
    config = Config()
    checkpoint_manager = CheckpointManager(config.FILE_NAME, config.CHECKPOINT_FILE)
    shutdown_handler = GracefulShutdown()
    aggregator = DataAggregator(config, checkpoint_manager, shutdown_handler)

    await aggregator.run()


if __name__ == "__main__":
    asyncio.run(main())
