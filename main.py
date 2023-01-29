import asyncio
import time
import aiohttp
import pandas as pd
from PIL import Image
from io import BytesIO

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

from constants import INPUT_SHEET_URL, OUTPUT_SHEET_URL, CREDENTIALS_PATH


def save_to_spreadsheet(df, path_to_file, spreadsheet_url):
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = Credentials.from_service_account_file(path_to_file, scopes=scopes)
    gc = gspread.authorize(credentials)

    # open a google sheet
    gs = gc.open_by_url(spreadsheet_url)

    # select a work sheet from its name
    worksheet = gs.worksheet('feed')

    # write to dataframe
    worksheet.clear()
    set_with_dataframe(worksheet=worksheet, dataframe=df, include_index=False,
                       include_column_header=True, resize=True)


# function to get image size
async def scrape(url, session):
    async with session.get(url) as resp:
        if resp.status == 200:
            body = await resp.read()
            img = Image.open(BytesIO(body))
            respond = img.size
        else:
            respond = "Sorry, this lik is invalid"
        return respond


async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        return await scrape(url, session)


async def main():
    # exporting sheet in csv format
    csv_url = INPUT_SHEET_URL.replace('/edit#gid=', '/export?format=csv&gid=')
    df = pd.read_csv(csv_url)

    # dropping empty fields
    df.dropna(how='all', inplace=True)

    # removing size from link, to get true size of the picture
    links = [link.removesuffix('size=feed-1080') for link in df['image_url']]
    tasks = []
    # setting semaphore
    sem = asyncio.Semaphore(50)

    # creating tasks
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=200)) as session:
        for image_url in links:
            task = asyncio.create_task(bound_fetch(sem, image_url, session))
            tasks.append(task)
        # processing tasks
        print('Processing tasks')
        result = await asyncio.gather(*tasks)
    df['SIZE'] = result
    print('Saving the output of extracted information')
    save_to_spreadsheet(df, CREDENTIALS_PATH, OUTPUT_SHEET_URL)



if __name__ == "__main__":
    start_time = time.time()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    time_difference = time.time() - start_time
    print(f'Scraping time: %.2f seconds.' % time_difference)
