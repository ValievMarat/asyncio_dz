import asyncio
import aiohttp
from more_itertools import chunked
from models import engine, Session, Base, SwapiPeople

MAX_SIZE = 10


async def paste_to_db(results):
    swapi_people = [SwapiPeople(**item) for item in results]
    async with Session() as session:
        session.add_all(swapi_people)
        await session.commit()

async def get_count_people(session):
    async with session.get(f'https://swapi.dev/api/people') as response:
        json_data = await response.json()
        return json_data.get('count')


async def get_people(session, people_id):
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data_people = await response.json()

    dict_people = {'id': people_id,
                   'birth_year': json_data_people.get('birth_year', ''),
                   'eye_color': json_data_people.get('eye_color', ''),
                   'gender': json_data_people.get('gender', ''),
                   'hair_color': json_data_people.get('hair_color', ''),
                   'height': json_data_people.get('height', ''),
                   'mass': json_data_people.get('mass', ''),
                   'name': json_data_people.get('name', ''),
                   'skin_color': json_data_people.get('skin_color', ''),
                   'films': '',
                   'species': '',
                   'starships': '',
                   'vehicles': ''}

    # Получаем название планеты
    homeworld = json_data_people.get('homeworld', '')
    if homeworld != '':
        async with session.get(homeworld) as response:
            json_data_homeworld = await response.json()
            homeworld = json_data_homeworld.get('name')
    dict_people['homeworld'] = homeworld

    # Получение данных в строку по некоторым свойствам. Для этого в словарь запишем
    # имя свойства и тэг, из которого получать название
    dict_properties = {'films': 'title',
                       'species': 'name',
                       'starships': 'name',
                       'vehicles': 'name'}

    for key, value in dict_properties.items():
        href_list = json_data_people.get(key)
        if href_list is not None:
            value_list = []
            for item_href in href_list:
                async with session.get(item_href) as response:
                    json_data = await response.json()
                    value_list.append(json_data.get(value))
            dict_people[key] = ','.join(value_list)

    return dict_people


async def fill_base():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session = aiohttp.ClientSession()
    count_people = await get_count_people(session)
    coros = (get_people(session, i) for i in range(1, count_people))

    for coros_chunk in chunked(coros, MAX_SIZE):
        results = await asyncio.gather(*coros_chunk)
        asyncio.create_task(paste_to_db(results))
        print(results)

    await session.close()
    set_tasks = asyncio.all_tasks()
    for task in set_tasks:
        if task != asyncio.current_task():
            await task


if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(fill_base())

