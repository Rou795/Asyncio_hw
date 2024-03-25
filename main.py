import asyncio
from datetime import datetime

import aiohttp

from models import init_db, People, Session, engine
from more_itertools import chunked

MAX_CHUNKED = 10


# функция получения информации о персонаже


async def get_person(client, person_id: int):
    response = await client.get(f"https://swapi.dev/api/people/{person_id}/")
    if 199 < response.status < 300:
        json_data = await response.json()
        #        print(person_id)
        person_data = {'id': person_id,
                       'birth_year': json_data['birth_year'],
                       'eye_color': json_data['eye_color'],
                       'gender': json_data['gender'],
                       'hair_color': json_data['hair_color'],
                       'height': json_data['height'],
                       'homeworld': await get_homeworld(client, json_data['homeworld']),
                       'mass': json_data['mass'],
                       'name': json_data['name'],
                       'skin_color': json_data['skin_color'],
                       'films_titles': (",".join(await get_films(client, json_data['films']))).rstrip(","),
                       'species_names': (",".join(await get_species(client, json_data['species']))).rstrip(","),
                       'starships_names': (",".join(await get_starships(client, json_data['starships']))).rstrip(","),
                       'vehicles_names': (",".join(await get_vehicles(client, json_data['vehicles']))).rstrip(","),
                       }
        return person_data


# функция получения названия родного мира персонажа


async def get_homeworld(client, url: str) -> list:
    response = await client.get(url)
    homeworld_data = await response.json()
    return homeworld_data['name']


# функция получения названий фильмов, в которых снимался персонаж


async def get_films(client, films_url: list) -> list:
    titles = []
    for url in films_url:
        response = await client.get(url)
        film_data = await response.json()
        titles.append(film_data['title'])
    return titles


# функция получиния информации о виде персонажа


async def get_species(client, species_url: list) -> list:
    species = []
    for url in species_url:
        response = await client.get(url)
        species_data = await response.json()
        species.append(species_data['name'])
    return species


# функция получения названий космических кораблей персонажа


async def get_starships(client, starships_url: list) -> list:
    starships = []
    for url in starships_url:
        response = await client.get(url)
        starship_data = await response.json()
        starships.append(starship_data['name'])
    return starships


# функция получения названий техники персонажа


async def get_vehicles(client, vehicles_url: list) -> list:
    vehicles = []
    for url in vehicles_url:
        response = await client.get(url)
        vehicle_data = await response.json()
        vehicles.append(vehicle_data['name'])
    return vehicles


# функция записи информации о персонаже в базу


async def insert_to_db(person_data_list: list):
    models = [People(id=person_data['id'],
                     birth_year=person_data['birth_year'],
                     eye_color=person_data['eye_color'],
                     films=person_data['films_titles'],
                     gender=person_data['gender'],
                     hair_color=person_data['hair_color'],
                     height=person_data['height'],
                     homeworld=person_data['homeworld'],
                     mass=person_data['mass'],
                     name=person_data['name'],
                     skin_color=person_data['skin_color'],
                     species=person_data['species_names'],
                     starships=person_data['starships_names'],
                     vehicles=person_data['vehicles_names'],
                     ) for person_data in person_data_list]
    async with Session() as session:
        session.add_all(models)
        await session.commit()


# функция получения информации о кол-ве персонажей у api


async def get_count(client) -> int:
    response = await client.get('https://swapi.dev/api/people')
    count = await response.json()
    return int((count['count']))


async def main():
    await init_db()
    client = aiohttp.ClientSession()
    count = await get_count(client)
    tasks = []
    for chunk in chunked(range(1, count), MAX_CHUNKED):
        coros = []
        for person_id in chunk:
            coro = get_person(client, person_id)
            coros.append(coro)
        person_data_list = [person_data for person_data in await asyncio.gather(*coros) if person_data is not None]
        insert_task = asyncio.create_task(insert_to_db(person_data_list))
        tasks.append(insert_task)
    tasks_set = asyncio.all_tasks()
    tasks_set.remove(asyncio.current_task())
    await asyncio.gather(*tasks_set)
    await client.close()
    await engine.dispose()


if __name__ == '__main__':
    start = datetime.now()
    asyncio.run(main())
    print(datetime.now() - start)
