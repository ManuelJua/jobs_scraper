import os
from dotenv import load_dotenv
import logging
from ReedProcessor import Database,JobAvailable
import asyncio
import aiohttp

load_dotenv()
logging.basicConfig(level=logging.INFO)

def main():
    connection_string=os.getenv('DATABASE_URL')
    db=Database(connection_string=connection_string)
    query="""SELECT id,job_url from jobs WHERE is_active='true';"""
    df=db.query(query)

    checker=JobAvailable()
    for index,row in df.iterrows():
        is_available=checker.check_is_available(row['job_url'])
        if not is_available:
            insert_query=f"""UPDATE jobs SET is_active ='false' WHERE id={row['id']}"""
            db.single_insert(insert_query)
            logging.info(f"{index}--{row['id']}: {is_available}")

main()

# async def check_and_update(id:int,url:str,db:Database,client):
#     checker=JobAvailable()
#     is_available=await checker.async_check_is_available(url,client)
#     logging.info(is_available)
#     if not is_available:
#         insert_query=f"""UPDATE jobs SET is_active ='false' WHERE id={id}"""
#         await db.single_insert(insert_query)
#         logging.info(f"{id}: {is_available}")


# async def main():
#     connection_string=os.getenv('DATABASE_URL')
#     db=Database(connection_string=connection_string)
#     query="""SELECT id,job_url from jobs WHERE is_active='true';"""
#     df=db.query(query)

#     async with aiohttp.ClientSession() as client:
#         tasks=[asyncio.create_task(check_and_update(row['id'],row['job_url'],db,client)) for index,row in df.iterrows()]
#         await asyncio.gather(*tasks)

# if __name__=="__main__":
#     asyncio.run(main())
