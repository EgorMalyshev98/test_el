import asyncio
import io
import pandas as pd
from uuid import uuid4
from os import environ as env
import psycopg
from dotenv import load_dotenv


async def copy_batch(conn: psycopg.AsyncConnection, df: pd.DataFrame, table: str):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    columns = ",".join(df.columns)
    stmt = f"COPY {table}({columns}) FROM STDIN (FORMAT csv, DELIMITER ',', NULL '')"
    async with conn.cursor().copy(stmt) as copy:
        await copy.write(csv_buffer.read())

async def process_csv(batch_size: int):
   
    async with await psycopg.AsyncConnection.connect(DB_CONN, autocommit=False) as conn:
        for df in pd.read_csv(
            'sample_data.csv', 
            usecols=range(7),
            names=['name', 'source', 'date', 'amount', 'subjects', 'courses', 'duration'],
            chunksize=batch_size
        ):

            df = df.dropna(how='all').fillna('')
            df['id'] = [uuid4() for _ in range(df.shape[0])]

            courses = (
                df[['id', 'courses']]
                .assign(course=df['courses'].str.split(','))
                .explode('course', ignore_index=True)
                .rename(columns={"id": "order_id"})
                [['order_id', 'course']]
            )
            subjects = (
                df[['id', 'subjects']]
                .assign(subject=df['subjects'].str.split(','))
                .explode('subject', ignore_index=True)
                .rename(columns={"id": "order_id"})
                [['order_id', 'subject']]
            )
            
            subjects[['subject', 'package']] = subjects['subject'].str.split('/', expand=True)
            subjects['subject'] = subjects['subject'].str.strip()
            subjects['package'] = subjects['package'].str.strip().fillna('')
        
            orders = df[['id', 'name', 'source', 'date', 'amount', 'duration']]
            orders['amount'] = (
                orders['amount']
                .str.replace(r'\s+', '', regex=True)
                .str.replace(',', '.')
                .astype('float')
            )
            
            await copy_batch(conn, orders, 'orders'),
            await copy_batch(conn, subjects, 'order_subjects'),
            await copy_batch(conn, courses, 'order_courses')
            
        await conn.commit()
        
            
    
if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    load_dotenv()
    
    DB_CONN = env.get('DB_CONN')
    BATCH_SIZE = 100_000
    
    for _ in range(20):
        asyncio.run(process_csv(BATCH_SIZE))
