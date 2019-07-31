from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from Scraper import Scraper
from declarations import Event, Base

import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

dag = DAG('event scraper',
          description='loads events from kufstein',
          schedule_interval='0 0 * * *',
          start_date=datetime.datetime(2019, 1, 1),
          catchup=False)

def scrape():
    engine = create_engine('sqlite:///events_db.sqlite')
    Base.metadata.bind = engine

    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    scraper = Scraper()
    events = scraper.get_data()

    for event in events:
        ev = session.query(Event).filter_by(identifier=event['identifier']).first()

        if not ev:
            session.add(Event(
                name=event['name'],
                location=event['location'],
                link=event['link'],
                short=event['short'],
                date=event['date'],
                source=event['source'],
                identifier=event['identifier']
            ))

    session.commit()
    session.close()

first_task = PythonOperator(task_id='first_task', python_callable=scrape, dag=dag)

first_task