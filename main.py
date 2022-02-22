import random
from uuid import uuid4
from datetime import datetime

from mimesis import Person, Hardware, Development
from mimesis.schema import Schema
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, TIMESTAMP, ForeignKey
import pandas as pd

from config import username, password, dbname, hostname, path


class DataGenerator:
    """

     Define class DataGenerator that creates datasets with fake date.
     Methods new_users, new_companies and new_devs create dataframes filled with fake data.
     Method create_tables creates tables users, companies, development inside MySQL DB.
     Method df_creation triggers methods new_users, new_companies and new_devs.
     Method insertion moves data from dataframes to tables inside MySQL DB.
     Method drop_tables drops created tables from MySQL DB.
    """
    def __init__(self):
        self.users = pd.DataFrame()
        self.companies = pd.DataFrame()
        self.devs = pd.DataFrame()

    def new_users(self, n):
        if n < 1:
            raise ValueError('n must be positive number')
        user = Person()
        ids = list(range(0, ))
        schema = Schema(schema=lambda: {
            'id': str(uuid4().int)[:7],
            'name': user.first_name(),
            'surname': user.surname(),
            'age': user.age(),
            'email': user.email(),
            'gender': user.gender(),
            'language': user.language(),
            'country': user.nationality(),
            'occupation': user.occupation(),
            'phone': user.telephone(),
            'username': user.username(),
            'added_at': datetime.now(),
            'added_by': 'mimesis.Person'
        })
        schema = schema.create(iterations=n)
        return pd.DataFrame(schema)

    def new_companies(self):
        user = Person()
        hard = Hardware()
        schema = Schema(schema=lambda: {
            'company': hard.manufacturer(),
            'ceo': user.full_name(),
            'country': user.nationality(),
            'added_at': datetime.now(),
            'added_by': 'mimesis.Person & mimesis.Hardware'
        })
        schema = schema.create(iterations=4)
        df = pd.DataFrame(schema)
        return df

    def new_devs(self, n):
        if n < 1:
            raise ValueError('n must be positive number')
        dev = Development()
        schema = Schema(schema=lambda: {
            'language': dev.programming_language(),
            'language_2': dev.programming_language(),
            'software_license': dev.software_license(),
            'os': dev.os(),
            'experience': random.randint(1, 30),
            'added_at': datetime.now(),
            'added_by': 'mimesis.Development',
            'income_usd': random.randint(2, 50) * 100
        })
        schema = schema.create(iterations=n)
        df = pd.DataFrame(schema)
        df = df.loc[df['language_2'] != df['language']]
        return df

    def create_tables(self):
        meta = MetaData()
        users_table = Table("users", meta,
                            Column("id", Integer, primary_key=True),
                            Column("name", String(255)),
                            Column("surname", String(255)),
                            Column("age", Integer),
                            Column("email", String(255)),
                            Column("gender", String(255)),
                            Column("language", String(255)),
                            Column("country", String(255)),
                            Column("occupation", String(255)),
                            Column("phone", String(255)),
                            Column("username", String(255)),
                            Column("added_at", TIMESTAMP),
                            Column("added_by", String(255))
                            )

        development_table = Table("development", meta,
                                  Column("dev_id", Integer, autoincrement=True, primary_key=True),
                                  Column("user_id", Integer, ForeignKey("users.id")),
                                  Column("language", String(255)),
                                  Column("language_2", String(255)),
                                  Column("software_license", String(255)),
                                  Column("os", String(255)),
                                  Column("experience", Integer),
                                  Column("added_at", TIMESTAMP),
                                  Column("added_by", String(255), default='mimesis.Person'),
                                  Column("income_usd", Integer)
                                  )

        companies_table = Table("companies", meta,
                                Column("company_id", Integer, autoincrement=True, primary_key=True),
                                Column("company", String(255), unique=True),
                                Column("ceo", String(255)),
                                Column("country", String(255)),
                                Column("added_at", TIMESTAMP),
                                Column("added_by", String(255))
                                )
        try:
            engine = create_engine(
                path.format(username, password, hostname, dbname),
                echo=True)
            meta.create_all(engine)
            connection = engine.connect()
            print('MySql connection is opened')
        finally:
            connection.close()
            print('Tables users, development, companies were created. Connection is closed')

    def df_creation(self, n):
        self.users = self.new_users(n)
        self.devs = self.new_devs(n)
        self.companies = self.new_companies()
        self.devs['user_id'] = self.users['id']
        print('Datasets new_users, new_devs, new_companies were created')

    def insertion(self):
        try:
            engine = create_engine(
                path.format(username, password, hostname, dbname),
                echo=True)
            conn = engine.connect()
            self.users.to_sql(name='users', con=conn, if_exists='append', index=False)
            self.devs.to_sql(name='development', con=conn, if_exists='append', index=False)
            self.companies.to_sql(name='companies', con=conn, if_exists='append', index=False)
        finally:
            conn.close()
            print('Data was inserted into MySQL DB. Connection is closed')

    def drop_tables(self):
        try:
            engine = create_engine(
                path.format(username, password, hostname, dbname),
                echo=True)
            conn = engine.connect()
            conn.execute("DROP TABLE development")
            conn.execute("DROP TABLE companies")
            conn.execute("DROP TABLE users")
        finally:
            conn.close()
            print('Tables were removed. Connection is closed')
