from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from config import username, password, hostname, dbname, cols, path

try:
    engine = create_engine(path.format(username, password, hostname, dbname),
                           echo=True)
    conn = engine.connect()
    df_users = pd.read_sql("select * from {}.users".format(dbname), conn)
    df_companies = pd.read_sql("select * from {}.companies".format(dbname), conn)
    df_development = pd.read_sql("select * from {}.development".format(dbname), conn)
finally:
    conn.close()
    print('Table has been read. Connection is close')

df_users['name'] = df_users[['name', 'surname']].agg(' '.join, axis=1)
df_users = df_users.drop('surname', axis=1).rename(columns={'name': 'full_name'})
pd.set_option('display.max_columns', None)
df_users['birth_year'] = pd.to_datetime('1991-01-01 00:00:01')
df_users['company_id'] = np.random.randint(1, 5, size=len(df_users))
df_users_companies = df_users.join(df_companies.set_index('company_id'), on='company_id', how='left',
                                   lsuffix='_user', rsuffix='_company')
df_users = df_users.rename(columns={'language': 'native_language'})
df_users_devs = df_users.join(df_development.set_index('user_id'), on='id', how='left',
                              rsuffix='_development')


for col in cols:
    df_users_devs[col] = df_users_devs[col].astype('Int32')

users_haskell = df_users_devs.loc[(df_users_devs['experience'] > 2) &
                                  ((df_users_devs['language'] == 'Haskell') |
                                  (df_users_devs['language_2'] == 'Haskell'))]

table = pa.Table.from_pandas(users_haskell)
pq.write_table(table, 'users_haskell.parquet')

df_users_devs = df_users_devs.sort_values(by=['added_at'])
df_users_devs.to_csv('users_devs.csv', sep='|', index=False, header=False)

df_users_companies.to_csv('users_companies.csv', sep='|', index=False, header=False)

users_big_salary = df_users_devs.loc[df_users_devs['income_usd'] > 3000]
users_big_salary.to_csv('users_big_salary.csv', sep='|', index=False, header=False)

users_apache_lic = df_users_devs[df_users_devs['software_license'].str.contains('Apache', na=False)]
users_apache_lic.to_csv('users_apache_lic.csv', sep='|', index=False, header=False)
print('Files users_haskell.parquet, users_devs.csv, users_companies.csv,'
      '\n users_big_salary.csv, users_apache_lic.csv were created')