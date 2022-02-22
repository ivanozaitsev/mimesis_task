from main import DataGenerator


dg = DataGenerator()
dg.create_tables()
dg.df_creation(1000)
dg.insertion()

"""

 In case you will need to drop tables from database:
"""
"""
dg.drop_tables()
"""