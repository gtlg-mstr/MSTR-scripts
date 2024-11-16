from sqlalchemy import create_engine, text

db_username=''
db_password=''
db_server=''
db_port=''  #non std port used for obscurity, CHANGE this
db=''
db_connectionstring=f'postgresql+psycopg2://{db_username}:{db_password}@{db_server}:{db_port}/{db}'


engine = create_engine(db_connectionstring)
#This will replace the entire table. If you dont want this change the 'if_exists' option
#Specifically for use with Pandas dataframes
df.to_sql(name=tableNamefct, con=engine, if_exists='replace',index=False)


#Change of how sqlalchemy does the connection from 1.4 to 2.x
with engine.connect() as db_conn:
    print(f'{datetime.now()} - Checking uploaded rows')
    result=db_conn.execute(text("select count(*) from "+tableNamefct+";"))
    for row in result:
        print(str(row) + " Rows found")
