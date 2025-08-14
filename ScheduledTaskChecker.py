#!/usr/bin/env python
# coding: utf-8

# # Scheduled Task Checker
# ### By:  Gary Bezet
# 
# I got tired of logging into my utility server to check if things had run, and looking up what times thing will next for people
# So I hacked this out in an hour or so to upload schedule task information to my SQL Sever
# 
# It's kinda cowboy but I write code every day and I never get around to santizing it for GitHub and this code has nothing private in it
# 
# So here it is

# In[1]:


import subprocess
import io
import pandas as pd
from datetime import datetime
import sqlalchemy as sa
from os import environ


# In[2]:


schema = 'etl'
tableName = 'ScheduledTasks'


# In[3]:


def lp(v):
    print(f"[{datetime.now()}] {v}")

lp("Starting...")


# In[4]:


lp("Loading enviconrment")
engine = sa.create_engine(environ.get("KNOS_Datawarehouse"), fast_executemany=True)
lp(engine)


# In[5]:


procArgs = ["schtasks.exe", "/query", "/FO", "CSV", "/v"]
lp(f"Loading {procArgs}")
results = io.StringIO( subprocess.run(
    procArgs,
    capture_output=True,
    text=True,
    check=True
).stdout)
results.flush()
results.seek(0)


# In[6]:


lp("Loading results to Pandas dataframe")
data = pd.read_csv(results)
lp(f"Dataframe shape:  {data.shape}")


# In[7]:


lp("Dropping rows with column labels")
data = data[data['HostName'] != 'HostName'].copy()


# In[8]:


for col in ['Next Run Time', 'Last Run Time']:
    lp(f"Changing [{col}] to datetime")
    data[col] = pd.to_datetime(data[col], format="%m/%d/%Y %H:%M:%S %p")


# In[9]:


for col in ['Last Result']:
    lp(f"Changing [{col}] to int")


# In[10]:


lp("Calculating SQL Types")
sqlTypes = {}
for col in data.columns:
    if data[col].dtype == object:
        colLen = data[col].str.len().max()
        sqlTypes[col] = sa.VARCHAR(int(colLen)) if colLen else sa.VARCHAR(1)

lp(sqlTypes)


# In[11]:


lp(f"Connecting to {engine}")
with engine.connect() as conn:
    tmpTable = '#looktmptable'
    lp(f"Uploading data to {tmpTable}")
    data.to_sql(tmpTable, conn, schema=schema, dtype=sqlTypes)
    conn.execute(sa.text('commit;'))
    lp(f"Finished copying data from {tmpTable} -> [{schema}].[{tableName}]")
    conn.execute(sa.text(f"""
        begin transaction;
            drop table if exists [{schema}].[{tableName}];
            select * 
            into [{schema}].[{tableName}]
            from {tmpTable};

        commit transaction;
    """))

lp("Finished")


# In[12]:


#jupyter nbconvert --to python .\ScheduledTaskChecker.ipynb

