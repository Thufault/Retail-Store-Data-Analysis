# -*- coding: utf-8 -*-
"""
Created on Mon Oct  9 17:19:23 2023

@author: Tyler Hufault
"""
### merge 12 datasets into a dataframe
import pandas as pd
import os

Current_path=os.getcwd()
os.listdir(Current_path)
mypath= 'C:\\Users\Tyler Hufault\Downloads\Sales_Data_ Analysis\SalesAnalysis\Sales_Data'

files = list(file for file in os.listdir(mypath))

all_months_data= pd.DataFrame()

for file in files:
    current_data = pd.read_csv(mypath+'\\'+ file)
    all_months_data = pd.concat([all_months_data, current_data])
all_months_data.to_csv('C:\\Users\Tyler Hufault\Downloads\Sales_Data_ Analysis\SalesAnalysis\Sales_Data\ all_data_copy.csv', index=False)
### what was the best month for sales ? how much was earned that month??
all_data=pd.read_csv('C:\\Users\Tyler Hufault\Downloads\Sales_Data_ Analysis\SalesAnalysis\Sales_Data\ all_data_copy.csv')

all_data.head()
### Augment data with additional columns
### Add Month Column
all_data['Month']=all_data['Order Date'].str[0:2]
### take all null values and delete them
nan_df=all_data[all_data.isna().any(axis=1)]
all_data= all_data.dropna(how='all')
###take all header records and delete them
all_data= all_data[all_data['Order Date'].str[0:2]!='Or']

### convert to integer
all_data['Month']=all_data['Month'].astype('int32')

### Task 3 add a sales column
#### convert columns to correct type
all_data['Quantity Ordered']=pd.to_numeric(all_data['Quantity Ordered'])
all_data['Price Each']=pd.to_numeric(all_data['Price Each'])

### Add a Sales Column
all_data['Sales']=all_data['Quantity Ordered']* all_data['Price Each']
### monthly sales analysis
results= all_data.groupby('Month').sum()

import matplotlib.pyplot as plt
months= range(1,13)
plt.bar(months, results['Sales'])
plt.xticks(months)
plt.ylabel('Sales in USD ($) Millions')
plt.xlabel('Month Number')
plt.title('Sales Per Month')
plt.show()
### What US city had the highest number of sales?
### add city column
def get_city(address):
    return address.split(',')[1]
def get_state(address):
    return address.split(',')[2].split(' ')[1]
all_data['City']=all_data['Purchase Address'].apply(lambda x:get_city(x)+' ('+ get_state(x)+')')

results1= all_data.groupby('City').sum()

cities= [city for city, df in all_data.groupby('City')]

plt.bar(cities, results1['Sales'])
plt.xticks(cities, rotation ='vertical',size=10)
plt.ylabel('Sales in USD ($)')
plt.xlabel('City name')
plt.title('Sales Per City')
plt.show()

### What time should we display advertisements to maximize likelihood of customer's buying the product?
all_data['Order Date']=pd.to_datetime(all_data['Order Date'])
all_data['Hour']=all_data['Order Date'].dt.hour
all_data['Minute']=all_data['Order Date'].dt.minute

hours= [hour for hour, df in all_data.groupby('Hour')]
### count of occurances of records for each hour of the day
plt.plot(hours, all_data.groupby(['Hour']).count())
plt.xticks(hours)
plt.grid()
plt.xlabel('Hours of the Day')
plt.ylabel('# of customers')
plt.title('Purchases of Products at Certain Times of the Day')
plt.show()

### my recommendation is to run advertisements just before 11am (11) or 7pm (19) because those the peak time that most cutomers order products

### What products are most often sold together??
df= all_data[all_data['Order ID'].duplicated(keep=False)]

df['Grouped']= df.groupby('Order ID')['Product'].transform(lambda x:','.join(x))
df= df[['Order ID', 'Grouped']].drop_duplicates()
from itertools import combinations
from collections import Counter

count = Counter()

for row in df['Grouped']:
    row_list = row.split(',')
    count.update(Counter(combinations(row_list, 2)))
    
for key, value in count.most_common(10):
    print(key, value)
    
### what product sold the most? Whyy??
Product_group=all_data.groupby('Product')
quantity_ordered= Product_group.sum()['Quantity Ordered']
Products= [Product for Product, df in Product_group]
plt.bar(Products,quantity_ordered)
plt.xticks(Products, rotation='vertical', size=8)
plt.ylabel('Quantity Ordered')
plt.xlabel('Product')


prices= all_data.groupby('Product').mean()['Price Each']

fig, ax1=plt.subplots()
ax2= ax1.twinx()
ax1.bar(Products, quantity_ordered, color='g')
ax2.plot(Products,prices,'b-')
plt.title('Prices Versus Product Quantity')
ax1.set_xlabel('Product Name')
ax1.set_ylabel('Quantity Ordered', color='g')
ax2.set_ylabel('Price ($)', color='b')
ax1.set_xticklabels(Products, rotation='vertical',size=8)
plt.show()
### Business Data Analytics of all_data
### time series moving average of product 
results2=results['Sales']
plt.plot(results2)
plt.xticks(months)
plt.ylabel('Sales in USD ($) Millions')
plt.xlabel('Month Number')
plt.title('Sales Per Month')
plt.show()
plt.plot(results2)
plt.plot(results2.rolling(window=2).mean())
plt.xticks(months)
plt.ylabel('Sales in USD ($) Millions')
plt.xlabel('Month Number')
plt.title('Sales Per Month Forcast')
plt.legend(['Actuals','Moving Avg(2)'])
plt.show()
results2=pd.DataFrame(results2)
results2['Moving_Avg']=results2['Sales'].rolling(window=2).mean()
results2['Forcast_Error']=results2['Sales']-results2['Moving_Avg']
supervised_data=results2.drop(['Sales','Moving_Avg'],axis=1)
results2=results2.reset_index(drop=True)
results2['Month']=range(1,13)
### clean up the data for accurate results of Monthly Sales
all_data_new=all_data.drop(['Order ID', 'Product','Purchase Address','Hour','Minute','Month','City'],axis=1)
all_data_new['Quantity Ordered']=pd.to_numeric(all_data_new['Quantity Ordered'])
all_data_new['Price Each']=pd.to_numeric(all_data_new['Price Each'])

### Add a Sales Column
all_data_new['Sales']=all_data_new['Quantity Ordered']* all_data_new['Price Each']
all_data_new=all_data_new.drop(['Quantity Ordered','Price Each'], axis=1)
#### Converting columns to integer64 and datetime64
all_data_new['Sales']=all_data_new['Sales'].astype('int64')
all_data_new['Order Date']= pd.to_datetime(all_data_new['Order Date'])
all_data_new.info()
#### converting date to month period 
all_data_new['Order Date']=all_data_new['Order Date'].dt.to_period('M')
monthly_sales= all_data_new.groupby('Order Date').sum().reset_index()
monthly_sales['Order Date']=monthly_sales['Order Date'].dt.to_timestamp()
plt.figure(figsize=(15,5))
plt.plot(monthly_sales['Order Date'],monthly_sales['Sales'])
moving3=monthly_sales['Sales'].rolling(window=3).mean()
plt.plot(monthly_sales['Order Date'],moving3)
plt.ylabel('Sales in USD ($) Millions')
plt.xlabel('Date')
plt.title('Sales Per Month')
plt.legend(['Actuals','Moving Avg(3)'])
plt.show()

###Converting Data to back to csv
### Sales Per Month Tbl
results.to_csv('C:/Users/Tyler Hufault/Downloads/Sales_Data_ Analysis/Sales Per Month/Sales_Per_Month_Tbl.csv')
### Sales Per City Tbl
results1.to_csv('C:/Users/Tyler Hufault/Downloads/Sales_Data_ Analysis/Sales Per City Bar/Sales_Per_City_Tbl.csv')
### Purchases of products Per Hour
lst=all_data.groupby(['Hour']).count()
df2=pd.DataFrame(lst,hours)
df2.to_csv('C:/Users/Tyler Hufault/Downloads/Sales_Data_ Analysis/Purchases of Products at Certain Times of the Day/Product Per Hour.csv')
### Price vs Product Quantity
df3=pd.DataFrame(quantity_ordered,prices)
df3.to_csv('C:/Users/Tyler Hufault/Downloads/Sales_Data_ Analysis/Price vs Product Quantity Bar/Price vs Product Quantity Tbl.csv')
### Sales Per Month Forcast
results2.to_csv('C:/Users/Tyler Hufault/Downloads/Sales_Data_ Analysis/Sales Per Month Forcast/Monthly Forcast Tbl.csv')
