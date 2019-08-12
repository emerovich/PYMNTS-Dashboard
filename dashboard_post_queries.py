

import pandas as pd
import matplotlib.pyplot as plt
#from pptx import Presentation

#Cargo los outputs de Final Query JFC y de Query Parche para Content Log
author_us = pd.read_table(r"D:\Dropbox (MPD)\Analytics Argentina\Dashboard\Trabajo\Exports From PostgreSQL\author_us_position_stats.txt")
author_b2b = pd.read_table(r"D:\Dropbox (MPD)\Analytics Argentina\Dashboard\Trabajo\Exports From PostgreSQL\author_b2b_position_stats.txt")
author_retail = pd.read_table(r"D:\Dropbox (MPD)\Analytics Argentina\Dashboard\Trabajo\Exports From PostgreSQL\author_retail_position_stats.txt")
author_sku = pd.read_table(r"D:\Dropbox (MPD)\Analytics Argentina\Dashboard\Trabajo\Exports From PostgreSQL\author_sku_position_stats.txt")
author_web = pd.read_table(r"D:\Dropbox (MPD)\Analytics Argentina\Dashboard\Trabajo\Exports From PostgreSQL\author_web_content_stats.txt")
stats3 = pd.read_table(r"D:\Dropbox (MPD)\Analytics Argentina\Dashboard\Trabajo\Exports From PostgreSQL\New Request Whatev - Stats3.txt")
content_log = pd.read_table(r"D:\Dropbox (MPD)\Analytics Argentina\Dashboard\Trabajo\For PostgreSQL\content_log - test.txt", header=None)
author_dictionary = pd.read_table(r"D:\Dropbox (MPD)\Analytics Argentina\Dashboard\authors_dictionary.txt",encoding='WINDOWS-1252')
content_log.rename(columns={0:'Campaign',
                            1:'Feature',
                            2:'Writer',
                            3:'Headline',
                            4:'Link',
                            5:'permalink',
                            6:'Final NumericPerm',
                            7:'Largo',
                            8:'Client',
                            9:'Date'},
                inplace=True)

#Agrego columna con la campaign y reordeno las columnas
author_us['Campaign']= 'US'
author_b2b['Campaign']= 'B2B'
author_retail['Campaign']= 'Retail'
author_sku['Campaign']= 'SKU'
author_web['Campaign']= 'Web Content'

cols = author_us.columns.tolist()
cols = cols[:3]+cols[-1:]+cols[4:-1]+cols[3:4]
author_us = author_us[cols]

cols = author_b2b.columns.tolist()
cols = cols[:3]+cols[-1:]+cols[4:-1]+cols[3:4]
author_b2b = author_b2b[cols]

cols = author_retail.columns.tolist()
cols = cols[:3]+cols[-1:]+cols[4:-1]+cols[3:4]
author_retail = author_retail[cols]

cols = author_sku.columns.tolist()
cols = cols[:3]+cols[-1:]+cols[4:-1]+cols[3:4]
author_sku = author_sku[cols]

cols = author_web.columns.tolist()
cols = cols[:3]+cols[-1:]+cols[4:-1]+cols[3:4]
author_web = author_web[cols]

#Cambio nombre de las columnas en los data frames para poder concatenarlos despues

author_us.rename(columns={'us_position':'position'},
                inplace=True)

author_b2b.rename(columns={'b2b_position':'position'},
                inplace=True)

author_retail.rename(columns={'retail_position':'position'},
                inplace=True)

author_sku.rename(columns={'sku_position':'position'},
                inplace=True)

author_web.rename(columns={'web_content':'position'},
                inplace=True)


frames = [author_us, author_b2b, author_retail, author_sku, author_web]
raw_data = pd.concat(frames)

#agrego la/las observaciones que vienen de query parche para content log (stats3). 
# Cambio nombre y orden de algunas columnas

stats3 = stats3[stats3['week_end_date']==raw_data.iloc[0,1]]

stats3.rename(columns={'btot_users':'t_users',
                       'btot_sessions':'t_sessions',
                       'btot_entrances':'t_entrances',
                       'btot_pageviews':'t_pageviews',
                       'btot_unique_pageviews':'t_unique_pageviews',
                       'btot_bounces':'t_bounces',
                       'btot_exits':'t_exits',
                       'btime_on_page':'t_time_on_page',
                       'bbounce_rate':'avg_bounce_rate',
                       'exit_rate':'avg_exit_rate',
                       'bavg_sessions_duration':'avg_session_duration',
                       'bavg_time_on_page':'avg_time_on_page',
                       'week_end_date':'last_week_day'},
              inplace=True)

stats3['avg_users']=stats3['t_users']
stats3['avg_sessions']=stats3['t_sessions']
stats3['avg_entrances']=stats3['t_entrances']
stats3['avg_pageviews']=stats3['t_pageviews']
stats3['avg_unique_pageviews']=stats3['t_unique_pageviews']
stats3['avg_bounces']=stats3['t_bounces']
stats3['avg_exits']=stats3['t_exits']
stats3['avg_t_time_on_page']=stats3['t_time_on_page']

stats3['t_session_duration']=stats3['avg_sessions']*stats3['avg_session_duration']
stats3['avg_t_session_duration']=stats3['t_session_duration']
stats3['semana']=raw_data.iloc[0,0]
stats3['qstories']=1

#Hago un join para agregarle a stats3 la info que viene de content log
#Cambio el nombre y orden de mas columnas

stats3 = pd.merge(stats3, content_log, left_on='categoria', right_on='Headline')
cols = stats3.columns.tolist()
cols = cols[0:1] + cols[2:-7] + cols[31:32]
cols = cols[23:24]+cols[0:1]+cols[27:28]+cols[25:27]+cols[24:25]+cols[1:8]+cols[21:22]+cols[8:9]+cols[13:20]+cols[22:23]+cols[20:21]+cols[9:13]+cols[-1:]
stats3 = stats3[cols]

stats3.rename(columns={'Feature':'position',
                       'Writer':'writer',
                       'permalink':'text_permalink'},
              inplace=True)

#ahora me falta cambiar los valores de Campaign en stats3 antes de hacer el merge entre raw data y lo que viene de stats3 y de sortear por semana.

stats3.loc[stats3['Campaign'] == 'Web', 'Campaign'] = 'Web Content'
stats3.loc[stats3['Campaign'] == 'Main', 'Campaign'] = 'US'

frames2 = [raw_data, stats3]
raw_data = pd.concat(frames2)

raw_data = raw_data.reset_index(drop=True)

#Ordeno por semana y por campaign. De la manera en que esta implementado esto, lo que venga de stats3 va a quedar desordenado
#Creo que no es necesario ordenar de esta manera. En el excel lo hace asi, pero me parece que cumple una finalidad mas visual que otra cosa.


raw_data = raw_data.rename_axis('MyIdx').sort_values(by = ['semana', 'MyIdx'], ascending = [False, True])

#CON ESTO TERMINA LA TAB DE RAW DATA
#AHORA PASO A CREAR LA TABLA Y EL GRAFICO DE AUTHOR_STATS_ALL_VARS

cols = raw_data.columns.tolist()
cols = cols[:-1]
author_stats_all_vars = raw_data[cols]
author_stats_all_vars['sumif']=author_stats_all_vars['last_week_day']+'--'+author_stats_all_vars['Campaign']

fecha = raw_data.iloc[0,1]

us_pageviews = author_stats_all_vars[author_stats_all_vars['sumif']==fecha+'--US']['t_pageviews'].sum()
b2b_pageviews = author_stats_all_vars[author_stats_all_vars['sumif']==fecha+'--B2B']['t_pageviews'].sum()
retail_pageviews = author_stats_all_vars[author_stats_all_vars['sumif']==fecha+'--Retail']['t_pageviews'].sum()
web_pageviews = author_stats_all_vars[author_stats_all_vars['sumif']==fecha+'--Web Content']['t_pageviews'].sum()

total_pageviews = us_pageviews + b2b_pageviews + retail_pageviews + web_pageviews

us_unique_pageviews = author_stats_all_vars[author_stats_all_vars['sumif']==fecha+'--US']['t_unique_pageviews'].sum()#MODIFICAR ESTO PARA QUE NO HAYA QUE PONER LAS FECHA DE MANERA MANUAL
b2b_unique_pageviews = author_stats_all_vars[author_stats_all_vars['sumif']==fecha+'--B2B']['t_unique_pageviews'].sum()
retail_unique_pageviews = author_stats_all_vars[author_stats_all_vars['sumif']==fecha+'--Retail']['t_unique_pageviews'].sum()
web_unique_pageviews = author_stats_all_vars[author_stats_all_vars['sumif']==fecha+'--Web Content']['t_unique_pageviews'].sum()

total_unique_pageviews = us_unique_pageviews + b2b_unique_pageviews + retail_unique_pageviews + web_unique_pageviews

share_us_pageviews = (us_pageviews/total_pageviews)*100
share_b2b_pageviews = (b2b_pageviews/total_pageviews)*100
share_retail_pageviews = (retail_pageviews/total_pageviews)*100
share_web_pageviews = (web_pageviews/total_pageviews)*100
share_us_unique = (us_unique_pageviews/total_unique_pageviews)*100
share_b2b_unique = (b2b_unique_pageviews/total_unique_pageviews)*100
share_retail_unique = (retail_unique_pageviews/total_unique_pageviews)*100
share_web_unique = (web_unique_pageviews/total_unique_pageviews)*100

#AHORA PASO A LA TAB AUTHOR_STATS_MAIN_VARS

cols = author_stats_all_vars.columns.tolist()
cols = cols[1:5]+cols[15:16]+cols[18:19]+cols[27:28]
author_stats_main_vars = author_stats_all_vars[cols]
author_stats_main_vars['writer'] = author_stats_main_vars['writer'].str.strip()
author_stats_main_vars['writer'] = author_stats_main_vars['writer'].str.lower() 
author_dictionary['author'] = author_dictionary['author'].str.strip()
author_dictionary['author'] = author_dictionary['author'].str.lower() 
author_dictionary = author_dictionary.drop_duplicates(subset = ['author'])

author_stats_main_vars = pd.merge(author_stats_main_vars, author_dictionary, left_on='writer', right_on='author', how='left')

author_stats_main_vars['Date--Writer']=author_stats_main_vars['last_week_day']+'--'+author_stats_main_vars['rename']
author_stats_main_vars['Date--Camp']=author_stats_main_vars['last_week_day']+'--'+author_stats_main_vars['Campaign']
author_stats_main_vars['Date--Type']=author_stats_main_vars['last_week_day']+'--'+author_stats_main_vars['position']
author_stats_main_vars['Date-Writer-Type']=author_stats_main_vars['last_week_day']+'--'+author_stats_main_vars['rename']+'--'+author_stats_main_vars['position']
author_stats_main_vars['Date-Writer-Camp-Type']=author_stats_main_vars['last_week_day']+'--'+author_stats_main_vars['rename']+'--'+author_stats_main_vars['Campaign']+'--'+author_stats_main_vars['position']
author_stats_main_vars['Number of Stories']=1

cols = author_stats_main_vars.columns.tolist()
cols = cols[0:1]+cols[8:9]+cols[2:4]+cols[9:15]+cols[4:7]
author_stats_main_vars = author_stats_main_vars[cols]


total_stories = author_stats_main_vars.pivot_table("Number of Stories", ["last_week_day"], "rename",aggfunc="count").fillna(0)
total_pageviews = author_stats_main_vars.pivot_table("avg_pageviews", ["last_week_day"], "rename",aggfunc="sum").fillna(0)

#Ahora creo la tabla que me da el numero de articulos por autor por quarter-year

author_stats_main_vars.last_week_day = pd.to_datetime(author_stats_main_vars.last_week_day)
author_stats_main_vars['quarter'] = pd.PeriodIndex(author_stats_main_vars.last_week_day, freq='Q')

stories_author_quarter = author_stats_main_vars.pivot_table("Number of Stories", ["quarter"], "rename",aggfunc="count").fillna(0)
pageviews_author_quarter = author_stats_main_vars.pivot_table("avg_pageviews", ["quarter"], "rename",aggfunc="sum").fillna(0)

###############################    
###############################     Para terminar con la tab "Author Stats Main Vars" tengo que armar la tabla que tiene como columna
###############################     los quarter-year de stories y pageviews y como filas los autores  

#GRAFICOS

fig = plt.figure(figsize=(10,5))
ax1 = fig.add_subplot(1,2,1)
labels = 'US', 'B2B', 'Retail', 'Web'
sizes = [share_us_pageviews, share_b2b_pageviews, share_retail_pageviews, share_web_pageviews]
colors = ['gold', 'yellowgreen', 'lightcoral', 'lightskyblue']
explode = (0,0,0,0)

plt.pie(sizes, explode=explode, labels=labels, colors=colors,
autopct='%1.1f%%', shadow=True, startangle=140)

plt.axis('equal')
ax1.set_title("Share of Pageviews by Outlet") 
ax2 = fig.add_subplot(1,2,2)
labels = 'US', 'B2B', 'Retail', 'Web'
sizes = [share_us_unique, share_b2b_unique, share_retail_unique, share_web_unique]
colors = ['gold', 'yellowgreen', 'lightcoral', 'lightskyblue']
explode = (0,0,0,0)

plt.pie(sizes, explode=explode, labels=labels, colors=colors,
autopct='%1.1f%%', shadow=True, startangle=140)

plt.axis('equal')
ax2.set_title("Share of Unique Pageviews by Outlet") 
fig.subplots_adjust(left=0.2, wspace=0.6)
plt.show()


