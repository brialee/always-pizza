import pandas as PD
import numpy as NP
import matplotlib.pyplot as PLT
import seaborn as SNS
from matplotlib.pylab import rcParams

from urllib.request import urlopen
from bs4 import BeautifulSoup


# Box plot setup
rcParams['figure.figsize'] = 15, 5

# Fetch the dataset
url = "http://www.hubertiming.com/results/2017GPTR10K"
html = urlopen(url)

# Soup it
soup = BeautifulSoup(html, 'lxml')

# The data we're looking for is found in table rows, get 'em
all_rows = soup.find_all('tr')
all_rows_str_list = []

for row in all_rows:
    row_data = row.find_all('td')
    clean_data = BeautifulSoup(str(row_data), 'lxml').get_text()
    if clean_data == '[]':
        pass
    else:
        all_rows_str_list.append(clean_data)

# Also need the headers for the data
header_labels = soup.find_all('th')
all_headers = []
clean_headers = BeautifulSoup(str(header_labels), 'lxml').get_text()
all_headers.append(clean_headers.replace('[', '').replace(']', ''))

df_headers = PD.DataFrame(all_headers)
headers_data_frames = df_headers[0].str.split(',', expand=True)

# Clean strings of unwanted characters
idx = 0
for i in all_rows_str_list:
    i = i.replace('[', '')
    i = i.replace(']', '')
    i = i.replace('\n', '')
    i = i.replace('\r', '')

    row_strs = i.split(',')
    s_idx = 0
    for s in row_strs:
        s = s.strip()
        row_strs[s_idx] = s
        s_idx +=1

    i = ','.join(row_strs)
    all_rows_str_list[idx] = i
    idx +=1

df = PD.DataFrame(all_rows_str_list)
race_data_frames = df[0].str.split(',', expand=True)

frames = [headers_data_frames, race_data_frames]

all_data = PD.concat(frames)
all_data = all_data.rename(columns=all_data.iloc[0]).dropna(axis=0, how='any').drop(all_data.index[0])

# Transform Chip Time into new column 
time_list = all_data[' Chip Time'].tolist()
time_mins = []
for t in time_list:
    try:
        h, m, s = t.split(':')
        math = (int(h) * 3600 + int(m) * 60 + int(s)) / 60
        time_mins.append(math)

    # Runner may have finished in less than an hour....
    except ValueError:
        m, s = t.split(':')
        math = (int(m) * 60 + int(s)) / 60
        time_mins.append(math)

# Add transformed data to new column, Runner_mins
all_data['Runner_mins'] = time_mins
print(str(all_data.head(30)))


'''
Question 1: what was the average finish time (in minutes) for the runners?​
'''

# Some stats
#print(all_data.describe(include=[NP.number]))

# Generate and display boxplot
# all_data.boxplot(column='Runner_mins')
# PLT.grid(True, axis='y')
# PLT.ylabel('Chip Time')
# PLT.xticks([1], ['Runners'])
#PLT.show()


'''
Question 2:
Did the runners' finish times follow a normal distribution?
One thing to note here is the large number of outliers on the high end of the race times. 
If we ignored those, then the normal distribution might be an even better model of the data. 
But if we include those outliers, then we might favor a more sophisticated 
distribution to model the data properly.
'''
# Generate and display distribution plot
#dist_plot = all_data['Runner_mins']
#ax = SNS.distplot(dist_plot, hist=True, kde=True, rug=False, color='m', bins=25, hist_kws={'edgecolor': 'black'})
#PLT.show()


'''
Question 3:
Were there any performance differences between males 
and females of various age groups?
'''

# FIXME?
# Seperate chip times into m/f
# female_times = all_data.loc[all_data[' Gender'] == ' F']['Runner_mins']
# male_times = all_data.loc[all_data[' Gender'] == ' M']['Runner_mins']

# SNS.distplot(female_times, hist=True, kde=True, rug=False, hist_kws={'edgecolor': 'black'}, label='Female')
# SNS.distplot(male_times, hist=False, kde=True, rug=False, hist_kws={'edgecolor': 'black'}, label='Male')
# PLT.legend()
# PLT.show()

# Or, in text form
gender_stats = all_data.groupby(" Gender", as_index=True).describe()
print(gender_stats)

# Or, in boxplot form
all_data.boxplot(column='Runner_mins', by=' Gender')
PLT.ylabel('Chip Time')
PLT.suptitle("")
PLT.show()
exit()