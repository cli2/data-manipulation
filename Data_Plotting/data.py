import csv
import math
# STEP 1
# read world_bank_indicators
f = open('world_bank_indicators.txt','rU')
reader = csv.reader(f, delimiter = '\t')
d = list(reader)

# filter rows
dn = []
head = d[0]
for i in range(1,len(d)):
    if d[i][1][-4:] == '2000' or d[i][1][-4:] == '2010':
        dn.append(d[i])

# filter columns
l_kept = [0,1,4,5,6,9,19]
lnn = [18,17,16,15,14,13,12,11,10,8,7,3,2]
for i in lnn:
    del head[i]
for row in dn:
    for i in lnn:
        del row[i]

# add columns
head.append('Mobile subscribers per capita')
head.append('log(GDP per capita)')
head.append('log(Health: mortality under-5)')

# set decimal points
def decimal(n):
    return "%.5f" % n
for row in dn:
    row[2]=row[2].replace(',','')
    row[5]=row[5].replace(',','')
    row[6]=row[6].replace(',','')
    row[4]=row[4].replace(',','')
    if row[2]!='' and row[5]!='':
        row.append(decimal(int(row[2])/float(int(row[5]))))
    else:
        row.append('')
    if row[6]!='':
        row.append(decimal(math.log(int(row[6]))))
    else:
        row.append('')
    if row[4]!='':
        row.append(decimal(math.log(int(row[4]))))
    else:
        row.append('')

# STEP 2
# open region file
f1 = open('world_bank_regions.txt','rU')
reader1 = csv.reader(f1, delimiter='\t')
d1=list(reader1)

# add region column
head.append('Region')
for row1 in d1:
    n=0
    for row2 in dn:
        if row2[0]==row1[2]:
            row2.append(row1[0])
            n+=1
        if n>=2:
            break
for row in dn:
    if len(row)!=11:
        row.append('')

# STEP 5
# sorting by year
dn.sort(key=lambda x:x[1][-4:])

# sorting by region within year
def find_year_split(l):
    n = -1
    for row in l:
        if row[1][-4:]=='2000':
            n+=1
        else:
            break
    return n
year_split = find_year_split(dn)
l1 = dn[0:year_split+1]
l2 = dn[year_split+1:]
l1.sort(key=lambda x:x[-1])
l2.sort(key=lambda x:x[-1])
dn = l1+l2

# dropping missing data
for i in range(len(dn)-1,-1,-1):
    for j in dn[i]:
        if j=='':
            del dn[i]
            break

# sorting by GDP within year and region
def find_region_split(l):
    n = 0
    spliters=[]
    while n<len(l)-1:
        if l[n+1][-1]==l[n][-1]:
            n+=1
        else:
            n+=1
            spliters.append(n)
    return spliters
region_spliters = find_region_split(dn)
d_temp = []
for i in range(len(region_spliters)+1):
    if i==0:
        temp = dn[:region_spliters[i]]
        temp.sort(key=lambda x:float(x[6]))
        d_temp.append(temp)
    if i<len(region_spliters):
        temp = dn[region_spliters[i-1]:region_spliters[i]]
        temp.sort(key=lambda x:float(x[6]))
        d_temp.append(temp)
    else:
        temp = dn[region_spliters[i-1]:]
        temp.sort(key=lambda x:float(x[6]))
        d_temp.append(temp)
dn = []
for i in d_temp:
    dn = dn + i

# STEP 6
# output to CSV format
dn.insert(0,head)
with open('worldbank_output_chongli.csv', 'wb') as outcsv:
    wr = csv.writer(outcsv,delimiter=',',quoting=csv.QUOTE_MINIMAL)
    for i in range(len(dn)):
        wr.writerow(dn[i])
