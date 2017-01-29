import re
from collections import Counter
f=open('access_log.txt','r')
valid = []
invalid = []
for line in f:
    if re.match(r'.*"(GET|POST)\s',line) != None and \
        re.match(r'.*"\s200\s',line) != None and \
        re.match(r'.*\s(http|https)://[a-zA-Z]+',line) != None and \
        re.match(r'.*\s(http|https)://([a-zA-Z0-9_-]+\.)([a-zA-Z0-9_-]+\.|())[a-zA-Z]+',line) != None:
        valid.append(line)
    else:
        invalid.append(line)

invalid_f = open('invalid_access_log_chongli.txt','w')
for i in invalid:
    invalid_f.write(i)
valid_dic={}
valid_time = ['09/Mar/2004','10/Mar/2004','11/Mar/2004','12/Mar/2004','13/Mar/2004','14/Mar/2004']
for time in valid_time:
    valid_dic[time]=[]
for line in valid:
    time = re.findall(r'\[([0-9]+/[a-zA-Z]+/[0-9]+):',line)[0]
    domain = re.findall(r'(http|https)://(.*?)\.([a-zA-Z]+)(/|:|"|\s)',line)[0][-2].lower()
    valid_dic[time].append(domain)
for key in valid_dic.keys():
    valid_dic[key]=Counter(valid_dic[key])

f2 = open('valid_log_summary_chongli.txt','w')
for key in valid_time:
    f2.write(key+'\t')
    s = valid_dic[key].keys()
    s.sort()
    for domain in s[:-1]:
        f2.write(domain + ':' + str(valid_dic[key][domain]) + '\t')
    f2.write(s[-1] + ':' + str(valid_dic[key][s[-1]]) + '\n')
