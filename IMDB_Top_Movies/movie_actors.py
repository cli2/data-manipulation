# -*- coding: utf-8 -*-
import urllib2, re, json, pydot, itertools, time, sys
from bs4 import BeautifulSoup
from collections import OrderedDict
reload(sys)
sys.setdefaultencoding('utf-8')

# step1
response = urllib2.urlopen('http://www.imdb.com/search/title?at=0&sort=num_votes&count=100')
html = response.read()
print type(html)
html = html.decode('utf-8')
print type(html)
html = html.encode('utf-8')
print type(html)
with open('step1.html','w') as f:
    f.write(html)

# # step2
soup = BeautifulSoup(html,'lxml')
h3 = soup.find_all('h3',{'class':"lister-item-header"})
l_imdb = []
with open('step2.txt','w') as f:
    f.write('IMDB_ID' + '\t' + 'Rank' + '\t' + 'Title' + '\n')
    for i in range(len(h3)):
        imdbid = re.findall(u'/title/([a-zA-Z0-9]*)/', h3[i].find_all('a')[0]['href'])[0]
        rank = h3[i].find_all('span',{'class':'lister-item-index'})[0].string[:-1]
        name = h3[i].find_all('a')[0].string
        print name
        name = name.decode('utf-8')
        name = name.encode('utf-8')
        l_imdb.append(imdbid)
        f.write(imdbid + '\t' + rank + '\t' + name + '\n')

# step3
# with open('step3.txt','w') as f3:
#     for i in range(len(l_imdb)):
#         response = urllib2.urlopen('http://www.omdbapi.com/?i='+l_imdb[i]).read()
#         f3.write(response + '\n')
#         time.sleep(5)

# step4
# with open('step3.txt','r') as f4:
#     content = f4.readlines()
# with open('step4.txt','w') as f4:
#     l = []
#     for line in content:
#         actors = []
#         j = json.loads(line)
#         title = j['Title']
#         actor_l = j['Actors'].split(',')
#         for i in range(min(len(actor_l),5)):
#             actors.append(actor_l[i].strip())
#         d = OrderedDict()
#         d['Title'] = title
#         d['Actors']=actors
#         l.append(d)
#     f4.write(json.dumps(l))

# step5
# graph = pydot.Dot(graph_type='graph',charset="utf8")
# with open('step4.txt','r') as f5:
#     content = json.loads(f5.read())
#     for i in range(len(content)):
#         actors = content[i]['Actors']
#         combinations = list(itertools.combinations(actors,2))
#         for combination in combinations:
#             edge = pydot.Edge(combination[0].strip(),combination[1].strip())
#             graph.add_edge(edge)
#     graph.write('actors_graph.dot')
