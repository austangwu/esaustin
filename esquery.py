from elasticsearch import Elasticsearch
import csv
from datetime import date

es = Elasticsearch()

def commonsurnames():
    names = open("surnames.txt", "r").read().splitlines()
    namequery = [];
    for name in names:
        namequery.append({ "match": { "Provider_Last_Name_(Legal_Name)": name }})
    return namequery

doc = {
    "query":{
        "bool":{
            "must":[
                {"match":{"Provider_Business_Mailing_Address_State_Name":"TX"}},
                {"bool":{"should":commonsurnames()}}
            ]
        }
    }
}

res = es.search(index="doctors", doc_type="type1", body=doc, search_type='scan', scroll='60s')
hits = []
scroll_size = res['hits']['total']
while (scroll_size > 0):
    try:
        scroll_id = res['_scroll_id']
        res = es.scroll(scroll_id=scroll_id, scroll='60s')
        hits += res['hits']['hits']
        scroll_size = len(res['hits']['hits'])
    except:
        break

#write output to results.csv
headings = open("headings.txt", "r").read().splitlines()
with open('results_%.csv' % date.today(), 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=headings)
    writer.writeheader()
    for hit in hits:
       writer.writerow(hit['_source'])

