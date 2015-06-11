from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import time
import csv

es = Elasticsearch()
filename = "total_data.csv"

def yieldsource():
    headings = open("headings.txt", "r").read().splitlines()

    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile, fieldnames = headings)
        for row in reader:
            data = {};
            for heading in headings:
                data.update({heading : row[heading]})
            yield {
            '_index': 'doctors',
            '_type': 'type1',
            '_id': row['NPI'],
            '_source': data
        }

start = time.time();
chunkcount = 1
chunksize = 1000
for success, data in streaming_bulk(es, yieldsource(), chunk_size=chunksize):
    if chunkcount % chunksize == 0:
        print("Docs Written: %d" % chunkcount)
    chunkcount += 1
end = time.time()
print("File name: %s" % filename)
print("Chunk size: %s" % chunksize)
print("Total time elapsed: %s" % str(end-start))