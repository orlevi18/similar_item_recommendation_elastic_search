from elasticsearch import Elasticsearch
es = Elasticsearch()

INDEX_NAME="za_sample"

# read csv file as unicode
import codecs
import csv

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, quotechar='"', delimiter=',',quoting=csv.QUOTE_ALL, skipinitialspace=True,**kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

testfile=codecs.open("test_listings.csv","r",encoding='utf=8')
csvfile= unicode_csv_reader(testfile)        
similar_listings = codecs.open("similar_listings.tsv","w",'utf-8')
pricerange=0.2

for line in csvfile:
    try:
        adid=line[1]
        adtitle=line[3].lower()
        if line[4]=='' or line[4]=='Unknown':
            adprice=''
        else:
            adprice=float(line[4])
        if adprice=='':
            adpricemin=-1
            adpricemax=100000000
        else:
            adpricemin=adprice*(1-pricerange)
            adpricemax=adprice*(1+pricerange)
            
        adcat=line[5]
        adidbody={
            "query": { 
                "bool": { 
                  "must": {
                    "multi_match" : {
                            "query" : adtitle,
                            "type" : "most_fields",
                            "fields": [ "title" ]
                        }
                  },
                  "filter": [{
                     "term":  {
                         "category": adcat
                         }
			},{
                     "range" : {
                         "price" : {
                             "gte" : adpricemin,
                             "lte" : adpricemax
                             }
                         }
                     }
				]
                  }
                }
              }
        print(str(adidbody)+"\n")
        adidhit = es.search(index=INDEX_NAME, size=5, body=adidbody)
        for hit in adidhit['hits']['hits']:
            simadid=hit["_source"]['id']
            simtitle=hit["_source"]['title']
            score=hit["_score"]
            if adid!=simadid:   # ignore the same item
                similar_listings.write(str(adid)+"\t"+str(simadid)+"\t"+str(score)+"\t"+str(hit["_source"])+"\n")
                print(str(adid)+"\t"+str(simadid)+"\t"+str(score)+"\t"+str(hit["_source"])+"\n")
    except Exception as e:
        print(e)

testfile.close()
similar_listings.close()

 
