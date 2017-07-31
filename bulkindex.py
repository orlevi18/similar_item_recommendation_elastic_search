from elasticsearch import Elasticsearch
es = Elasticsearch()

# build index
INDEX_NAME="za_sample"
TYPE_NAME="listing"
ID_FIELD="id"

mapping = {
    "settings": {
        "index": {
          "similarity": {
            "default": {
              "type" : "BM25",
            }
          }
        },
		"analysis": {
		  "filter": {
			"my_shingle_filter": {
                    "type":	"shingle",
                    "output_unigrams":  False

            },
		  },
		  "analyzer": {
			"my_shingle_analyzer": {
			  "type":	"custom",
			  "tokenizer":	"standard",
			  "filter": [
				"lowercase",
				"my_shingle_filter",
			  ]
			}
		  }
		}	
		
      },
    "mappings": {
        "listing": {
          "properties": {
            "title": {
				"type":     "text",
				"fields": {
						"shingles": {
							"type":     "text",
							"analyzer": "my_shingle_analyzer"
						}
					}
			},
		    "category": {
				"type":     "keyword",
			},
			"price": {
				"type":     "double",
			},
		}
    }
  }
}

if es.indices.exists(INDEX_NAME):
    print("deleting '%s' index..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    
res = es.indices.create(index=INDEX_NAME, body=mapping)
print(" response: '%s'" % (res))

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

dumpfile=codecs.open("za_sample_listings_incl_cat_no_desc.csv","r",encoding='utf=8')
csvfile= unicode_csv_reader(dumpfile)

# index in batches
bulk_data = [] 
i=1
listings_failed_to_index=open("listings_failed_to_index.txt","w")
for line in csvfile:
    try:
        str_line=",".join(line)
        uni_line=str_line.decode('utf-8')
        data_dict = {}
        data_dict['id']=line[1]
        data_dict['title']=line[3].lower()
        data_dict['category']=line[5]
        if line[4]=='' or line[4]=='Unknown':
            data_dict['price']=''
        else:
            data_dict['price']=float(line[4])
        op_dict = {
            "index": {
                "_index": INDEX_NAME, 
                "_type": TYPE_NAME, 
                "_id": data_dict[ID_FIELD]
            }
        }
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)
    except Exception as e:
        listings_failed_to_index.write(line[1]+"\n")
    if i%10000==0:
        print("bulk index "+str(i))
        res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
        bulk_data = []
    i=i+1

print("bulk index "+str(i))
res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)

dumpfile.close()
listings_failed_to_index.close()
