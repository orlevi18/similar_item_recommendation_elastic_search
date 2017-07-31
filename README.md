# Similar Item Recommendation Using elasticsearch

Given an item, find K items from same category with price difference of less than 20%* and highest title similarity, calculated using cosine and Okapi BM25 with bigrams (shingles). In other words, category and price range are used for match set (hard constraint) and title similarity used for ranking the match set.

Consisting of 2 Steps:

Bulkindex - Creates the index of listings for similarity search. Using Okapi BM25 and bi-grams.
1. Building the index and defining its structure
2. Reading file with data to index
3. Indexing batches of 10k items

Search Listing - Finds similar items for a given test item(s) as per the above

![alt text](https://github.com/orlevi18/similar_item_recommendation_elastic_search/some_examples.png?raw=true)

# dependcies
elasticsearch - see here how to install on windows: https://www.youtube.com/watch?v=l6wXE5SBJ_A
python elasticsearch - https://elasticsearch-py.readthedocs.io/en/master/
