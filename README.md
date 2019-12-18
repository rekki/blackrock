# WORK IN PROGRESS


```
* what is it
+ event log search engine
+ collective/queriable session
+ event sourced user session
+ not generic
+ business metrics and logs in same place
+ user features, alerts, and graphs

* how it works
+ how search works
+ how the storage works
+ lazy loading
+ protobuf
+ size
+ time locality
+ ...

* why it
+ search engines(lucene/xapian) are not optimized for scanning over *all* the matches
+ databases can not run code near your data(to this extend)
+ access to the forward index while scoring is complicated and slow (esp in lucene wrappers like elasticsearch)
+ simple ~2k lines of code
+ forkable
+ fast
+ pluggable [kafka store, redis store, ...]
+ not generic

* why not alternatives
+ lucene
+ xapian
+ bleve
+ postgres
+ mongo
+ cassandra
+ elasticsearch
+ solr
+ ...

* benchmarks (compare blackrock to lucene/es/postgres)
+ custom score 10_000_000 items
+ search top 10 items in 10_000_000
+ ...
```