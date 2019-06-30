* search
curl -d '{
  "partition": 1,
  "query": {
    "or": [
      {
        "tag": {
          "key": "type",
          "value": "example"
        }
      },
      {
        "tag": {
          "key": "app_open",
          "value": "true"
        }
      }
    ]
  }
}' localhost:9002/search

* get
curl localhost:9002/get/:partition/:offset