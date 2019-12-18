package main

const esIndex = "twitter"
const esType = "item"

const esMapping = `
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "item": {
      "properties": {
        "name": {
          "type": "keyword"
        },
        "city": {
          "type": "keyword"
        },
        "gh5": {
          "type": "keyword"
        },
        "gh6": {
          "type": "keyword"
        },
        "gh7": {
          "type": "keyword"
        },
        "match_all": {
          "type": "keyword"
        }
      }
    }
  }
}`
