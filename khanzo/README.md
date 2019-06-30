# KHANZO scout : searcher
![khanzo](../_/img/khanzo.jpg)

# [LORE](https://wow.gamepedia.com/Khanzo)

Khanzo was an orc blademaster of the Blackrock clan, and commander of
the Blackrock Scouts, who guarded a demon gate in the Alterac
Mountains of Lordaeron. Arthas and the lich Kel'Thuzad killed him
along with four other orcs, in order for Kel'Thuzad to commune with
the demon lord Archimonde.


# searching

```
% curl -d '{
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
}' http://khanzo/search

```



# stats

get all possible tag keys and total number of documents in the index

```
% curl http://khanzo/stat
{
  "tags": [
    "_",
    "open",
    "type",
    "year",
    "year-month",
    "year-month-day",
    "year-month-day-hour",
    "year-month-day-hour-minute"
  ],
  "total_documents": 8
}

```

# introspect

get all values per tag with their term count

```
% curl http://khanzo/introspect/:tag
{
  "values": {
    "web": 8
    "app": 20,
  }
}
```