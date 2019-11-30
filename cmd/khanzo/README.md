# KHANZO scout : searcher
![khanzo](../../assets/khanzo.jpg)

# [LORE](https://wow.gamepedia.com/Khanzo)

Khanzo was an orc blademaster of the Blackrock clan, and commander of
the Blackrock Scouts, who guarded a demon gate in the Alterac
Mountains of Lordaeron. Arthas and the lich Kel'Thuzad killed him
along with four other orcs, in order for Kel'Thuzad to commune with
the demon lord Archimonde.

# speed

By default khanzo picks last 100k documents by simply seeking the
inverted index file to size(file) - (100_000 * 8), and for scanning it
seeks the forward file (event log) to size(file) - 50mb, this allow
all basic operations to be semi constant.

Imagine a scenario where you are generating 100 errors per second, any
conventional log store starts being delayed because it has O(n)
somewhere, where N is the number of events(oversimplification of
course.  could be in log tree merges or sst compactions), but this is
not the case here, because we just seek to near the end.

You can parameterize the queries to specify if you want to start from
the beginning (`scan_max_documents: -1` in the query), also things can
be faster if you decide not to decode the metadata in the hit.


# searching

```
% curl -d '{
  "query": {
    "type": "DISMAX",
    "queries": [
      {
        "key": "product",
        "value": "amazon_com"
      },
      {
        "key": "env",
        "value": "live",
        "boost": 2
      }
    ]
  },
  "limit": 1
}' -H 'Content-Type: application/json' localhost:9002/api/v0/search

{
  "hits": [
    {
      "foreign_id": "dfe5992d20004791be066b0dc67558a1",
      "foreign_type": "user_id",
      "id": 8394,
      "kafka": {
        "offset": 37,
        "partition": 3
      },
      "metadata": {
        "created_at_ns": 1563131082828701400,
        "event_type": "skip",
        "foreign_id": "dfe5992d20004791be066b0dc67558a1",
        "foreign_type": "user_id",
        "properties": [
          {
            "key": "currency",
            "value": "UAH"
          },
          {
            "key": "timezone",
            "value": "America/Cayenne"
          },
          {
            "key": "user_agent",
            "value": "urlfan-bot/1.0; +http://www.urlfan.com/site/bot/350.html"
          }
        ],
        "tags": [
          {
            "key": "book_id",
            "value": "d2ab0acd480c46b2920fed7f8db9af5f"
          }
        ]
      },
      "score": 1
    },
     ...
  ],
  "total": 2045
}


# fetching many (one event per line)
size:0 means unlimited

```
% curl -d '{
  "query": {
    "type": "DISMAX",
    "queries": [
      {
        "key": "product",
        "value": "amazon_com"
      },
      {
        "key": "env",
        "value": "live",
        "boost": 2
      }
    ]
  },
  "limit": 1
}' -H 'Content-Type: application/json' localhost:9002/api/v0/fetch
```


# favicon

* <div>Made by <a href="https://www.freepik.com/" title="Freepik">Freepik</a> from <a href="https://www.flaticon.com/"                 title="Flaticon">www.flaticon.com</a> is licensed by <a href="http://creativecommons.org/licenses/by/3.0/"                 title="Creative Commons BY 3.0" target="_blank">CC 3.0 BY</a></div>


