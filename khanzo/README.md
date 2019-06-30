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

```