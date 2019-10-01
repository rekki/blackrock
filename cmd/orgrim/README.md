# ORGRIM DOOMHAMMER  warchief : leader

![orgrim](../../assets/orgrim.jpg)

> “I rule the Horde now, Gul'dan. Not you, not your
>  warlocks. Doomhammer alone. And there will be no more dishonor. No
>  more treachery. No more deceit and lies!”


# [LORE](https://wow.gamepedia.com/Orgrim_Doomhammer)

Orgrim Doomhammer was the Warchief of the Old Horde and Chieftain of
the Blackrock clan during the end of the First War and the entirety of
the Second War. The orcish capital of Orgrimmar, the Horde-controlled
town of Hammerfall in the Arathi Highlands, and the flying battleship
Orgrim's Hammer patrolling the skies of Icecrown, are named in his
honor. He was also known as the Backstabber by loyalists of Gul'dan
and Blackhand.


# consume events and context



```
curl -d '{
  "created_at_ns": 1,
  "foreign_id": "england",
  "foreign_type": "region",
  "properties": [
    {
      "key": "hello",
      "value": "world"
    },
    {
      "key": "brave",
      "value": "world"
    }
  ]
}' http://orgrim/push/context

```

all query params are tags in the form of key:value, this creates protobuf message with the following schema:

```
message KV {
        string key = 1;
        string value = 2;
}

message Metadata {
        repeated KV tags = 1;
        repeated KV properties = 2;
        int64 created_at_ns  = 5;
        string event_type = 7;

        string foreign_id = 9;
        string foreign_type = 10;
}

message Context {
        repeated KV properties = 2;
        int64 created_at_ns  = 5;

        string foreign_id = 6;
        string foreign_type = 7;
}

message Envelope {
        Metadata metadata = 1;
        bytes payload = 2;
}

```
