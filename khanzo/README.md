# KHANZO scout : searcher
![khanzo](../_/img/khanzo.jpg)

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

Imagine a scenario where you are generating 100 errors per second,
any conventional log store starts being delayed because it has O(n)
where N is the number of events, but this is not the case here,
because we just seek to near the end.

You can parameterize the queries to specify if you want to start from
the beginning (`scan_max_documents: -1` in the query), also things can
be faster if you decide not to decode the metadata in the hit.


# context

event stream
```
[
   {created: "Sun 14 Jul 21:19:58", user: 5, book: 10, event: "click"},
   {created: "Sun 14 Jul 21:20:55", user: 5, book: 10, event: "click"},
   {created: "Sun 14 Jul 22:41:02", user: 5, book: 10, event: "book"}
[   

```

context stream

```
[
   {created: "Sun 14 Jul 21:28:55", author:60, name: "jrr tolkien",... }
   {created: "Sun 14 Jul 21:29:55", book:10, name: "lotr", author: 60,... }
   {created: "Sun 14 Jul 21:30:55", user:5, name: "jack",... }
]
```

In this example, the context for `user:5` is created at `"Sun 14 Jul
21:30:55"`, and will be visible to all events after that (keep in mind
you can also insert context in the past).

Khanzo will automatically and recursively join the context with the
event stream. So for the event `{created: "Sun 14 Jul 22:41:02", user:
5, book: 10, event: "book"}`, we will lookup `book:10` from the
context and if found it will lookup from the book's properties and try
to find accessible context for them, so it will lookup for `author:
60` and join with the author.


# searching

```
% curl -d '{
  "size": 1,
  "scan_max_documents": -1,
  "query": {
    "or": [
      {
        "tag": {
          "key": "user_id",
          "value": "717f780d067d4abf95b28e013f4570c1"
        }
      }
    ]
  },
  "decode_metadata": true
}' http://khanzo/search/json

{
  "hits": [
    {
      "context": [
        {
          "created_at_ns": 1563131082772484400,
          "foreign_id": "3f40a264710d4874aadc2ba88b3ed0d3",
          "foreign_type": "author_id",
          "properties": [
            {
              "key": "date_of_birth",
              "value": "1974-11-29"
            },
            {
              "key": "name",
              "value": "quidem"
            }
          ]
        },
        {
          "created_at_ns": 1563131082772820500,
          "foreign_id": "d2ab0acd480c46b2920fed7f8db9af5f",
          "foreign_type": "book_id",
          "properties": [
            {
              "key": "author_id",
              "value": "3f40a264710d4874aadc2ba88b3ed0d3"
            },
            {
              "key": "genre",
              "value": "ea"
            },
            {
              "key": "name",
              "value": "velit"
            },
            {
              "key": "published_at",
              "value": "1970-03-20"
            }
          ]
        }
      ],
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
    {
      "context": [
        {
          "created_at_ns": 1563131082772584000,
          "foreign_id": "772e1c891915440288565a861bd5ce7a",
          "foreign_type": "author_id",
          "properties": [
            {
              "key": "date_of_birth",
              "value": "1980-08-20"
            },
            {
              "key": "name",
              "value": "labore"
            }
          ]
        },
        {
          "created_at_ns": 1563131082772648200,
          "foreign_id": "f1f503b783b345faacdcdb5fdd0e8fee",
          "foreign_type": "author_id",
          "properties": [
            {
              "key": "date_of_birth",
              "value": "2000-03-21"
            },
            {
              "key": "name",
              "value": "qui"
            }
          ]
        },
        ...
  ],
  "total": 2045
}


```

# scan

http://khanzo/scan/html/ (or /scan/text/ for text), example output of /scan/text/ (see full output on [baxx.dev](https://baxx.dev/s/3b50e58d-b0e5-43ae-bf23-45cd002708c7))


```
┌                                                                              ┐
│ FOREIGN......................................................................│
└                                                                              ┘
« user_id » total: 10000, 100.00%
    dfe5992d20004791be066b0dc67558a1     2045  20.45% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    061b3b7bd2034fbfb1ce9454bf1f0570     2035  20.35% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    793a2482e79a4b9285c37515df732846      793   7.93% ▒▒▒▒▒▒▒▒▒▒
    920aa39eaf74430db07ba740abd8a4f7      756   7.56% ▒▒▒▒▒▒▒▒▒
    555c32def93a4ae1a7058addb76c903b      752   7.52% ▒▒▒▒▒▒▒▒▒
    5b259853f3344fab9db3995c0096bd18      291   2.91% ▒▒▒
    8264196f234b485c9ad8e1228def3025      283   2.83% ▒▒▒
    e86bd65482134521a653426d898a4d3c      270   2.70% ▒▒▒
    7b74ea43443d42e087eec9f82ce596cb      267   2.67% ▒▒▒
    cfcdd5c7822745a4a52ba9651e4b4e85      247   2.47% ▒▒▒
    3813413352444366aabbdbc16a0aaa16       38   0.38% 
    4ac30af5a6204b04b6298731ba67c1bc       36   0.36% 
    [ ... cut ]
┌                                                                              ┐
│ EVENT_TYPES..................................................................│
└                                                                              ┘
« event_type » total: 10000, 100.00%
    ignore     3399  33.99% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    click      3318  33.18% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    skip       2194  21.94% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    buy        1089  10.89% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
┌                                                                              ┐
│ TAGS.........................................................................│
└                                                                              ┘
« book_id » total: 15155, 100.00%
    5634976e4d8942e8b51f8c97f559897f     1416   9.34% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    14273466cb2b47a68c185d8ea04c462a     1386   9.15% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    bee8f865406248e38335e5d02dbbd1e9     1362   8.99% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    7f68113ab1e448dfa7c7ec63cc29096d     1335   8.81% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    5fa70292d42c416f8ec968d1fdaba9fd     1333   8.80% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    f3eefb5a3fd549f4adcb0646627aa377      652   4.30% ▒▒▒▒▒▒▒▒▒▒▒
    1a6cb1e184354458aff8e53e5b31f3f1      645   4.26% ▒▒▒▒▒▒▒▒▒▒▒
    317660a79cce4d669ad39bb3b9011c0d      614   4.05% ▒▒▒▒▒▒▒▒▒▒▒
    d2ab0acd480c46b2920fed7f8db9af5f      594   3.92% ▒▒▒▒▒▒▒▒▒▒
    e791ac1cba6b4f259bbf6c9896c27636      584   3.85% ▒▒▒▒▒▒▒▒▒▒
    7a25c156c20c4037a4b7fa1a488e80ff      251   1.66% ▒▒▒▒
    15d7da551b29476882a05bac2cf37781      231   1.52% ▒▒▒▒
    [ ... cut ]

┌                                                                              ┐
│ PROPERTIES...................................................................│
└                                                                              ┘
« currency » total: 10000, 33.33%
    AZN       84   0.84% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    HNL       78   0.78% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    TJS       76   0.76% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    UYU       76   0.76% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    VND       74   0.74% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    PEN       73   0.73% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    FJD       72   0.72% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    KMF       71   0.71% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ZMW       71   0.71% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    [ ... cut ]
--------

« timezone » total: 10000, 33.33%
    America/Halifax                        30   0.30% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    America/Mendoza                        29   0.29% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Asia/Novokuznetsk                      28   0.28% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    US/Indiana-Starke                      28   0.28% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Asia/Aqtobe                            27   0.27% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Africa/Mbabane                         27   0.27% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Australia/North                        27   0.27% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    PRC                                    27   0.27% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    America/Detroit                        27   0.27% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Europe/Sofia                           26   0.26% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    [ ... cut ]
--------

« user_agent » total: 10000, 33.33%
    Netvibes (http://www.netvibes.com)          130   1.30% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Moreoverbot/5.1 (+http://w.moreover...      127   1.27% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    trunk.ly spider contact@trunk.ly            125   1.25% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    MLBot (www.metadatalabs.com/mlbot)          124   1.24% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Mozilla/5.0 (compatible; TweetedTim...      120   1.20% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Owlin.com/1.3 (http://owlin.com/)           120   1.20% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    [ ... cut ]
┌                                                                              ┐
│ SAMPLE.......................................................................│
└                                                                              ┘
user_id:69c6d8c30c294fef96aaa2b92d30ad2d
type:click
Sun Jul 14 21:04:42 CEST 2019
  book_id                       : 15d7da551b29476882a05bac2cf37781
  book_id                       : 5871f4ee144b4db7ace6d1a72ea27912
  book_id                       : 70b6ef5ccb80410aa9137a88729dd657
  currency                      : XUA
  timezone                      : America/Grand_Turk
  user_agent                    : Trapit/1.1
  @author_id:3f40a264710d4874aadc2ba88b3ed0d3:
    date_of_birth               : 1974-11-29
    name                        : quidem
  @author_id:652e781362ab4a7b8b3d056b900b05f5:
    date_of_birth               : 1989-06-09
    name                        : odio
  @author_id:6a01fb7c04054652ab8f9dc3dbf8a53b:
    date_of_birth               : 1972-11-07
    name                        : adipisci
  @author_id:d220fb93899c40c2affd877b918529d6:
    date_of_birth               : 2005-05-02
    name                        : cumque
  @book_id:15d7da551b29476882a05bac2cf37781:
    author_id                   : d220fb93899c40c2affd877b918529d6
    genre                       : beatae
    name                        : totam
    published_at                : 2010-08-28
  @book_id:5871f4ee144b4db7ace6d1a72ea27912:
    author_id                   : 6a01fb7c04054652ab8f9dc3dbf8a53b
    genre                       : quis
    name                        : eos
    published_at                : 1977-08-01
  @book_id:70b6ef5ccb80410aa9137a88729dd657:
    author_id                   : 3f40a264710d4874aadc2ba88b3ed0d3
    author_id                   : 652e781362ab4a7b8b3d056b900b05f5
    genre                       : suscipit
    name                        : consequatur
    published_at                : 2000-12-19


user_id:061b3b7bd2034fbfb1ce9454bf1f0570
type:ignore
Sun Jul 14 21:04:42 CEST 2019
  book_id                       : 14273466cb2b47a68c185d8ea04c462a
  book_id                       : 5fa70292d42c416f8ec968d1fdaba9fd
  currency                      : SSP
  timezone                      : Pacific/Auckland
  user_agent                    : ia_archiver (+http://www.alexa.com/site/help/webmasters; crawler@alexa.com)
  @book_id:14273466cb2b47a68c185d8ea04c462a:
    genre                       : adipisci
    name                        : sit
    published_at                : 2003-06-23
  @book_id:5fa70292d42c416f8ec968d1fdaba9fd:
    genre                       : necessitatibus
    name                        : eveniet
    published_at                : 2014-06-20


  [ ...cut ]

```


# favicon

* <div>Made by <a href="https://www.freepik.com/" title="Freepik">Freepik</a> from <a href="https://www.flaticon.com/"                 title="Flaticon">www.flaticon.com</a> is licensed by <a href="http://creativecommons.org/licenses/by/3.0/"                 title="Creative Commons BY 3.0" target="_blank">CC 3.0 BY</a></div>


# vowpal wabbit fun

```

% curl -d '{
  "size": 1000,
  "query": {
        "tag": {
          "key": "user_id",
          "value": "717f780d067d4abf95b28e013f4570c1"
        }
   }
  },
  "decode_metadata": true
}' 'http://localhost:9002/search/vw?label_0=click&label_1=buy'

```

this will take the hits and return a vw friendly output such as
```

0 |user_id 717f780d067d4abf95b28e013f4570c1 |book_id f4936e9dff4449f482330ee4b8b7cb3e |currency PAB |timezone Asia_Qatar
1 |user_id 717f780d067d4abf95b28e013f4570c1 |book_id f4936e9dff4449f482330ee4b8b7cb3e |currency RWF |timezone Australia_Lindeman

```

so just train and have fun haha

```

curl -s -d '{
  "size": 1000,
  "query": {
    "or": [
      {
        "tag": {
          "key": "user_id",
          "value": "717f780d067d4abf95b28e013f4570c1"
        }
      }
    ]
  },
  "decode_metadata": true
}' 'http://localhost:9002/search/vw?label_0=click&label_1=buy' | vw -f model.bin

...
final_regressor = model.bin
Num weight bits = 18
learning rate = 0.5
initial_t = 0
power_t = 0.5
using no cache
Reading datafile = 
num sources = 1
average  since         example        example  current  current  current
loss     last          counter         weight    label  predict features
0.000000 0.000000            1            1.0   0.0000   0.0000       11
0.500000 1.000000            2            2.0   1.0000   0.0000       10
0.462168 0.424336            4            4.0   0.0000   0.3757        8
0.414165 0.366163            8            8.0   0.0000   0.2263        5
0.338320 0.262474           16           16.0   1.0000   0.1924        6
0.303402 0.268484           32           32.0   0.0000   0.1746        8
0.243563 0.183725           64           64.0   1.0000   0.2051        5
0.237753 0.231942          128          128.0   0.0000   0.2672        6
0.239558 0.241364          256          256.0   1.0000   0.3225        9

finished run
number of examples = 450
weighted example sum = 450.000000
weighted label sum = 129.000000
average loss = 0.242226
best constant = 0.286667
best constant's loss = 0.204489
total feature number = 3592

```

then get more results and test

```
% curl -s -d '{
  "size": 100000,
  "query": {
    "or": [
      {
        "tag": {
          "key": "user_id",
          "value": "717f780d067d4abf95b28e013f4570c1"
        }
      }
    ]
  },
  "decode_metadata": true
}' 'http://localhost:9002/search/vw?label_0=click&label_1=buy' | vw -i model.bin -t -p pred.txt

predictions = pred.txt
Num weight bits = 18
learning rate = 0.5
initial_t = 0
power_t = 0.5
using no cache
Reading datafile = 
num sources = 1
average  since         example        example  current  current  current
loss     last          counter         weight    label  predict features
0.047196 0.047196            1            1.0   0.0000   0.2172       11
0.129045 0.210894            2            2.0   1.0000   0.5408       10
0.162038 0.195031            4            4.0   0.0000   0.2456        8
0.150880 0.139722            8            8.0   0.0000   0.0972        5
0.143162 0.135444           16           16.0   1.0000   0.5189        6
0.166099 0.189036           32           32.0   0.0000   0.5005        8
0.142567 0.119035           64           64.0   1.0000   0.6230        5
0.123137 0.103707          128          128.0   0.0000   0.0976        6
0.128413 0.133689          256          256.0   1.0000   0.0646        9
0.127537 0.126660          512          512.0   0.0000   0.0662        7
0.177952 0.228366         1024         1024.0   0.0000   0.1812        5
0.206171 0.234391         2048         2048.0   0.0000   0.0615        8
0.215887 0.225602         4096         4096.0   0.0000   0.2154        6
0.220699 0.225512         8192         8192.0   1.0000   0.3293       11

finished run
number of examples = 9101
weighted example sum = 9101.000000
weighted label sum = 2347.000000
average loss = 0.221114
best constant = 0.257884
best constant's loss = 0.191380
total feature number = 73008


% head -10 pred.txt       
0.217246
0.540768
0.425754
0.245566
0.267821
0.041510
0.107864
0.097188
0
0.062993
```

fun times