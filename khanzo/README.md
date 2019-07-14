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
      "foreign_id": "717f780d067d4abf95b28e013f4570c1",
      "foreign_type": "user_id",
      "id": 91799307,
      "kafka": {
        "offset": 104244,
        "partition": 3
      },
      "metadata": {
        "created_at_ns": 1563113016592233200,
        "event_type": "ignore",
        "foreign_id": "717f780d067d4abf95b28e013f4570c1",
        "foreign_type": "717f780d067d4abf95b28e013f4570c1",
        "properties": [
          {
            "key": "currency",
            "value": "EUR"
          },
          {
            "key": "timezone",
            "value": "Antarctica/Casey"
          },
          {
            "key": "user_agent",
            "value": "Aghaven/Nutch-1.2 (www.aghaven.com)"
          }
        ],
        "tags": [
          {
            "key": "book_id",
            "value": "439c3bfbfcc04dab9eceb65a4effa039"
          },
          {
            "key": "book_id",
            "value": "a29b2a883e10474894399acfcd6c61a9"
          },
          {
            "key": "book_id",
            "value": "c1d8326a4db945769d4fe2185019f7dc"
          },
          {
            "key": "book_id",
            "value": "e4d1cc5be1b343e5934687cd12b82bcd"
          },
          {
            "key": "book_id",
            "value": "f4936e9dff4449f482330ee4b8b7cb3e"
          }
        ]
      },
      "score": 1
    }
  ],
  "total": 20259
}


```

# scan

http://khanzo/scan/html/ (or /scan/text/ for text), example output of /scan/text/ (see full output on [baxx.dev](https://baxx.dev/s/e659718f-9eec-45b5-92f7-2be5fb4a46ad))

```

┌                                                                              ┐
│ FOREIGN......................................................................│
└                                                                              ┘
« user_id » total: 34822, 100.00%
    717f780d067d4abf95b28e013f4570c1     7122  20.45% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ad5e3dc7da9a48a1883470ea0d34489a     6991  20.08% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    06fed53b590c43198e0b4bc5c8abc229     2763   7.93% ▒▒▒▒▒▒▒▒▒▒
    cc7aa0b55e0a46b399162b9cbc105ccc     2663   7.65% ▒▒▒▒▒▒▒▒▒
    a94c4d4595d74bdd8006c54a87e16e7b     2656   7.63% ▒▒▒▒▒▒▒▒▒
    3d0e080b1d254e69838afa7472ec0ce0     1005   2.89% ▒▒▒
    c93ab736518d4945a0f5a271324d6ffe      977   2.81% ▒▒▒
    ... [ cut ]
┌                                                                              ┐
│ EVENT_TYPES..................................................................│
└                                                                              ┘
« event_type » total: 34822, 100.00%
    click     11668  33.51% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ignore    11482  32.97% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    skip       7719  22.17% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    buy        3953  11.35% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
┌                                                                              ┐
│ TAGS.........................................................................│
└                                                                              ┘
« book_id » total: 105256, 100.00%
    ea647026d57543d999f972457e180aa5     9581   9.10% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    c99129fd6dcf488f9bd5b0b4b36f0b33     9514   9.04% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    9b1283fd81bb4bd3b8b4cb5432ad246a     9512   9.04% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    f4936e9dff4449f482330ee4b8b7cb3e     9488   9.01% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    2329dfa714d745a1b5b39dabe51f5065     9414   8.94% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    0e223cede80c4a7aa2c1285c4d060c72     4281   4.07% ▒▒▒▒▒▒▒▒▒▒▒
    4ca3126219e64b6b9840e6092edf38f7     4265   4.05% ▒▒▒▒▒▒▒▒▒▒▒
    a29b2a883e10474894399acfcd6c61a9     4206   4.00% ▒▒▒▒▒▒▒▒▒▒▒
    737e62b2fc3a45fe934e08775b1ac925     4135   3.93% ▒▒▒▒▒▒▒▒▒▒▒
    e4d4bbfa3c8f487f858c81d4a7429a73     4029   3.83% ▒▒▒▒▒▒▒▒▒▒
    f0369680c9f34369b50602c6bcbcd06c     1643   1.56% ▒▒▒▒
    439c3bfbfcc04dab9eceb65a4effa039     1612   1.53% ▒▒▒▒
    c1d8326a4db945769d4fe2185019f7dc     1605   1.52% ▒▒▒▒
    b3377bf4744d4f9d9da19a7dc609a7d4     1595   1.52% ▒▒▒▒
    ... [ cut ]
┌                                                                              ┐
│ PROPERTIES...................................................................│
└                                                                              ┘
« currency » total: 34822, 33.33%
    MMK      236   0.68% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    NGN      230   0.66% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    VES      229   0.66% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    LKR      228   0.65% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    KMF      227   0.65% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    SOS      225   0.65% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    IQD      221   0.63% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    LYD      220   0.63% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    AED      216   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ALL      216   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    PGK      216   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ... [ cut ]

--------

« timezone » total: 34822, 33.33%
    Pacific/Kwajalein                      88   0.25% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    US/East-Indiana                        86   0.25% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Etc/GMT-13                             84   0.24% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Africa/Nairobi                         82   0.24% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Europe/Prague                          81   0.23% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Pacific/Kiritimati                     81   0.23% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ... [ cut ]
--------

« user_agent » total: 34822, 33.33%
    UnwindFetchor/1.0 (+http://www.gnip...      423   1.21% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Mozilla/5.0 (compatible; PaperLiBot...      419   1.20% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Voyager/1.0                                 390   1.12% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Mozilla/5.0 (compatible; MSIE 6.0b;...      389   1.12% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Mozilla/5.0 (compatible; woriobot s...      389   1.12% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Summify (Summify/1.0.1; +http://sum...      389   1.12% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    SeznamBot/3.0 (+http://fulltext.sbl...      388   1.11% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    PostPost/1.0 (+http://postpo.st/cra...      386   1.11% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ssearch_bot (sSearch Crawler; http:...      386   1.11% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    wikiwix-bot-3.0                             386   1.11% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    radian6_default_(www.radian6.com/cr...      384   1.10% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Trapit/1.1                                  383   1.10% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Mozilla/5.0 (compatible; AhrefsBot/...      379   1.09% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Mozilla/5.0 (compatible; PrintfulBo...      379   1.09% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    Mozilla/4.0 (compatible; www.euro-d...      378   1.09% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ... [ cut ]
┌                                                                              ┐
│ SAMPLE.......................................................................│
└                                                                              ┘
ad5e3dc7da9a48a1883470ea0d34489a:ad5e3dc7da9a48a1883470ea0d34489a
type:skip
Sun Jul 14 16:03:43 CEST 2019
  book_id                       : a29b2a883e10474894399acfcd6c61a9
  currency                      : GEL
  timezone                      : Pacific/Truk
  user_agent                    : Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)


717f780d067d4abf95b28e013f4570c1:717f780d067d4abf95b28e013f4570c1
type:ignore
Sun Jul 14 16:03:43 CEST 2019
  book_id                       : 21a5f69718784eeb97a86deb0a15db98
  book_id                       : 4d107d12348c450abd28c59d14f9f57e
  book_id                       : 6fb9a161af4142c58b9d26d860ff2f51
  book_id                       : 942cb675fbcb4d7fbb4cd37ff6bd57dc
  book_id                       : d2885d6001e54083814e182e1fd0a949
  currency                      : CLF
  timezone                      : America/Mendoza
  user_agent                    : UnwindFetchor/1.0 (+http://www.gnip.com/)


717f780d067d4abf95b28e013f4570c1:717f780d067d4abf95b28e013f4570c1
type:click
Sun Jul 14 16:03:43 CEST 2019
  book_id                       : 9b1283fd81bb4bd3b8b4cb5432ad246a
  book_id                       : c99129fd6dcf488f9bd5b0b4b36f0b33
  book_id                       : c99129fd6dcf488f9bd5b0b4b36f0b33
  book_id                       : ea647026d57543d999f972457e180aa5
  book_id                       : f4936e9dff4449f482330ee4b8b7cb3e
  currency                      : MVR
  timezone                      : Atlantic/Cape_Verde
  user_agent                    : Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)

  ... [ cut ]

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