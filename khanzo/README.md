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

Imagine a scenario where you are generating 100 errors per second, any
conventional log store starts being delayed because it has O(n)
somewhere, where N is the number of events(oversimplification of
course.  could be in log tree merges or sst compactions), but this is
not the case here, because we just seek to near the end.

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

![context](../_/img/context_thumb.jpg)

[full size image](https://github.com/rekki/blackrock/raw/master/_/img/context_full.jpg)

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

http://khanzo/scan/html/ (or /scan/text/ for text), example output of
/scan/text/ (see full output on
[baxx.dev](https://baxx.dev/s/72eb433e-4bae-4533-a0f3-86dcbca835aa))


```
┌                                                                              ┐
│ FOREIGN......................................................................│
└                                                                              ┘
« user_id » total: 32871, 100.00%
    e321afbe045f461c88405680262fe43c      703   2.14% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    fd63de2ccb84402594b5d2c3a25e947d      694   2.11% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    072f15a1e165450dbeaf176b0d7d3a44      693   2.11% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    41fa6c839818497fbecae43179eea84d      693   2.11% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    622141e66c9948c6a268ae89f264fafa      684   2.08% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    5bdbf2df87da47a5ad2a80c4223d0177      683   2.08% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ffd1094c0dc6492c9f2418bb1b4de8b6      680   2.07% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    fbabc404cb5f4eaab610d44116ba903f      671   2.04% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    31ebc9a054ce4292948c0163d2e0e810      666   2.03% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    75254c461acf432f87e759931387b60b      665   2.02% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    0077cfa3edf94bcbbdb6665bdb70f3b7      664   2.02% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    [ ... cut]
┌                                                                              ┐
│ EVENT_TYPES..................................................................│
└                                                                              ┘
« event_type » total: 32871, 100.00%
    click     10987  33.42% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ignore    10845  32.99% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    skip       7335  22.31% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    buy        3704  11.27% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
┌                                                                              ┐
│ SEARCH.......................................................................│
└                                                                              ┘
« book_id » total: 49628, 27.40%
    b8af7ff2ccf94e29a4812260d0031160      486   0.98% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    0711d4298dbb43399ee0cea3c27d191b      482   0.97% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ed0b204710ed4149bbea5fd5adf4cd04      481   0.97% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    de33bd63ae8840ea8edd5925468608cc      475   0.96% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    c9f0f1aaf7e443ebb58f2506fd510418      470   0.95% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    faa378523fda499587efadbe0f2af8f4      470   0.95% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    a0c115a15f454c5f83b4d8c6d756b3a5      468   0.94% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    35bd4ea5b4bc4adcb05985c23f5f60fa      466   0.94% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    b751ee28bcbb4c37a90999d9e5929f51      465   0.94% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    8f93d5594272486bbbd2bf82fda58c49      463   0.93% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    1ea820cf26c6425b8ece73505dd3ca16      460   0.93% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    e06638984a1d405cbbb0cabd3cfb8bc5      460   0.93% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    9b0e2d81178845808f8eb25cdc8f6469      459   0.92% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    d814d3f059594ab7854216de24e2de36      459   0.92% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    [ ... cut ]
┌                                                                              ┐
│ COUNT........................................................................│
└                                                                              ┘
« currency » total: 32871, 50.00%
    vuv      220   0.67% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    huf      214   0.65% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    gel      212   0.64% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    usd      211   0.64% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    tzs      210   0.64% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    xag      210   0.64% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    lbp      207   0.63% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    eur      206   0.63% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    svc      206   0.63% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    chf      205   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    sll      205   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    mru      204   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    idr      204   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    nok      204   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    sgd      203   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    nio      203   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    hnl      203   0.62% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    top      201   0.61% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    czk      200   0.61% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    ves      200   0.61% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    [ ... cut ]

--------

« timezone » total: 32871, 50.00%
    america_ensenada                       82   0.25% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    us_alaska                              82   0.25% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    indian_mayotte                         76   0.23% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    israel                                 76   0.23% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    asia_jakarta                           75   0.23% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    gb-eire                                75   0.23% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    america_porto_acre                     74   0.23% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    america_resolute                       74   0.23% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    asia_brunei                            74   0.23% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    etc_gmt                                74   0.23% ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
    america_guadeloupe
    [ ... cut ]

┌                                                                              ┐
│ SAMPLE.......................................................................│
└                                                                              ┘
user_id:75254c461acf432f87e759931387b60b
type:ignore
Fri Jul 19 13:58:11 CEST 2019
  book_id                       : 7b1ab10e58d046b38324d91d9002801b
  book_id                       : 7cfa0d83c12544be9de0ddfa658a7860
  book_id                       : fb2327cdd8f145c392f0c0a93d3d3fcd
  year                          : 2019
  year-month                    : 2019-07
  year-month-day                : 2019-07-19
  year-month-day-hour           : 2019-07-19-11
  currency                      : kwd
  timezone                      : asia_katmandu
  random                        : 7316819579397678157
  user_agent                    : facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)
  @author_id:5fb3319127e54b918dba83fdcd2915f5:
    date_of_birth               : 1985-02-05
    name                        : inventore
  @author_id:680ed8535f33430fad16263f7f18573e:
    date_of_birth               : 1982-10-14
    name                        : et
  @book_id:7b1ab10e58d046b38324d91d9002801b:
    genre                       : quis
    name                        : natus
    published_at                : 1993-08-06
  @book_id:7cfa0d83c12544be9de0ddfa658a7860:
    author_id                   : 5fb3319127e54b918dba83fdcd2915f5
    genre                       : sit
    name                        : assumenda
    published_at                : 1977-10-19
  @book_id:fb2327cdd8f145c392f0c0a93d3d3fcd:
    author_id                   : 680ed8535f33430fad16263f7f18573e
    genre                       : at
    name                        : et
    published_at                : 1972-04-07

user_id:ffd1094c0dc6492c9f2418bb1b4de8b6
type:ignore
Fri Jul 19 13:58:11 CEST 2019
  year                          : 2019
  year-month                    : 2019-07
  year-month-day                : 2019-07-19
  year-month-day-hour           : 2019-07-19-11
  currency                      : hkd
  timezone                      : africa_ouagadougou
  random                        : 1940828931966163429
  user_agent                    : OctoBot/2.1 (OctoBot/2.1.0; +http://www.octofinder.com/octobot.html?2.1)

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