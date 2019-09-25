# BLACKROCK CLAN

![blackrock](_/img/blackrock.jpg)

# [LORE](https://wow.gamepedia.com/Blackrock_clan)

The Blackrock clan is a prominent orcish clan originally hailing from
the caverns of Gorgrond. On Draenor, they were known for their strict
military discipline and skills in mining and blacksmithing. One of the
first orcish clans to be taught the ways of fel magic, the Blackrocks
were the strongest faction within the Old Horde during the course of
the First and Second Wars, and the Horde's first two Warchiefs —
Blackhand and his deposer Orgrim Doomhammer — were both Blackrocks.

# heroes

This is simple proof of concept events index and search system.

It has only kafka as dependency, it abuses the fact that offsets are
ordered within a partition, and builds inverted indexes that can be searched


Composed of the following characters:

* [orgrim ](orgrim/) - consume events
* [jubei](jubei/) - create indexes
* [khanzo](khanzo/) - search

# do not use in production, it is 1 day old

# running it locally

You can run it locally without any dependencies, there is
special parameter to khanzo that embeds orgrim and jubei(for testing
purposes).

```
$ cd khanzo && go build
$ ./khanzo -root /tmp/gen13  -not-production-accept-events -not-production-geoip GeoLite2-City.mmdb -bind :9001
```
you can download GeoLite2-City from https://dev.maxmind.com/geoip/geoip2/geolite2/

this will start khanzo at port 9001, so you can already start using it both to search (http://localhost:9001/scan/html/) and to send events to.

example event:

```
curl -d '{
  "count": {
    "sizeWH": "1455x906",
    "tz_offset": -120,
    "window_scroll": {
      "x": 0,
      "y": 66
    }
  },
  "event_type": "buy_book",
  "foreign_id": "c2ace436-cbb9-46e5-8bf8-fcae013c904b",
  "foreign_type": "user_id",
  "properties": {
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36"
  },
  "search": {
    "book_isbn_code": {
      "9783161484100": true,
      "9781234567897": true
    },
    "campaign": "google",
    "version": {
      "branch": "test",
      "sha": "878345202",
      "version": "1.2.3"
    }
  }
}' http://127.0.0.1:9001/push/flatten

```

Now if you go to http://localhost:9001/scan/html/event_type:buy_book you will see all events with this type.

## event structure

There are 3 endpoints on orgrim /push/flatten, /push/envelope and
/push/context, /push/envelope takes a protobuf Envelope object from
[orgrim/spec/spec.proto](orgrim/spec/spec.proto), /push/context takes
Context object from [orgrim/spec/spec.proto](orgrim/spec/spec.proto),
/push/flatten can take complex json and flatten it into Envelope using
the dot notation (meaning `{a:{b:true}}` becomes `{a.b: true}`)

### EVENT_TYPE, FOREIGN_ID, FOREIGN_TYPE

Those are mandatory fields

* event_type: this is the type of the event (haha its in the name), there are also charts of events per event type per time
* foreign_type: e.g. user_id or book_id or whatever foreign_id refers to
* foreign_id: the id of the event creator (for example the user id that is buying the book)

### SEARCH

Everything in this section is searchable and postings lists are
created for every key:value pair e.g. going to
http://localhost:9001/scan/html/event_type:buy_book/campaign:google
will give you all events that match on the query `event_type=book AND
campaign=google`, the url DSL is not very sophisticated but works for
now, you can do
`http://localhost:9001/scan/html/event_type:buy_book|event_type:click_book`
to do `event_type=buy_book OR event_type=click_book`, and you can also
do AND NOT with
`http://localhost:9001/scan/html/event_type:buy_book/-campaign:google`

All fields are also aggregated and counted

### COUNT

Everything in this section is used only in the aggregations views but it is not indexed

### PROPERTIES

This is not indexed nor counted, so good for stuff like user-agent

# Query DSL

README:TODO

# Experimentation

README:TODO


# cool text

example khanzo output (see full output on [baxx.dev](https://baxx.dev/s/72eb433e-4bae-4533-a0f3-86dcbca835aa))

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
