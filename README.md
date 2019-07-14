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
* [blackhand](blackhand/) - fetch specific offset

# do not use in production, it is 1 day old

# cool text

example khanzo output (see full output on [baxx.dev](https://baxx.dev/s/3b50e58d-b0e5-43ae-bf23-45cd002708c7))

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