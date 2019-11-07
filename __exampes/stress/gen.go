package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/bxcodec/faker"

	"github.com/rekki/blackrock/cmd/orgrim/client"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
)

var UA = []string{
	"Baiduspider+(+http://www.baidu.com/search/spider.htm",
	"Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)",
	"Moreoverbot/5.1 (+http://w.moreover.com; webmaster@moreover.com) Mozilla/5.0",
	"UnwindFetchor/1.0 (+http://www.gnip.com/)",
	"Voyager/1.0",
	"PostRank/2.0 (postrank.com)",
	"R6_FeedFetcher(www.radian6.com/crawler)",
	"R6_CommentReader(www.radian6.com/crawler)",
	"radian6_default_(www.radian6.com/crawler)",
	"Mozilla/5.0 (compatible; Ezooms/1.0; ezooms.bot@gmail.com)",
	"ia_archiver (+http://www.alexa.com/site/help/webmasters; crawler@alexa.com)",
	"Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)",
	"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
	"Mozilla/5.0 (en-us) AppleWebKit/525.13 (KHTML, like Gecko; Google Web Preview) Version/3.1 Safari/525.13",
	"Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
	"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
	"Twitterbot/0.1",
	"LinkedInBot/1.0 (compatible; Mozilla/5.0; Jakarta Commons-HttpClient/3.1 +http://www.linkedin.com)",
	"bitlybot",
	"MetaURI API/2.0 +metauri.com",
	"Mozilla/5.0 (compatible; Birubot/1.0) Gecko/2009032608 Firefox/3.0.8",
	"Mozilla/5.0 (compatible; PrintfulBot/1.0; +http://printful.com/bot.html)",
	"Mozilla/5.0 (compatible; PaperLiBot/2.1)",
	"Summify (Summify/1.0.1; +http://summify.com)",
	"Mozilla/5.0 (compatible; TweetedTimes Bot/1.0; +http://tweetedtimes.com)",
	"PycURL/7.18.2",
	"facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)",
	"Python-urllib/2.6",
	"Python-httplib2/$Rev$",
	"AppEngine-Google; (+http://code.google.com/appengine; appid: lookingglass-server)",
	"Wget/1.9+cvs-stable (Red Hat modified)",
	"Mozilla/5.0 (compatible; redditbot/1.0; +http://www.reddit.com/feedback)",
	"Mozilla/5.0 (compatible; MSIE 6.0b; Windows NT 5.0) Gecko/2009011913 Firefox/3.0.6 TweetmemeBot",
	"Mozilla/5.0 (compatible; discobot/1.1; +http://discoveryengine.com/discobot.html)",
	"Mozilla/5.0 (compatible; Exabot/3.0; +http://www.exabot.com/go/robot)",
	"Mozilla/5.0 (compatible; SiteBot/0.1; +http://www.sitebot.org/robot/)",
	"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1 + FairShare-http://fairshare.cc)",
	"HTTP_Request2/2.0.0beta3 (http://pear.php.net/package/http_request2) PHP/5.3.2",
	"Mozilla/5.0 (compatible; Embedly/0.2; +http://support.embed.ly/)",
	"magpie-crawler/1.1 (U; Linux amd64; en-GB; +http://www.brandwatch.net)",
	"(TalkTalk Virus Alerts Scanning Engine)",
	"Sogou web spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)",
	"Googlebot/2.1 (http://www.googlebot.com/bot.html)",
	"msnbot-NewsBlogs/2.0b (+http://search.msn.com/msnbot.htm)",
	"msnbot/2.0b (+http://search.msn.com/msnbot.htm)",
	"msnbot-media/1.1 (+http://search.msn.com/msnbot.htm)",
	"Mozilla/5.0 (compatible; oBot/2.3.1; +http://www-935.ibm.com/services/us/index.wss/detail/iss/a1029077?cntxt=a1027244)",
	"Sosospider+(+http://help.soso.com/webspider.htm)",
	"COMODOspider/Nutch-1.0",
	"trunk.ly spider contact@trunk.ly",
	"Mozilla/5.0 (compatible; Purebot/1.1; +http://www.puritysearch.net/)",
	"Mozilla/5.0 (compatible; MJ12bot/v1.4.0; http://www.majestic12.co.uk/bot.php?+)",
	"knowaboutBot 0.01",
	"Showyoubot (http://showyou.com/support)",
	"Flamingo_SearchEngine (+http://www.flamingosearch.com/bot)",
	"MLBot (www.metadatalabs.com/mlbot)",
	"my-robot/0.1",
	"Mozilla/5.0 (compatible; woriobot support [at] worio [dot] com +http://worio.com)",
	"Mozilla/5.0 (compatible; YoudaoBot/1.0; http://www.youdao.com/help/webmaster/spider/; )",
	"chilitweets.com",
	"Mozilla/5.0 (TweetBeagle; http://app.tweetbeagle.com/)",
	"OctoBot/2.1 (OctoBot/2.1.0; +http://www.octofinder.com/octobot.html?2.1)",
	"Mozilla/5.0 (compatible; FriendFeedBot/0.1; +Http://friendfeed.com/about/bot)",
	"Mozilla/5.0 (compatible; WASALive Bot ; http://blog.wasalive.com/wasalive-bots/)",
	"Mozilla/5.0 (compatible; Apercite; +http://www.apercite.fr/robot/index.html)",
	"urlfan-bot/1.0; +http://www.urlfan.com/site/bot/350.html",
	"SeznamBot/3.0 (+http://fulltext.sblog.cz/)",
	"Yeti/1.0 (NHN Corp.; http://help.naver.com/robots/)",
	"Mozilla/5.0 (Windows; U; Windows NT 6.0; en-GB; rv:1.0; trendictionbot0.4.2; trendiction media ssppiiddeerr; http://www.trendiction.com/bot/; please let us know of any problems; ssppiiddeerr at trendiction.com) Gecko/20071127 Firefox/2.0.0.11",
	"yacybot (freeworld/global; amd64 Linux 2.6.35-24-generic; java 1.6.0_20; Asia/en) http://yacy.net/bot.html",
	"Mozilla/5.0 (compatible; suggybot v0.01a, http://blog.suggy.com/was-ist-suggy/suggy-webcrawler/)",
	"ssearch_bot (sSearch Crawler; http://www.semantissimo.de)",
	"Mozilla/5.0 (compatible; Linux; Socialradarbot/2.0; en-US; crawler@infegy.com)",
	"wikiwix-bot-3.0",
	"Mozilla/5.0 (compatible; AhrefsBot/1.0; +http://ahrefs.com/robot/)",
	"Mozilla/5.0 (compatible; DotBot/1.1; http://www.dotnetdotcom.org/, crawler@dotnetdotcom.org)",
	"GarlikCrawler/1.1 (http://garlik.com/, crawler@garik.com)",
	"Mozilla/5.0 (compatible; SISTRIX Crawler; http://crawler.sistrix.net/)",
	"Mozilla/5.0 (compatible; 008/0.83; http://www.80legs.com/webcrawler.html) Gecko/2008032620",
	"PostPost/1.0 (+http://postpo.st/crawlers)",
	"Aghaven/Nutch-1.2 (www.aghaven.com)",
	"SBIder/Nutch-1.0-dev (http://www.sitesell.com/sbider.html)",
	"Mozilla/5.0 (compatible; ScoutJet; +http://www.scoutjet.com/)",
	"Trapit/1.1",
	"Jakarta Commons-HttpClient/3.1",
	"Readability/0.1",
	"kame-rt (support@backtype.com)",
	"Mozilla/5.0 (compatible; Topix.net; http://www.topix.net/topix/newsfeeds)",
	"Megite2.0 (http://www.megite.com)",
	"SkyGrid/1.0 (+http://skygrid.com/partners)",
	"Netvibes (http://www.netvibes.com)",
	"Zemanta Aggregator/0.7 +http://www.zemanta.com",
	"Owlin.com/1.3 (http://owlin.com/)",
	"Mozilla/5.0 (compatible; Twitturls; +http://twitturls.com)",
	"Tumblr/1.0 RSS syndication (+http://www.tumblr.com/) (support@tumblr.com)",
	"Mozilla/4.0 (compatible; www.euro-directory.com; urlchecker1.0)",
	"Covario-IDS/1.0 (Covario; http://www.covario.com/ids; support at covario dot com)",
}

type SomeAction struct {
	TimeZone string `faker:"timezone"`
	Currency string `faker:"currency"`
}

type User struct {
	UUID        string `faker:"uuid_digit"`
	UserName    string `faker:"username"`
	Title       string `faker:"title_male"`
	Name        string `faker:"name"`
	TimeZone    string `faker:"timezone"`
	PhoneNumber string `faker:"phone_number"`
	Email       string `faker:"email"`
	Currency    string `faker:"currency"`
}

type Author struct {
	UUID        string `faker:"uuid_digit"`
	Name        string `faker:"word"`
	DateOfBirth string `faker:"date"`
}

type Country struct {
	UUID string `faker:"uuid_digit"`
	Name string `faker:"word"`
}

type Book struct {
	UUID        string `faker:"uuid_digit"`
	Name        string `faker:"word"`
	Genre       string `faker:"word"`
	PublishDate string `faker:"date"`
}

func genUser(times []time.Time) *spec.Metadata {
	var s User
	err := faker.FakeData(&s)
	if err != nil {
		panic(err)
	}
	c := &spec.Metadata{
		CreatedAtNs: 1,
		ForeignType: "user_id",
		ForeignId:   s.UUID,
	}
	c.Properties = append(c.Properties, spec.KV{Key: "name", Value: s.Name})
	c.Properties = append(c.Properties, spec.KV{Key: "title", Value: s.Title})
	c.Properties = append(c.Properties, spec.KV{Key: "timezone", Value: s.TimeZone})
	c.Properties = append(c.Properties, spec.KV{Key: "email", Value: s.Email})
	c.Properties = append(c.Properties, spec.KV{Key: "currency", Value: s.Currency})
	c.Properties = append(c.Properties, spec.KV{Key: "phone", Value: s.PhoneNumber})
	return c
}

func genCountry(times []time.Time) *spec.Metadata {
	var s Country
	err := faker.FakeData(&s)
	if err != nil {
		panic(err)
	}
	c := &spec.Metadata{
		CreatedAtNs: 1,
		ForeignType: "country_id",
		ForeignId:   s.UUID,
	}
	c.Properties = append(c.Properties, spec.KV{Key: "name", Value: s.Name})
	return c
}

func genAuthor(times []time.Time, countries []*spec.Metadata) *spec.Metadata {
	var s Author
	err := faker.FakeData(&s)
	if err != nil {
		panic(err)
	}

	c := &spec.Metadata{
		CreatedAtNs: 1,
		ForeignType: "author_id",
		ForeignId:   s.UUID,
	}
	c.Properties = append(c.Properties, spec.KV{Key: "name", Value: s.Name})
	c.Properties = append(c.Properties, spec.KV{Key: "country_id", Value: countries[rand.Intn(len(countries))].ForeignId})
	c.Properties = append(c.Properties, spec.KV{Key: "date_of_birth", Value: s.DateOfBirth})
	return c
}

func genBook(times []time.Time, author ...*spec.Metadata) *spec.Metadata {
	var s Book
	err := faker.FakeData(&s)
	if err != nil {
		panic(err)
	}

	c := &spec.Metadata{
		CreatedAtNs: 1,
		ForeignType: "book_id",
		ForeignId:   s.UUID,
	}
	c.Properties = append(c.Properties, spec.KV{Key: "name", Value: s.Name})
	c.Properties = append(c.Properties, spec.KV{Key: "genre", Value: s.Genre})
	c.Properties = append(c.Properties, spec.KV{Key: "published_at", Value: s.PublishDate})
	if len(author) == 0 {
		panic("no author")
	}
	for _, a := range author {
		c.Properties = append(c.Properties, spec.KV{Key: "author_id", Value: a.ForeignId})
	}
	return c
}

func pickAuthors(authors []*spec.Metadata) []*spec.Metadata {
	out := []*spec.Metadata{}
	seen := map[string]bool{}
	for i := 1; i < 2+rand.Intn(5); i++ {
		a := authors[rand.Intn(len(authors))]
		if _, ok := seen[a.ForeignId]; !ok {
			out = append(out, a)
			seen[a.ForeignId] = true
		}
	}
	return out
}

var actions = []string{"buy", "click", "skip", "ignore", "click", "click", "ignore", "ignore", "skip"}

func genEvent(days []time.Time, users []*spec.Metadata, books []*spec.Metadata) *spec.Envelope {
	userIdx := rand.Intn(len(users))
	user := users[userIdx]
	var s SomeAction
	err := faker.FakeData(&s)
	if err != nil {
		panic(err)
	}

	action := actions[rand.Intn(len(actions))]
	search := []spec.KV{}
	for i := 0; i < rand.Intn(5); i++ {
		book := books[rand.Intn(len(books))]
		search = append(search, spec.KV{Key: "book_id", Value: book.ForeignId})
	}
	if userIdx%2 == 1 {
		search = append(search, spec.KV{Key: "env", Value: "staging"})
	} else {
		search = append(search, spec.KV{Key: "env", Value: "live"})
	}

	search = append(search, spec.KV{Key: "product", Value: "amazon.com"})

	ua := UA[rand.Intn(len(UA))]
	m := &spec.Metadata{
		CreatedAtNs: days[rand.Intn(len(days))].UnixNano(),
		ForeignType: "user_id",
		ForeignId:   user.ForeignId,
		EventType:   action,
		Search:      search,
		Count:       []spec.KV{spec.KV{Key: "currency", Value: s.Currency}, spec.KV{Key: "timezone", Value: s.TimeZone}},
		Properties:  []spec.KV{spec.KV{Key: "user_agent", Value: ua}, spec.KV{Key: "random", Value: fmt.Sprintf("%d", rand.Int())}},
	}

	return &spec.Envelope{Metadata: m}
}

func pushManyEvents(orgrim *client.Client, x ...*spec.Envelope) {
	for _, v := range x {
		err := orgrim.Push(v)
		if err != nil {
			log.Fatalf("%v %s", v, err)
		}
	}
}

func expandYYYYMMDD(from string, to string) []time.Time {
	fromTime := time.Now().UTC().AddDate(0, 0, -3)
	toTime := time.Now().UTC()

	if from != "" {
		d, err := time.Parse("2006-01-02", from)
		if err == nil {
			fromTime = d
		} else {
			panic(err)
		}
	}
	if to != "" {
		d, err := time.Parse("2006-01-02", to)
		if err == nil {
			toTime = d
		} else {
			panic(err)
		}
	}

	dateQuery := []time.Time{}
	start := fromTime.AddDate(0, 0, 0)
	for {
		dateQuery = append(dateQuery, start)
		start = start.AddDate(0, 0, 1)
		if start.Sub(toTime) > 0 {
			break
		}
	}
	return dateQuery
}

func main() {
	nUsers := flag.Int("n-users", 100, "number of users")
	nAuthors := flag.Int("n-authors", 100, "number of authors")
	nBooks := flag.Int("n-books", 100, "number of books")
	from := flag.String("from", "", "from")
	to := flag.String("to", "", "to")
	nEvents := flag.Int("n-events", 100, "number of events")
	flag.Parse()
	times := expandYYYYMMDD(*from, *to)
	users := []*spec.Metadata{}
	authors := []*spec.Metadata{}
	books := []*spec.Metadata{}
	countries := []*spec.Metadata{}

	for i := 0; i < *nUsers; i++ {
		users = append(users, genUser(times))
	}

	for i := 0; i < 255; i++ {
		countries = append(countries, genCountry(times))
	}

	for i := 0; i < *nAuthors; i++ {
		authors = append(authors, genAuthor(times, countries))
	}

	for i := 0; i < *nBooks; i++ {
		books = append(books, genBook(times, pickAuthors(authors)...))
	}

	orgrim := client.NewClient("http://localhost:9001/", "", nil)

	for i := 0; i < *nEvents/4; i++ {
		pushManyEvents(
			orgrim,
			genEvent(times, users, books),
			genEvent(times, users[:*nUsers/50], books[:*nBooks/20]),
			genEvent(times, users[:*nUsers/20], books[:*nBooks/10]),
			genEvent(times, users[:*nUsers/10], books[:*nBooks/5]),
		)
	}
}
