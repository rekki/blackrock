package spec

import (
	"encoding/json"
	fmt "fmt"
	io "io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/jackdoe/blackrock/depths"
	"github.com/mssola/user_agent"
	"github.com/oschwald/geoip2-golang"
	"github.com/tomasen/realip"
)

/*

{
   restaurant: {
       "92e2e4af-f833-492e-9ade-f797bbaa80fd": true,
       "ca91f7ab-13fa-46b7-9fbc-3f0276647238": true
   }
   message: "hello",
}
in this case you want to search for message:helo/restaurant:92e2e4af-f833-492e-9ade-f797bbaa80fd

{
   restaurant: {
       "92e2e4af-f833-492e-9ade-f797bbaa80fd": { updated: true  },
       "ca91f7ab-13fa-46b7-9fbc-3f0276647238": { updated: false }
   }
}


{
   example: {
      restaurant: {
          "92e2e4af-f833-492e-9ade-f797bbaa80fd": { updated: true  },
          "ca91f7ab-13fa-46b7-9fbc-3f0276647238": { updated: false }
      }
   }
}

possible search would be restaurant:92e2e4af-f833-492e-9ade-f797bbaa80fd/updated:true
but never restaurant:true or example:true

because of this we make extremely simple convention, every key that
has _id or _ids is expanded, e.g.:

{
   example: {
      restaurant_id: {
          "92e2e4af-f833-492e-9ade-f797bbaa80fd": { updated: true  },
          "ca91f7ab-13fa-46b7-9fbc-3f0276647238": { updated: false }
      }
   }
}

this event will be findable by 'restaurant_id:ca91f7ab-13fa-46b7-9fbc-3f0276647238'
but also 'example.retaurant_id.ca91f7ab-13fa-46b7-9fbc-3f0276647238.updated:true'


*/

type JsonFrame struct {
	Search      map[string]interface{} `json:"search"`
	Count       map[string]interface{} `json:"count"`
	Properties  map[string]interface{} `json:"properties"`
	CreatedAtNs int64                  `json:"created_at_ns"`
	ForeignId   string                 `json:"foreign_id"`
	ForeignType string                 `json:"foreign_type"`
	EventType   string                 `json:"event_type"`
	Payload     interface{}            `json:"payload"`
}

func Transform(m map[string]interface{}, expand bool) ([]KV, error) {
	out := []KV{}
	flatten, err := depths.Flatten(m, "", depths.DotStyle)
	if err != nil {
		return nil, err
	}
	hasID := func(s string) bool {
		return strings.HasSuffix(s, "_id") || strings.HasSuffix(s, "_ids") || strings.HasSuffix(s, "_code")
	}
	for k, v := range flatten {
		if expand {

			// a lot of CoC here, path like example.restaurant_id.92e2e4af-f833-492e-9ade-f797bbaa80fd.updated = true
			// will be expanded to restaurant_id:92e2e4af-f833-492e-9ade-f797bbaa80fd and example.updated:true
			// so that it can be found, this of course is not ideal
			if v == "true" {
				splitted := strings.Split(k, ".")
				if len(splitted) > 1 {
					for i := 0; i < len(splitted)-1; i++ {
						p := splitted[i]
						if hasID(p) {
							k = strings.Join(splitted[:i+1], ".")
							v = strings.Join(splitted[i+1:], ".")

							break
						}
					}
				}
			}
		}
		out = append(out, KV{Key: k, Value: v})
	}

	return out, nil
}

func Decorate(g *geoip2.Reader, r *http.Request, e *Envelope) error {
	m := e.Metadata
	ip := net.ParseIP(realip.FromRequest(r))
	m.Search = append(m.Search, KV{Key: "ip", Value: ip.String()})

	if g != nil {
		record, err := g.City(ip)
		if err != nil {
			return err
		}
		lang := "en"

		m.Search = append(m.Search, KV{Key: "geoip_city", Value: record.City.Names[lang]})
		m.Search = append(m.Search, KV{Key: "geoip_country", Value: record.Country.Names[lang]})
	}
	agent := r.Header.Get("User-Agent")
	ua := user_agent.New(agent)
	m.Search = append(m.Search, KV{"ua_is_bot", fmt.Sprintf("%v", ua.Bot())})
	m.Search = append(m.Search, KV{"ua_is_mobile", fmt.Sprintf("%v", ua.Mobile())})
	m.Search = append(m.Search, KV{"ua_platform", ua.Platform()})
	m.Search = append(m.Search, KV{"ua_os", ua.OS()})

	name, version := ua.Engine()
	m.Search = append(m.Search, KV{"ua_engine_name", name})
	m.Search = append(m.Search, KV{"ua_engine_version", version})

	name, version = ua.Browser()
	m.Search = append(m.Search, KV{"ua_browser_name", name})
	m.Search = append(m.Search, KV{"ua_browser_version", version})

	return nil
}

func DecodeAndFlatten(body io.Reader) (*Envelope, error) {
	var metadata JsonFrame
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return nil, err
	}

	search := []KV{}
	if metadata.Search != nil {
		search, err = Transform(metadata.Search, true)
		if err != nil {
			return nil, err
		}
	}

	count := []KV{}
	if metadata.Count != nil {
		count, err = Transform(metadata.Count, true)
		if err != nil {
			return nil, err
		}
	}

	properties := []KV{}
	if metadata.Properties != nil {
		properties, err = Transform(metadata.Properties, true)
		if err != nil {
			return nil, err
		}
	}

	converted := Envelope{
		Metadata: &Metadata{
			Search:      search,
			Count:       count,
			Properties:  properties,
			CreatedAtNs: metadata.CreatedAtNs,
			EventType:   metadata.EventType,
			ForeignId:   metadata.ForeignId,
			ForeignType: metadata.ForeignType,
		},
	}

	if metadata.Payload != nil {
		payload, err := json.Marshal(&metadata.Payload)
		if err != nil {
			return nil, err
		}
		converted.Payload = payload
	}

	if converted.Metadata.CreatedAtNs == 0 {
		converted.Metadata.CreatedAtNs = time.Now().UnixNano()
	}

	err = ValidateEnvelope(&converted)

	return &converted, err
}

type AccessToken struct {
	product map[string]bool
	context map[string]bool
}

func NewAccessToken() *AccessToken {
	return &AccessToken{product: map[string]bool{}, context: map[string]bool{}}
}

func (at *AccessToken) AllowContext(c *Context) bool {
	return at.context[c.ForeignType]
}

func (at *AccessToken) AllowEnvelope(e *Envelope) bool {
	for _, s := range e.Metadata.Search {
		if s.Key == "product" {
			if _, ok := at.product[s.Value]; ok {
				return true
			}
		}
	}
	return false
}

type TokenToProduct struct {
	byToken map[string]*AccessToken
}

func NewTokenMap(toproduct, tocontext string) *TokenToProduct {
	tp := &TokenToProduct{
		byToken: map[string]*AccessToken{},
	}
	depths.ForeachCSV(toproduct, func(k, v string) {
		at, ok := tp.byToken[k]
		if !ok {
			at = NewAccessToken()
			tp.byToken[k] = at
		}
		at.product[v] = true

	})
	depths.ForeachCSV(tocontext, func(k, v string) {
		at, ok := tp.byToken[k]
		if !ok {
			at = &AccessToken{product: map[string]bool{}, context: map[string]bool{}}
			tp.byToken[k] = at
		}
		at.context[v] = true
	})
	return tp
}

func (tp *TokenToProduct) ExtractFromRequest(r *http.Request) *AccessToken {
	reqToken := r.Header.Get("Authorization")
	splitToken := strings.Split(reqToken, "Bearer")

	if len(splitToken) != 2 {
		return nil
	}

	reqToken = strings.TrimSpace(splitToken[1])
	t, ok := tp.byToken[reqToken]
	if ok {
		return t
	}
	return NewAccessToken()
}

func (tp *TokenToProduct) ExtractFromToken(token string) *AccessToken {
	t, ok := tp.byToken[token]
	if ok {
		return t
	}
	return NewAccessToken()
}
