package blackrock_io

import (
	"encoding/json"
	fmt "fmt"
	io "io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mssola/user_agent"
	"github.com/oschwald/geoip2-golang"
	"github.com/rekki/blackrock/pkg/depths"
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

func ToString(v interface{}) string {
	var value string
	switch v.(type) {
	case string:
		value = v.(string)
	case int:
		value = fmt.Sprintf("%d", v.(int))
	case int32:
		value = fmt.Sprintf("%d", v.(int32))
	case []byte:
		value = string(v.([]byte))
	case int64:
		value = fmt.Sprintf("%d", v.(int64))
	case int16:
		value = fmt.Sprintf("%d", v.(int16))
	case uint32:
		value = fmt.Sprintf("%d", v.(uint32))
	case uint64:
		value = fmt.Sprintf("%d", v.(uint64))
	case uint16:
		value = fmt.Sprintf("%d", v.(uint16))
	case float32:
		value = strconv.FormatFloat(float64(v.(float32)), 'f', 0, 64)
	case float64:
		value = strconv.FormatFloat(v.(float64), 'f', 0, 64)
	case nil:
		value = "nil"
	default:
		value = fmt.Sprintf("%v", v)
	}

	return value
}

func ToKV(key string, v interface{}) KV {
	value := ToString(v)
	return KV{Key: key, Value: value}
}
