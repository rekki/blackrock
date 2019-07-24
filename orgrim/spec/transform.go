package spec

import (
	"encoding/json"
	io "io"
	"io/ioutil"
	"sort"
	"strings"
	"time"

	"github.com/jackdoe/blackrock/depths"
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

func Transform(m map[string]interface{}, expand bool) ([]*KV, error) {
	out := []*KV{}
	flatten, err := depths.Flatten(m, "", depths.DotStyle)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	add := func(k, v string) {
		key := k + "_" + v
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = true
		out = append(out, &KV{Key: k, Value: v})
	}
	hasID := func(s string) bool {
		return strings.HasSuffix(s, "_id") || strings.HasSuffix(s, "_ids") || strings.HasSuffix(s, "_code")
	}
	for k, v := range flatten {
		if expand {
			// a lot of CoC here, path like example.restaurant_id.92e2e4af-f833-492e-9ade-f797bbaa80fd.updated = true
			// will be expanded to restaurant_id:92e2e4af-f833-492e-9ade-f797bbaa80fd and example.updated:true
			// so that it can be found, this of course is not ideal

			splitted := strings.Split(k, ".")
			noid := []string{}

			for i := len(splitted) - 1; i >= 0; i-- {
				part := splitted[i]
				prev := ""
				if i > 0 {
					prev = splitted[i-1]
				}
				if hasID(part) {
					add(part, v)
				} else {
					if hasID(prev) {
						add(prev, part)
						i--
					} else {
						noid = append(noid, part)
					}
				}
			}

			sort.SliceStable(noid, func(i, j int) bool {
				return true
			})
			if len(noid) > 0 {
				add(strings.Join(noid, "."), v)
			}
		} else {
			out = append(out, &KV{Key: k, Value: v})
		}
	}

	return out, nil
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

	search := []*KV{}
	if metadata.Search != nil {
		search, err = Transform(metadata.Search, true)
		if err != nil {
			return nil, err
		}
	}

	count := []*KV{}
	if metadata.Count != nil {
		count, err = Transform(metadata.Count, true)
		if err != nil {
			return nil, err
		}
	}

	properties := []*KV{}
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
