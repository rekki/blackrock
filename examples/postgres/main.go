package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	orgrim "github.com/rekki/blackrock/cmd/orgrim/client"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
)

func tryAnything(b []byte) map[string]interface{} {
	m := map[string]interface{}{}
	err := json.Unmarshal(b, &m)
	if err != nil {
		log.Panic(err)
	}
	return m
}

func main() {
	ftype := flag.String("foreign-type", "", "type of the context (e.g user_id)")
	fid := flag.String("foreign-id-field", "id", "which field will be used as foreign_id")
	fcreatedAt := flag.String("created-at-field", "created_at", "which field to be used as the created at field")
	createdAtFormat := flag.String("created-at-format", "2006-01-02T15:04:05", "textual representation of Mon Jan 2 15:04:05 -0700 MST 2006")
	fsql := flag.String("sql", "select id, name from users", "execute query")
	fdest := flag.String("orgrim", "http://localhost:9001", "orgrim server to upload the context to")
	pgurl := flag.String("postgres-url", "host=localhost user=postgres dbname=example password=example", "Postgres URL")
	flag.Parse()

	og := orgrim.NewClient(*fdest, "", nil)
	if *ftype == "" {
		log.Fatal("need -foreign-type")
	}

	if *fid == "" {
		log.Fatal("need -foreign-id")
	}

	if *fcreatedAt == "" {
		log.Fatal("need -created-at-field")
	}

	db, err := sql.Open("postgres", *pgurl)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("select row_to_json(foo) from (%s) foo", *fsql))
	if err != nil {
		log.Panic(err)

	}
	i := 0
	for rows.Next() {
		p := []byte{}
		err = rows.Scan(&p)
		if err != nil {
			log.Panic(err)

		}
		parsed := tryAnything(p)
		id, ok := parsed[*fid]
		if !ok {
			log.Panicf("row without id, %v", parsed)
		}

		icreatedAt, ok := parsed[*fcreatedAt]
		if !ok {
			log.Panicf("row without created at, %v", parsed)
		}
		screatedAt, ok := icreatedAt.(string)
		if !ok {
			log.Panicf("crated at is not string (will be decoded), %v", parsed)
		}
		createdAt, err := time.Parse(*createdAtFormat, screatedAt)
		if err != nil {
			log.Panicf("error decoding time, err: %s, %v", err.Error(), parsed)
		}

		ctx := &spec.Context{
			ForeignType: *ftype,
			ForeignId:   orgrim.ToString(id),
			CreatedAtNs: createdAt.UnixNano(),
		}

		for k, v := range parsed {
			if k == *fcreatedAt || k == *fid {
				continue
			}
			flattened := map[string]string{}
			switch v.(type) {
			case []interface{}:
				flattened, err = depths.FlattenArray(v.([]interface{}), k+".", depths.DotStyle)
				if err != nil {
					log.Panicf("failed to flatten: err: %s, value: %v", err.Error(), parsed)
				}

			case map[string]interface{}:
				flattened, err = depths.Flatten(v.(map[string]interface{}), k+".", depths.DotStyle)
				if err != nil {
					log.Panicf("failed to flatten: err: %s, value: %v", err.Error(), parsed)
				}
			default:
				flattened[k] = orgrim.ToString(v)
			}
			for fk, fv := range flattened {
				ctx.Properties = append(ctx.Properties, orgrim.KV(fk, fv))
			}
		}
		err = og.PushContext(ctx)
		if err != nil {
			log.Panic(err)
		}

		fmt.Printf(".")
		i++
		if i%70 == 0 {
			fmt.Printf("\n")
		}
	}
	fmt.Printf("\n")
}
