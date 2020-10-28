package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gogo/gateway"
	"github.com/gogo/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	spec "github.com/rekki/blackrock/pkg/blackrock_io"

	"github.com/rekki/blackrock/pkg/index"
	. "github.com/rekki/blackrock/pkg/logger"
	"google.golang.org/grpc"
)

type server struct {
	si         *index.SearchIndex
	ignoreType map[string]bool
}

func (s *server) SaySearch(ctx context.Context, qr *spec.SearchQueryRequest) (*spec.SearchQueryResponse, error) {
	out := &spec.SearchQueryResponse{
		Total: 0,
	}

	scored := []spec.Hit{}
	err := s.si.ForEach(qr, 0, func(segment *index.Segment, did int32, score float32) error {
		out.Total++
		if qr.Limit == 0 {
			return nil
		}
		doInsert := false
		if len(scored) < int(qr.Limit) {
			doInsert = true
		} else if scored[len(scored)-1].Score < score {
			doInsert = true
		}

		if doInsert {
			hit := spec.Hit{Score: score}
			m := spec.Metadata{}
			err := segment.ReadForwardDecode(did, &m)
			if err != nil {
				// FIXME(jackdoe): should we skip here? or return partial result.
				return nil
			}
			hit.Id = m.Id
			if hit.Id == 0 {
				hit.Id = uint64(did) + 1
			}
			hit.Metadata = &m
			hit.Score = score
			if len(scored) < int(qr.Limit) {
				scored = append(scored, hit)
			}
			for i := 0; i < len(scored); i++ {
				if scored[i].Score < hit.Score {
					copy(scored[i+1:], scored[i:])
					scored[i] = hit
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	out.Hits = make([]*spec.Hit, len(scored))
	for i, v := range scored {
		out.Hits[i] = &v
	}

	return out, nil
}

func (s *server) SayFetch(qr *spec.SearchQueryRequest, stream spec.Search_SayFetchServer) error {
	err := s.si.ForEach(qr, uint32(qr.Limit), func(segment *index.Segment, did int32, score float32) error {
		metadata := &spec.Metadata{}
		err := segment.ReadForwardDecode(did, metadata)
		if err != nil {
			return err
		}

		hit := toHit(did, metadata)
		return stream.Send(hit)
	})

	return err
}

func (s *server) SayAggregate(ctx context.Context, qr *spec.AggregateRequest) (*spec.Aggregate, error) {
	steps := s.si.ExpandFromTo(qr.Query.FromSecond, qr.Query.ToSecond)
	dates := []time.Time{}
	for _, ns := range steps {
		dates = append(dates, time.Unix(ns/1000000000, 0))
	}
	if len(dates) == 0 {
		return nil, errors.New("bad date range, to_second must be older than from_second")
	}
	out := &spec.Aggregate{
		Search:    map[string]*spec.CountPerKV{},
		Count:     map[string]*spec.CountPerKV{},
		EventType: map[string]*spec.CountPerKV{},
		ForeignId: map[string]*spec.CountPerKV{},
		Possible:  map[string]uint32{},
		Total:     0,
	}
	var chart *Chart
	if qr.TimeBucketSec != 0 {
		chart = NewChart(qr.TimeBucketSec, dates)
		out.Chart = chart.out
	}

	eventTypeKey := "event_type"
	foreignIdKey := "foreign_id"
	etype := &spec.CountPerKV{Count: map[string]uint32{}, Key: eventTypeKey}

	wantEventType := qr.Fields[eventTypeKey]
	wantForeignId := qr.Fields[foreignIdKey]

	if wantEventType {
		out.EventType[eventTypeKey] = etype
	}
	add := func(x []spec.KV, into map[string]*spec.CountPerKV) {
		for _, kv := range x {
			out.Possible[kv.Key]++
			if _, ok := qr.Fields[kv.Key]; !ok {
				continue
			}
			m, ok := into[kv.Key]
			if !ok {
				m = &spec.CountPerKV{Count: map[string]uint32{}, Key: kv.Key}
				into[kv.Key] = m
			}
			m.Count[kv.Value]++
			m.Total++
		}
	}

	err := s.si.ForEach(qr.Query, 0, func(segment *index.Segment, did int32, score float32) error {
		out.Total++

		data, err := segment.ReadForward(did)
		if err != nil {
			return err
		}

		metadata := &spec.CountableMetadata{}
		err = proto.Unmarshal(data, metadata)
		if err != nil {
			return err
		}

		add(metadata.Search, out.Search)
		add(metadata.Count, out.Count)

		if wantEventType {
			etype.Count[metadata.EventType]++
			etype.Total++
		}

		if wantForeignId {
			m, ok := out.ForeignId[metadata.ForeignType]
			if !ok {
				m = &spec.CountPerKV{Count: map[string]uint32{}, Key: metadata.ForeignType}
				out.ForeignId[metadata.ForeignType] = m
			}
			m.Count[metadata.ForeignId]++
			m.Total++
		}

		if len(out.Sample) < int(qr.SampleLimit) {
			full := &spec.Metadata{}
			err = proto.Unmarshal(data, full)
			if err != nil {
				return err
			}

			hit := toHit(did, full)
			out.Sample = append(out.Sample, hit)
		}
		if chart != nil {
			chart.Add(metadata)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	out.Possible[foreignIdKey] = out.Total
	out.Possible[eventTypeKey] = out.Total

	sort.Slice(out.Sample, func(i, j int) bool {
		return out.Sample[i].Metadata.CreatedAtNs < out.Sample[j].Metadata.CreatedAtNs
	})
	return out, nil
}

func (s *server) SayPush(stream spec.Search_SayPushServer) error {
	for {
		envelope, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if envelope.Metadata != nil && s.ignoreType[envelope.Metadata.EventType] {
			continue
		}
		err = s.si.Ingest(envelope)
		if err != nil {
			return err
		}
	}
	return stream.SendAndClose(&spec.Success{Success: true})
}

func (s *server) SayHealth(context.Context, *spec.HealthRequest) (*spec.Success, error) {
	return &spec.Success{Success: true}, nil
}

func toHit(did int32, p *spec.Metadata) *spec.Hit {
	id := p.Id
	if id == 0 {
		id = uint64(did) + 1
	}
	hit := &spec.Hit{
		Id:       id,
		Metadata: p,
	}

	return hit
}

func runProxy(bindHttp string, bindGrpc string) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jsonpb := &gateway.JSONPb{
		EmitDefaults: true,
		OrigName:     true,
	}
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, jsonpb),
		// This is necessary to get error details properly
		// marshalled in unary requests.
		runtime.WithProtoErrorHandler(runtime.DefaultHTTPProtoErrorHandler),
	)

	opts := []grpc.DialOption{grpc.WithInsecure()}

	err := spec.RegisterSearchHandlerFromEndpoint(ctx, mux, bindGrpc, opts)
	if err != nil {
		return err
	}

	return http.ListenAndServe(bindHttp, mux)
}

func main() {
	var proot = flag.String("root", "/blackrock/data-topic", "root directory for the files root/topic")
	var bindHttp = flag.String("http", ":9002", "bind to")
	var bindGrpc = flag.String("grpc", ":8002", "bind to")

	var logLevel = flag.Int("log-level", 0, "log level")
	var segmentStep = flag.Int("segment-step", 3600, "segment step")
	var maxOpenFD = flag.Int("max-open-fd", 1000, "max open file descriptors to write")
	var pwhitelist = flag.String("whitelist", "", "csv list of indexable search terms, nothing means all")
	var pignore = flag.String("ignore-type", "", "csv list of event types to ignore")
	var enableSegmentCache = flag.Bool("enable-segment-cache", false, "enable memory cache")
	flag.Parse()

	LogInit(*logLevel)

	go func() {
		Log.Info(http.ListenAndServe("localhost:6060", nil))
	}()

	root := *proot
	whitelist := map[string]bool{}
	for _, v := range strings.Split(*pwhitelist, ",") {
		if len(v) > 0 {
			whitelist[v] = true
		}
	}

	ignoreType := map[string]bool{}
	for _, v := range strings.Split(*pignore, ",") {
		if len(v) > 0 {
			ignoreType[v] = true
		}
	}

	si := index.NewSearchIndex(root, *maxOpenFD, int64(*segmentStep), *enableSegmentCache, whitelist)
	go func() {
		err := runProxy(*bindHttp, *bindGrpc)
		if err != nil {
			Log.Warnf("failed to run the proxy, err: %s", err.Error())
			os.Exit(0)
		}
	}()

	lis, err := net.Listen("tcp", *bindGrpc)
	if err != nil {
		Log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(AddLogging([]grpc.ServerOption{})...)

	srv := &server{si: si, ignoreType: ignoreType}
	spec.RegisterSearchServer(grpcServer, srv)
	err = grpcServer.Serve(lis)
	Log.Fatal(err)
}
