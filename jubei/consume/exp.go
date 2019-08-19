package consume

import (
	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/orgrim/spec"
	log "github.com/sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

type ExperimentState map[string]map[string][]spec.TrackExperiment

func (es ExperimentState) Find(from uint64, name string, foreignType, foreignId string) *spec.TrackExperiment {
	perId, ok := es[foreignType]
	if !ok {
		return nil
	}
	v, ok := perId[foreignId]
	if !ok {
		return nil
	}

	for _, e := range v {
		if e.FirstTrackedAtNs >= from && e.Name == name {
			return &e
		}
	}
	return nil
}

func (es ExperimentState) FindAny(from uint64, foreignType, foreignId string) []*spec.TrackExperiment {
	perId, ok := es[foreignType]
	if !ok {
		return nil
	}
	v, ok := perId[foreignId]
	if !ok {
		return nil
	}
	out := []*spec.TrackExperiment{}
	for _, e := range v {
		if e.FirstTrackedAtNs >= from {
			out = append(out, &e)
		}
	}
	return out
}

func (es ExperimentState) Add(t spec.TrackExperiment) bool {
	perId, ok := es[t.ForeignType]
	if !ok {
		perId = map[string][]spec.TrackExperiment{}
		es[t.ForeignType] = perId
	}
	existing, ok := perId[t.ForeignId]
	if !ok {
		perId[t.ForeignId] = []spec.TrackExperiment{t}
		log.Infof("new first seen %s:%s exp: %s, variant: %d %v", t.ForeignType, t.ForeignId, t.Name, t.Variant, t)
		return true
	}

	for _, v := range existing {
		if v.Name == t.Name {
			if v.Variant != t.Variant {
				log.Warnf("ignoring event with variant mismatch, was %v got %v", v, t)
			}
			return false
		}
	}
	existing = append(existing, t)
	perId[t.ForeignId] = existing
	return true
}

type ExperimentStateWriter struct {
	data    ExperimentState
	forward *disk.ForwardWriter
}

func ExpDice(ftype, id, exp string, variants uint32) uint32 {
	h := murmur3.Sum32WithSeed([]byte(ftype), 0) + murmur3.Sum32WithSeed([]byte(id), 0) + murmur3.Sum32WithSeed([]byte(exp), 0)
	return h % variants
}

func NewExperimentStateWriter(root string) (*ExperimentStateWriter, error) {
	f, err := disk.NewForwardWriter(root, "exp")
	if err != nil {
		return nil, err
	}

	d, err := LoadExperiments(f)
	if err != nil {
		return nil, err
	}
	return &ExperimentStateWriter{
		data:    d,
		forward: f,
	}, nil
}
func (ex *ExperimentStateWriter) Close() {
	ex.forward.Close()
}
func (ex *ExperimentStateWriter) Add(t spec.TrackExperiment) error {
	added := ex.data.Add(t)
	if added {
		b, err := proto.Marshal(&t)
		if err != nil {
			return err
		}
		_, err = ex.forward.Append(b)
		return err
	}
	return nil
}

func LoadExperimentsFromPath(root string) (ExperimentState, error) {
	f, err := disk.NewForwardWriter(root, "exp")
	if err != nil {
		return nil, err
	}
	return LoadExperiments(f)
}

func LoadExperiments(f *disk.ForwardWriter) (ExperimentState, error) {
	out := ExperimentState{}
	err := f.Scan(0, func(next uint32, data []byte) error {
		var v spec.TrackExperiment
		err := proto.Unmarshal(data, &v)
		if err != nil {
			return err
		}
		out.Add(v)
		return nil
	})

	return out, err
}
