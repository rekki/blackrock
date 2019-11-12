package main

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/rekki/blackrock/cmd/jubei/disk"
	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
)

func compactEverything(root string) error {
	days, err := ioutil.ReadDir(path.Join(root))
	if err != nil {
		return err
	}

	for _, day := range days {
		if !day.IsDir() || !depths.IsDigit(day.Name()) {
			continue
		}

		n, err := strconv.Atoi(day.Name())
		if err != nil {
			log.Warnf("skipping %s", day.Name())
			continue
		}
		ts := n * 3600 * 24
		t := time.Unix(int64(ts), 0)
		since := time.Since(t)
		if since < time.Hour*48 {
			log.Warnf("skipping %s, too new: %s", day.Name(), since)
			continue
		}
		p := path.Join(root, day.Name())

		if _, err := os.Stat(path.Join(p, "segment.header")); !os.IsNotExist(err) {
			log.Warnf("skipping %s, already compacted", day.Name())
			continue
		}

		log.Warnf("reading %v", p)
		segment, err := disk.ReadAllTermsInSegment(p)
		if err != nil {
			return err
		}

		log.Warnf("compacting %v, %d terms", p, len(segment))
		err = disk.WriteCompactedIndex(p, segment)
		if err != nil {
			return err
		}
		log.Warnf("deleting raw files %v", p)
		err = disk.DeleteUncompactedPostings(p)
		if err != nil {
			return err
		}
	}
	return nil
}
