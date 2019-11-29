package main

import (
	"flag"
	"fmt"
	"github.com/osrg/hookfs/pkg/hookfs"
	"github.com/osrg/hookfs/pkg/qfs"
	"github.com/plimble/ace"
	"math/rand"
	"os"
	"time"

	//hookfs "github.com/osrg/hookfs/hookfs"
	log "github.com/sirupsen/logrus"
)

var ops = []string{
	"PreOpen",
	"PostOpen",
	"PreRead",
	"PostRead",
	"PreWrite",
	"PostWrite",
	"PreMkdir",
	"PostMkdir",
	"PreRmdir",
	"PostRmdir",
	"PreOpenDir",
	"PostOpenDir",
	"PreFsync",
	"PostFsync",
}

type Srv struct {
	a    *ace.Ace
	hook *qfs.QfsHook
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func _init(h *qfs.QfsHook) {
	h.FuseStats = make(map[string]uint16, 0)
	for _, s := range ops {
		h.FuseStats[s] = 0
	}

}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "%s [OPTIONS] MOUNTPOINT ORIGINAL...\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options\n")
		flag.PrintDefaults()
	}

	logLevel := flag.Int("log-level", 0, fmt.Sprintf("log level (%d..%d)", hookfs.LogLevelMin, hookfs.LogLevelMax))

	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(2)
	}

	mountpoint := flag.Arg(0)
	original := flag.Arg(1)
	hookfs.SetLogLevel(*logLevel)

	srv := &Srv{
		a:    ace.New(),
		hook: &qfs.QfsHook{},
	}

	_init(srv.hook)

	HttpSrv(srv)
	serve(original, mountpoint, srv.hook)
}

func serve(original string, mountpoint string, hook *qfs.QfsHook) {
	fs, err := hookfs.NewHookFs(original, mountpoint, hook)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("Serving %s", fs)
	log.Infof("Please run `fusermount -u %s` after using this, manually", mountpoint)
	if err = fs.Serve(); err != nil {
		log.Fatal(err)
	}
}
