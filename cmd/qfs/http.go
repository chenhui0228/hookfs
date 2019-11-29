package main

import (
	"github.com/osrg/hookfs/pkg/qfs"
	"github.com/plimble/ace"
)

type Req struct {
	PreOpen     uint16 `json:"pre_open"`
	PostOpen    uint16 `json:"post_open"`
	PreRead     uint16 `json:"pre_read"`
	PostRead    uint16 `json:"post_read"`
	PreWrite    uint16 `json:"pre_write"`
	PostWrite   uint16 `json:"post_write"`
	PreMkdir    uint16 `json:"pre_mkdir"`
	PostMkdir   uint16 `json:"post_mkdir"`
	PreRmdir    uint16 `json:"pre_rmdir"`
	PostRmdir   uint16 `json:"post_rmdir"`
	PreOpenDir  uint16 `json:"pre_open_dir"`
	PostOpenDir uint16 `json:"post_open_dir"`
	PreFsync    uint16 `json:"pre_fsync"`
	PostFsync   uint16 `json:"post_fsync"`
}

func HttpSrv(srv *Srv) {
	a := srv.a
	a.Use(middlewareFunc(srv.hook))
	a.POST("/status", func(c *ace.C) {
		c.JSON(200, nil)
	})
	a.Run(":32768")
}

func middlewareFunc(h *qfs.QfsHook) ace.HandlerFunc {
	return func(c *ace.C) {
		r := &Req{}
		c.ParseJSON(r)
		setQfsHookFuseStats(h, r)
	}
}

func setQfsHookFuseStats(h *qfs.QfsHook, r *Req) {
	h.FuseStats["PreOpen"] = r.PreOpen
	h.FuseStats["PostOpen"] = r.PostOpen
	h.FuseStats["PreRead"] = r.PreRead
	h.FuseStats["PostRead"] = r.PostRead
	h.FuseStats["PreWrite"] = r.PreWrite
	h.FuseStats["PostWrite"] = r.PostWrite
	h.FuseStats["PreMkdir"] = r.PreMkdir
	h.FuseStats["PostMkdir"] = r.PostMkdir
	h.FuseStats["PreRmdir"] = r.PreRmdir
	h.FuseStats["PostRmdir"] = r.PostRmdir
	h.FuseStats["PreOpenDir"] = r.PreOpenDir
	h.FuseStats["PostOpenDir"] = r.PostOpenDir
	h.FuseStats["PreFsync"] = r.PreFsync
	h.FuseStats["PostFsync"] = r.PostFsync
}
