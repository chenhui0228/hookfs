package qfs

import (
	"github.com/osrg/hookfs/pkg/hookfs"
	log "github.com/sirupsen/logrus"
	"syscall"
	"time"
)

const timeoutThreshold uint16 = 1 << 15

const (
	EPERM  = 1
	EIO    = 5
	EACCES = 13
	EEXIST = 17
	ENOSPC = 28
)

type QfsHookContext struct {
	path string
}

type QfsHook struct {
	FuseStats map[string]uint16
}

// Init implements hookfs.HookWithInit
func (h *QfsHook) Init() error {
	log.WithFields(log.Fields{
		"h": h,
	}).Info("QfsInit: initializing")
	return nil
}

// PreOpen implements hookfs.HookOnOpen
func (h *QfsHook) PreOpen(path string, flags uint32) (bool, hookfs.HookContext, error) {
	var errCode string
	ctx := QfsHookContext{path: path}
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPreOpen: returning %s", errCode)
	}()
	if h.FuseStats["PreOpen"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PreOpen"]^timeoutThreshold) * time.Second)
		return false, ctx, nil
	}
	switch h.FuseStats["PreOpen"] {
	case EIO:
		errCode = "EIO"
		return true, ctx, syscall.EIO
	case EACCES:
		errCode = "EACCES"
		return true, ctx, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return true, ctx, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, ctx, syscall.EPERM
	//case ENOSPC:
	//	errCode = "ENOSPC"
	//	return true, ctx, syscall.ENOSPC
	default:
		return false, ctx, nil
	}
}

// PostOpen implements hookfs.HookOnOpen
func (h *QfsHook) PostOpen(realRetCode int32, ctx hookfs.HookContext) (bool, error) {
	var errCode string
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPostOpen: returning %s", errCode)
	}()
	if h.FuseStats["PostOpen"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PostOpen"]^timeoutThreshold) * time.Second)
		return false, nil
	}
	switch h.FuseStats["PostOpen"] {
	case EIO:
		errCode = "EIO"
		return true, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return true, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return true, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, syscall.EPERM
	//case ENOSPC:
	//	errCode = "ENOSPC"
	//	return true, syscall.ENOSPC
	default:
		return false, nil
	}
}

// PreRead implements hookfs.HookOnRead
func (h *QfsHook) PreRead(path string, length int64, offset int64) ([]byte, bool, hookfs.HookContext, error) {
	var errCode string
	ctx := QfsHookContext{path: path}
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPreRead: returning %s", errCode)
	}()
	if h.FuseStats["PreRead"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PreRead"]^timeoutThreshold) * time.Second)
		return nil, false, ctx, nil
	}
	switch h.FuseStats["PreRead"] {
	case EIO:
		errCode = "EIO"
		return nil, true, ctx, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return nil, true, ctx, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return nil, true, ctx, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return nil, true, ctx, syscall.EPERM
	//case ENOSPC:
	//	errCode = "ENOSPC"
	//	return nil, true, ctx, syscall.ENOSPC
	default:
		return nil, false, ctx, nil
	}
}

// PostRead implements hookfs.HookOnRead
func (h *QfsHook) PostRead(realRetCode int32, realBuf []byte, ctx hookfs.HookContext) ([]byte, bool, error) {
	var errCode string
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPostRead: returning %s", errCode)
	}()
	if h.FuseStats["PostRead"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PostRead"]^timeoutThreshold) * time.Second)
		return realBuf, false, nil
	}
	switch h.FuseStats["PostRead"] {
	case EIO:
		errCode = "EIO"
		return nil, true, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return nil, true, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return nil, true, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return nil, true, syscall.EPERM
	//case ENOSPC:
	//	errCode = "ENOSPC"
	//	return nil, true, syscall.ENOSPC
	default:
		return realBuf, false, nil
	}
}

// PreWrite implements hookfs.HookOnWrite
func (h *QfsHook) PreWrite(path string, buf []byte, offset int64) (bool, hookfs.HookContext, error) {
	var errCode string
	ctx := QfsHookContext{path: path}
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPreWrite: returning %s", errCode)
	}()
	if h.FuseStats["PreWrite"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PreWrite"]^timeoutThreshold) * time.Second)
		return false, ctx, nil
	}
	switch h.FuseStats["PreWrite"] {
	case EIO:
		errCode = "EIO"
		return true, ctx, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return true, ctx, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return true, ctx, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, ctx, syscall.EPERM
	//case ENOSPC:
	//	errCode = "ENOSPC"
	//	return true, ctx, syscall.ENOSPC
	default:
		return false, ctx, nil
	}
}

// PostWrite implements hookfs.HookOnWrite
func (h *QfsHook) PostWrite(realRetCode int32, ctx hookfs.HookContext) (bool, error) {
	var errCode string
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPostWrite: returning %s", errCode)
	}()
	if h.FuseStats["PostWrite"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PostWrite"]^timeoutThreshold) * time.Second)
		return false, nil
	}
	switch h.FuseStats["PostWrite"] {
	case EIO:
		errCode = "EIO"
		return true, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return true, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return true, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, syscall.EPERM
	case ENOSPC:
		errCode = "ENOSPC"
		return true, syscall.ENOSPC
	default:
		return false, nil
	}
}

// PreMkdir implements hookfs.HookOnMkdir
func (h *QfsHook) PreMkdir(path string, mode uint32) (bool, hookfs.HookContext, error) {
	var errCode string
	ctx := QfsHookContext{path: path}
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPreMkdir: returning %s", errCode)
	}()
	if h.FuseStats["PreMkdir"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PreMkdir"]^timeoutThreshold) * time.Second)
		return false, ctx, nil
	}
	switch h.FuseStats["PreMkdir"] {
	case EIO:
		errCode = "EIO"
		return true, ctx, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return true, ctx, syscall.EACCES
	case EEXIST:
		errCode = "EEXIST"
		return true, ctx, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, ctx, syscall.EPERM
	//case ENOSPC:
	//	return true, ctx, syscall.ENOSPC
	default:
		return false, ctx, nil
	}
}

// PostMkdir implements hookfs.HookOnMkdir
func (h *QfsHook) PostMkdir(realRetCode int32, ctx hookfs.HookContext) (bool, error) {
	var errCode string
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPostMkdir: returning %s", errCode)
	}()
	if h.FuseStats["PostMkdir"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PostMkdir"]^timeoutThreshold) * time.Second)
		return false, nil
	}
	switch h.FuseStats["PostMkdir"] {
	case EIO:
		errCode = "EIO"
		return true, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return true, syscall.EACCES
	case EEXIST:
		errCode = "EEXIST"
		return true, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, syscall.EPERM
	//case ENOSPC:
	//	errCode = "ENOSPC"
	//	return true, syscall.ENOSPC
	default:
		return false, nil
	}
}

// PreRmdir implements hookfs.HookOnRmdir
func (h *QfsHook) PreRmdir(path string) (bool, hookfs.HookContext, error) {
	var errCode string
	ctx := QfsHookContext{path: path}
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPreRmdir: returning %s", errCode)
	}()
	if h.FuseStats["PreRmdir"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PreRmdir"]^timeoutThreshold) * time.Second)
		return false, ctx, nil
	}
	switch h.FuseStats["PreRmdir"] {
	case EIO:
		errCode = "EIO"
		return true, ctx, syscall.EIO
	case EACCES:
		errCode = "EACCES"
		return true, ctx, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return true, ctx, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, ctx, syscall.EPERM
	//case ENOSPC:
	//	return true, ctx, syscall.ENOSPC
	default:
		return false, ctx, nil
	}
}

// PostRmdir implements hookfs.HookOnRmdir
func (h *QfsHook) PostRmdir(realRetCode int32, ctx hookfs.HookContext) (bool, error) {
	var errCode string
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPostRmdir: returning %s", errCode)
	}()
	if h.FuseStats["PostRmdir"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PostRmdir"]^timeoutThreshold) * time.Second)
		return false, nil
	}
	switch h.FuseStats["PostRmdir"] {
	case EIO:
		errCode = "EIO"
		return true, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return true, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return true, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, syscall.EPERM
	//case ENOSPC:
	//	errCode = "ENOSPC"
	//	return true, syscall.ENOSPC
	default:
		return false, nil
	}
}

// PreOpenDir implements hookfs.HookOnOpenDir
func (h *QfsHook) PreOpenDir(path string) (bool, hookfs.HookContext, error) {
	var errCode string
	ctx := QfsHookContext{path: path}
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPreOpenDir: returning %s", errCode)
	}()
	if h.FuseStats["PreOpenDir"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PreOpenDir"]^timeoutThreshold) * time.Second)
		return false, ctx, nil
	}
	switch h.FuseStats["PreOpenDir"] {
	case EIO:
		errCode = "EIO"
		return true, ctx, syscall.EIO
	case EACCES:
		errCode = "EACCES"
		return true, ctx, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return true, ctx, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, ctx, syscall.EPERM
	//case ENOSPC:
	//	return true, ctx, syscall.ENOSPC
	default:
		return false, ctx, nil
	}
}

// PostOpenDir implements hookfs.HookOnOpenDir
func (h *QfsHook) PostOpenDir(realRetCode int32, ctx hookfs.HookContext) (bool, error) {
	var errCode string
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPostOpenDir: returning %s", errCode)
	}()
	if h.FuseStats["PostOpenDir"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PostOpenDir"]^timeoutThreshold) * time.Second)
		return false, nil
	}
	switch h.FuseStats["PostOpenDir"] {
	case EIO:
		errCode = "EIO"
		return true, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return true, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return true, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, syscall.EPERM
	//case ENOSPC:
	//	errCode = "ENOSPC"
	//	return true, syscall.ENOSPC
	default:
		return false, nil
	}
}

// PreFsync implements hookfs.HookOnFsync
func (h *QfsHook) PreFsync(path string, flags uint32) (bool, hookfs.HookContext, error) {
	var errCode string
	ctx := QfsHookContext{path: path}
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPreFsync: returning %s", errCode)
	}()
	if h.FuseStats["PreFsync"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PreFsync"]^timeoutThreshold) * time.Second)
		return false, ctx, nil
	}
	switch h.FuseStats["PreFsync"] {
	case EIO:
		errCode = "EIO"
		return true, ctx, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return true, ctx, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return true, ctx, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, ctx, syscall.EPERM
	//case ENOSPC:
	//	return true, ctx, syscall.ENOSPC
	default:
		return false, ctx, nil
	}
}

// PostFsync implements hookfs.HookOnFsync
func (h *QfsHook) PostFsync(realRetCode int32, ctx hookfs.HookContext) (bool, error) {
	var errCode string
	defer func() {
		log.WithFields(log.Fields{
			"h":   h,
			"ctx": ctx,
		}).Info("QfsPostFsync: returning %s", errCode)
	}()
	if h.FuseStats["PostFsync"] > timeoutThreshold {
		errCode = "TIMEOUT"
		time.Sleep(time.Duration(h.FuseStats["PostFsync"]^timeoutThreshold) * time.Second)
		return false, nil
	}
	switch h.FuseStats["PostFsync"] {
	case EIO:
		errCode = "EIO"
		return true, syscall.EIO
	//case EACCES:
	//	errCode = "EACCES"
	//	return true, syscall.EACCES
	//case EEXIST:
	//	errCode = "EEXIST"
	//	return true, syscall.EEXIST
	case EPERM:
		errCode = "EPERM"
		return true, syscall.EPERM
	//case ENOSPC:
	//	errCode = "ENOSPC"
	//	return true, syscall.ENOSPC
	default:
		return false, nil
	}
}
