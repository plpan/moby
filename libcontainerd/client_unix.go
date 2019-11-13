// +build linux solaris

package libcontainerd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
	containerd "github.com/docker/containerd/api/grpc/types"
	"github.com/docker/docker/pkg/idtools"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/net/context"
)

func (clnt *client) prepareBundleDir(uid, gid int) (string, error) {
	root, err := filepath.Abs(clnt.remote.stateDir)
	if err != nil {
		return "", err
	}
	if uid == 0 && gid == 0 {
		return root, nil
	}
	p := string(filepath.Separator)
	for _, d := range strings.Split(root, string(filepath.Separator))[1:] {
		p = filepath.Join(p, d)
		fi, err := os.Stat(p)
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		if os.IsNotExist(err) || fi.Mode()&1 == 0 {
			p = fmt.Sprintf("%s.%d.%d", p, uid, gid)
			if err := idtools.MkdirAs(p, 0700, uid, gid); err != nil && !os.IsExist(err) {
				return "", err
			}
		}
	}
	return p, nil
}

func (clnt *client) Create(containerID string, checkpoint string, checkpointDir string, spec specs.Spec, attachStdio StdioCallback, options ...CreateOption) (err error) {
	clnt.lock(containerID)
	defer clnt.unlock(containerID)

	if _, err := clnt.getContainer(containerID); err == nil {
		return fmt.Errorf("Container %s is already active", containerID)
	}
	fmt.Printf("%#v\n", clnt)
	// &libcontainerd.client{clientCommon:libcontainerd.clientCommon{backend:(*daemon.Daemon)(0xc000332200), containers:map[string]*libcontainerd.container{"13ba6e6a35f22205ad5bdc51c9f04262ca8992a67cd3354202206fc398986a23":(*libcontainerd.container)(0xc0003702d0), "44b9bd1128899319101310a8e85bf943c5cee4f4752b331972d481854eb30c3a":(*libcontainerd.container)(0xc0003703f0), "54188e9bd997fa37f69533564941e53601d21560a17fc571a21ee6bc3091da7f":(*libcontainerd.container)(0xc000240240), "7f8e9843f55d3f38aa19accf0ccf9c3af0436c0764c3a53b1374e26de7fafd26":(*libcontainerd.container)(0xc00041e630), "e60d0d5c879e600e11b14aecb9dda8165f19d7d050e9e46de4faea1459852f42":(*libcontainerd.container)(0xc0002042d0)}, locker:(*locker.Locker)(0xc0001be9d0), mapMutex:sync.RWMutex{w:sync.Mutex{state:0, sema:0x0}, writerSem:0x0, readerSem:0x0, readerCount:0, readerWait:0}}, remote:(*libcontainerd.remote)(0xc0004a0dd0), q:libcontainerd.queue{Mutex:sync.Mutex{state:0, sema:0x0}, fns:map[string]chan struct {}(nil)}, exitNotifiers:map[string]*libcontainerd.exitNotifier{}, liveRestore:true}
	fmt.Printf("%#v\n", clnt.remote)
	// &libcontainerd.remote{RWMutex:sync.RWMutex{w:sync.Mutex{state:0, sema:0x0}, writerSem:0x0, readerSem:0x0, readerCount:0, readerWait:0}, apiClient:(*types.aPIClient)(0xc00014e1d0), daemonPid:5671, stateDir:"/var/run/docker/libcontainerd", rpcAddr:"/var/run/docker/libcontainerd/docker-containerd.sock", startDaemon:true, closeManually:false, debugLog:true, rpcConn:(*grpc.ClientConn)(0xc0003a6b40), clients:[]*libcontainerd.client{(*libcontainerd.client)(0xc0000909c0), (*libcontainerd.client)(0xc000144660)}, eventTsPath:"/var/run/docker/libcontainerd/event.ts", runtime:"docker-runc", runtimeArgs:[]string(nil), daemonWaitCh:(chan struct {})(0xc000376060), liveRestore:true, oomScore:-500, restoreFromTimestamp:(*timestamp.Timestamp)(0xc000153b10)}

	uid, gid, err := getRootIDs(specs.Spec(spec))
	if err != nil {
		return err
	}
	dir, err := clnt.prepareBundleDir(uid, gid)
	if err != nil {
		return err
	}

	container := clnt.newContainer(filepath.Join(dir, containerID), options...)
	if err := container.clean(); err != nil {
		return err
	}
	fmt.Printf("%#v\n", container)
	// &libcontainerd.container{containerCommon:libcontainerd.containerCommon{process:libcontainerd.process{processCommon:libcontainerd.processCommon{client:(*libcontainerd.client)(0xc000144660), containerID:"275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509", friendlyName:"init", systemPid:0x0}, dir:"/var/run/docker/libcontainerd/275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509"}, processes:map[string]*libcontainerd.process{}}, pauseMonitor:libcontainerd.pauseMonitor{Mutex:sync.Mutex{state:0, sema:0x0}, waiters:map[string][]chan struct {}(nil)}, oom:false, runtime:"docker-runc", runtimeArgs:[]string(nil)}

	defer func() {
		if err != nil {
			container.clean()
			clnt.deleteContainer(containerID)
		}
	}()

	if err := idtools.MkdirAllAs(container.dir, 0700, uid, gid); err != nil && !os.IsExist(err) {
		return err
	}

	f, err := os.Create(filepath.Join(container.dir, configFilename))
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(spec); err != nil {
		return err
	}

	return container.start(checkpoint, checkpointDir, attachStdio)
}

func (clnt *client) Signal(containerID string, sig int) error {
	clnt.lock(containerID)
	defer clnt.unlock(containerID)
	_, err := clnt.remote.apiClient.Signal(context.Background(), &containerd.SignalRequest{
		Id:     containerID,
		Pid:    InitFriendlyName,
		Signal: uint32(sig),
	})
	return err
}

func (clnt *client) newContainer(dir string, options ...CreateOption) *container {
	container := &container{
		containerCommon: containerCommon{
			process: process{
				dir: dir,
				processCommon: processCommon{
					containerID:  filepath.Base(dir),
					client:       clnt,
					friendlyName: InitFriendlyName,
				},
			},
			processes: make(map[string]*process),
		},
	}
	for _, option := range options {
		if err := option.Apply(container); err != nil {
			logrus.Errorf("libcontainerd: newContainer(): %v", err)
		}
	}
	return container
}

type exitNotifier struct {
	id     string
	client *client
	c      chan struct{}
	once   sync.Once
}

func (en *exitNotifier) close() {
	en.once.Do(func() {
		close(en.c)
		en.client.mapMutex.Lock()
		if en == en.client.exitNotifiers[en.id] {
			delete(en.client.exitNotifiers, en.id)
		}
		en.client.mapMutex.Unlock()
	})
}
func (en *exitNotifier) wait() <-chan struct{} {
	return en.c
}
