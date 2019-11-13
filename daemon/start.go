package daemon

import (
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Sirupsen/logrus"
	apierrors "github.com/docker/docker/api/errors"
	"github.com/docker/docker/api/types"
	containertypes "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/container"
	"github.com/docker/docker/runconfig"
)

// ContainerStart starts a container.
func (daemon *Daemon) ContainerStart(name string, hostConfig *containertypes.HostConfig, checkpoint string, checkpointDir string) error {
	if checkpoint != "" && !daemon.HasExperimental() {
		return apierrors.NewBadRequestError(fmt.Errorf("checkpoint is only supported in experimental mode"))
	}

	container, err := daemon.GetContainer(name)
	if err != nil {
		return err
	}

	if container.IsPaused() {
		return fmt.Errorf("Cannot start a paused container, try unpause instead.")
	}

	if container.IsRunning() {
		err := fmt.Errorf("Container already started")
		return apierrors.NewErrorWithStatusCode(err, http.StatusNotModified)
	}

	// Windows does not have the backwards compatibility issue here.
	if runtime.GOOS != "windows" {
		// This is kept for backward compatibility - hostconfig should be passed when
		// creating a container, not during start.
		if hostConfig != nil {
			logrus.Warn("DEPRECATED: Setting host configuration options when the container starts is deprecated and has been removed in Docker 1.12")
			oldNetworkMode := container.HostConfig.NetworkMode
			if err := daemon.setSecurityOptions(container, hostConfig); err != nil {
				return err
			}
			if err := daemon.mergeAndVerifyLogConfig(&hostConfig.LogConfig); err != nil {
				return err
			}
			if err := daemon.setHostConfig(container, hostConfig); err != nil {
				return err
			}
			newNetworkMode := container.HostConfig.NetworkMode
			if string(oldNetworkMode) != string(newNetworkMode) {
				// if user has change the network mode on starting, clean up the
				// old networks. It is a deprecated feature and has been removed in Docker 1.12
				container.NetworkSettings.Networks = nil
				if err := container.ToDisk(); err != nil {
					return err
				}
			}
			container.InitDNSHostConfig()
		}
	} else {
		if hostConfig != nil {
			return fmt.Errorf("Supplying a hostconfig on start is not supported. It should be supplied on create")
		}
	}

	// check if hostConfig is in line with the current system settings.
	// It may happen cgroups are umounted or the like.
	if _, err = daemon.verifyContainerSettings(container.HostConfig, nil, false); err != nil {
		return err
	}
	// Adapt for old containers in case we have updates in this function and
	// old containers never have chance to call the new function in create stage.
	if hostConfig != nil {
		if err := daemon.adaptContainerSettings(container.HostConfig, false); err != nil {
			return err
		}
	}

	return daemon.containerStart(container, checkpoint, checkpointDir, true)
}

// Start starts a container
func (daemon *Daemon) Start(container *container.Container) error {
	return daemon.containerStart(container, "", "", true)
}

// containerStart prepares the container to run by setting up everything the
// container needs, such as storage and networking, as well as links
// between containers. The container is left waiting for a signal to
// begin running.
func (daemon *Daemon) containerStart(container *container.Container, checkpoint string, checkpointDir string, resetRestartManager bool) (err error) {
	start := time.Now()
	container.Lock()
	defer container.Unlock()

	if resetRestartManager && container.Running { // skip this check if already in restarting step and resetRestartManager==false
		return nil
	}

	if container.RemovalInProgress || container.Dead {
		return fmt.Errorf("Container is marked for removal and cannot be started.")
	}

	// if we encounter an error during start we need to ensure that any other
	// setup has been cleaned up properly
	defer func() {
		if err != nil {
			container.SetError(err)
			// if no one else has set it, make sure we don't leave it at zero
			if container.ExitCode() == 0 {
				container.SetExitCode(128)
			}
			container.ToDisk()

			container.Reset(false)

			daemon.Cleanup(container)
			// if containers AutoRemove flag is set, remove it after clean up
			if container.HostConfig.AutoRemove {
				container.Unlock()
				if err := daemon.ContainerRm(container.ID, &types.ContainerRmConfig{ForceRemove: true, RemoveVolume: true}); err != nil {
					logrus.Errorf("can't remove container %s: %v", container.ID, err)
				}
				container.Lock()
			}
		}
	}()

	if err := daemon.conditionalMountOnStart(container); err != nil {
		return err
	}

	// Make sure NetworkMode has an acceptable value. We do this to ensure
	// backwards API compatibility.
	container.HostConfig = runconfig.SetDefaultNetModeIfBlank(container.HostConfig)

	if err := daemon.initializeNetworking(container); err != nil {
		return err
	}
	fmt.Printf("%#v\n", container)
	// &container.Container{CommonContainer:container.CommonContainer{StreamConfig:(*stream.Config)(0xc000ef6c00), State:(*container.State)(0xc000176230), Root:"/home/docker_rt/containers/275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509", BaseFS:"/home/docker_rt/overlay2/6d14163512543a3a5e00695f7a19e47c014ac036a19c068e54704c27ffad556b/merged", RWLayer:(*layer.referencedRWLayer)(0xc000674220), ID:"275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509", Created:time.Time{wall:0x3211f2d, ext:63709254420, loc:(*time.Location)(nil)}, Managed:false, Path:"bash", Args:[]string{}, Config:(*container.Config)(0xc000cf0140), ImageID:"sha256:540a289bab6cb1bf880086a9b803cf0c4cefe38cbb5cdefa199b69614525199f", NetworkSettings:(*network.Settings)(0xc00045c500), LogPath:"", Name:"/nifty_booth", Driver:"overlay2", MountLabel:"", ProcessLabel:"", RestartCount:0, HasBeenStartedBefore:false, HasBeenManuallyStopped:false, MountPoints:map[string]*volume.MountPoint{}, HostConfig:(*container.HostConfig)(0xc000188800), ExecCommands:(*exec.Store)(0xc00126f680), SecretStore:exec.SecretGetter(nil), SecretReferences:[]*swarm.SecretReference(nil), LogDriver:logger.Logger(nil), LogCopier:(*logger.Copier)(nil), restartManager:restartmanager.RestartManager(nil), attachContext:(*container.attachContext)(0xc00126f6e0)}, AppArmorProfile:"", HostnamePath:"/home/docker_rt/containers/275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509/hostname", HostsPath:"/home/docker_rt/containers/275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509/hosts", ShmPath:"", ResolvConfPath:"/home/docker_rt/containers/275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509/resolv.conf", SeccompProfile:"", NoNewPrivileges:false}
	fmt.Printf("%#v\n", container.ExecCommands)
	// &exec.Store{commands:map[string]*exec.Config{}, RWMutex:sync.RWMutex{w:sync.Mutex{state:0, sema:0x0}, writerSem:0x0, readerSem:0x0, readerCount:0, readerWait:0}}
	fmt.Printf("%#v\n", container.HostConfig)
	// &container.HostConfig{Binds:[]string(nil), ContainerIDFile:"", LogConfig:container.LogConfig{Type:"json-file", Config:map[string]string{}}, NetworkMode:"default", PortBindings:nat.PortMap{}, RestartPolicy:container.RestartPolicy{Name:"no", MaximumRetryCount:0}, AutoRemove:true, VolumeDriver:"", VolumesFrom:[]string(nil), CapAdd:strslice.StrSlice(nil), CapDrop:strslice.StrSlice(nil), DNS:[]string{}, DNSOptions:[]string{}, DNSSearch:[]string{}, ExtraHosts:[]string(nil), GroupAdd:[]string(nil), IpcMode:"", Cgroup:"", Links:[]string{}, OomScoreAdj:0, PidMode:"", Privileged:false, PublishAllPorts:false, ReadonlyRootfs:false, SecurityOpt:[]string(nil), StorageOpt:map[string]string(nil), Tmpfs:map[string]string(nil), UTSMode:"", UsernsMode:"", ShmSize:67108864, Sysctls:map[string]string(nil), Runtime:"runc", ConsoleSize:[2]uint{0x0, 0x0}, Isolation:"", Resources:container.Resources{CPUShares:0, Memory:0, NanoCPUs:0, CgroupParent:"", BlkioWeight:0x0, BlkioWeightDevice:[]*blkiodev.WeightDevice(nil), BlkioDeviceReadBps:[]*blkiodev.ThrottleDevice(nil), BlkioDeviceWriteBps:[]*blkiodev.ThrottleDevice(nil), BlkioDeviceReadIOps:[]*blkiodev.ThrottleDevice(nil), BlkioDeviceWriteIOps:[]*blkiodev.ThrottleDevice(nil), CPUPeriod:0, CPUQuota:0, CPURealtimePeriod:0, CPURealtimeRuntime:0, CpusetCpus:"", CpusetMems:"", Devices:[]container.DeviceMapping{}, DiskQuota:0, KernelMemory:0, MemoryReservation:0, MemorySwap:0, MemorySwappiness:(*int64)(0xc000aada00), OomKillDisable:(*bool)(0xc000aada0a), PidsLimit:0, Ulimits:[]*units.Ulimit(nil), CPUCount:0, CPUPercent:0, IOMaximumIOps:0x0, IOMaximumBandwidth:0x0}, Mounts:[]mount.Mount(nil), Init:(*bool)(nil), InitPath:""}

	spec, err := daemon.createSpec(container)
	if err != nil {
		return err
	}
	fmt.Printf("%#v\n", spec)
	// &specs.Spec{Version:"1.0.0-rc2-dev", Platform:specs.Platform{OS:"linux", Arch:"amd64"}, Process:specs.Process{Terminal:true, ConsoleSize:specs.Box{Height:0x0, Width:0x0}, User:specs.User{UID:0x0, GID:0x0, AdditionalGids:[]uint32(nil), Username:""}, Args:[]string{"bash"}, Env:[]string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOSTNAME=275dfb25283f", "TERM=xterm", "NGINX_VERSION=1.17.5", "NJS_VERSION=0.3.6", "PKG_RELEASE=1~buster"}, Cwd:"/", Capabilities:[]string{"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FSETID", "CAP_FOWNER", "CAP_MKNOD", "CAP_NET_RAW", "CAP_SETGID", "CAP_SETUID", "CAP_SETFCAP", "CAP_SETPCAP", "CAP_NET_BIND_SERVICE", "CAP_SYS_CHROOT", "CAP_KILL", "CAP_AUDIT_WRITE"}, Rlimits:[]specs.Rlimit(nil), NoNewPrivileges:false, ApparmorProfile:"", SelinuxLabel:""}, Root:specs.Root{Path:"/home/docker_rt/overlay2/6d14163512543a3a5e00695f7a19e47c014ac036a19c068e54704c27ffad556b/merged", Readonly:false}, Hostname:"275dfb25283f", Mounts:[]specs.Mount{specs.Mount{Destination:"/proc", Type:"proc", Source:"proc", Options:[]string{"nosuid", "noexec", "nodev"}}, specs.Mount{Destination:"/dev", Type:"tmpfs", Source:"tmpfs", Options:[]string{"nosuid", "strictatime", "mode=755"}}, specs.Mount{Destination:"/dev/pts", Type:"devpts", Source:"devpts", Options:[]string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}}, specs.Mount{Destination:"/sys", Type:"sysfs", Source:"sysfs", Options:[]string{"nosuid", "noexec", "nodev", "ro"}}, specs.Mount{Destination:"/sys/fs/cgroup", Type:"cgroup", Source:"cgroup", Options:[]string{"ro", "nosuid", "noexec", "nodev"}}, specs.Mount{Destination:"/dev/mqueue", Type:"mqueue", Source:"mqueue", Options:[]string{"nosuid", "noexec", "nodev"}}, specs.Mount{Destination:"/etc/resolv.conf", Type:"bind", Source:"/home/docker_rt/containers/275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509/resolv.conf", Options:[]string{"rbind", "rprivate"}}, specs.Mount{Destination:"/etc/hostname", Type:"bind", Source:"/home/docker_rt/containers/275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509/hostname", Options:[]string{"rbind", "rprivate"}}, specs.Mount{Destination:"/etc/hosts", Type:"bind", Source:"/home/docker_rt/containers/275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509/hosts", Options:[]string{"rbind", "rprivate"}}, specs.Mount{Destination:"/dev/shm", Type:"bind", Source:"/home/docker_rt/containers/275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509/shm", Options:[]string{"rbind", "rprivate"}}}, Hooks:specs.Hooks{Prestart:[]specs.Hook{specs.Hook{Path:"/usr/bin/dockerd", Args:[]string{"libnetwork-setkey", "275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509", "c9b0bcd8067aab5fed3bb9bbe032137118e84a6ca1041474f69b31f178a04b2b"}, Env:[]string(nil), Timeout:(*int)(nil)}}, Poststart:[]specs.Hook(nil), Poststop:[]specs.Hook(nil)}, Annotations:map[string]string(nil), Linux:(*specs.Linux)(0xc0003e5d40), Solaris:(*specs.Solaris)(nil), Windows:(*specs.Windows)(nil)}

	createOptions, err := daemon.getLibcontainerdCreateOptions(container)
	if err != nil {
		return err
	}
	fmt.Printf("%#v\n", createOptions)
	// []libcontainerd.CreateOption{libcontainerd.runtime{path:"docker-runc", args:[]string(nil)}}

	if resetRestartManager {
		container.ResetRestartManager(true)
	}

	if checkpointDir == "" {
		checkpointDir = container.CheckpointDir()
	}
	fmt.Println(checkpointDir)
	// /home/docker_rt/containers/275dfb25283f443e4fef46556678396b95a0c0390d22a315c86560c13c351509/checkpoints

	if err := daemon.containerd.Create(container.ID, checkpoint, checkpointDir, *spec, container.InitializeStdio, createOptions...); err != nil {
		errDesc := grpc.ErrorDesc(err)
		contains := func(s1, s2 string) bool {
			return strings.Contains(strings.ToLower(s1), s2)
		}
		logrus.Errorf("Create container failed with error: %s", errDesc)
		// if we receive an internal error from the initial start of a container then lets
		// return it instead of entering the restart loop
		// set to 127 for container cmd not found/does not exist)
		if contains(errDesc, container.Path) &&
			(contains(errDesc, "executable file not found") ||
				contains(errDesc, "no such file or directory") ||
				contains(errDesc, "system cannot find the file specified")) {
			container.SetExitCode(127)
		}
		// set to 126 for container cmd can't be invoked errors
		if contains(errDesc, syscall.EACCES.Error()) {
			container.SetExitCode(126)
		}

		// attempted to mount a file onto a directory, or a directory onto a file, maybe from user specified bind mounts
		if contains(errDesc, syscall.ENOTDIR.Error()) {
			errDesc += ": Are you trying to mount a directory onto a file (or vice-versa)? Check if the specified host path exists and is the expected type"
			container.SetExitCode(127)
		}

		return fmt.Errorf("%s", errDesc)
	}

	containerActions.WithValues("start").UpdateSince(start)

	return nil
}

// Cleanup releases any network resources allocated to the container along with any rules
// around how containers are linked together.  It also unmounts the container's root filesystem.
func (daemon *Daemon) Cleanup(container *container.Container) {
	daemon.releaseNetwork(container)

	container.UnmountIpcMounts(detachMounted)

	if err := daemon.conditionalUnmountOnCleanup(container); err != nil {
		// FIXME: remove once reference counting for graphdrivers has been refactored
		// Ensure that all the mounts are gone
		if mountid, err := daemon.layerStore.GetMountID(container.ID); err == nil {
			daemon.cleanupMountsByID(mountid)
		}
	}

	if err := container.UnmountSecrets(); err != nil {
		logrus.Warnf("%s cleanup: failed to unmount secrets: %s", container.ID, err)
	}

	for _, eConfig := range container.ExecCommands.Commands() {
		daemon.unregisterExecCommand(container, eConfig)
	}

	if container.BaseFS != "" {
		if err := container.UnmountVolumes(daemon.LogVolumeEvent); err != nil {
			logrus.Warnf("%s cleanup: Failed to umount volumes: %v", container.ID, err)
		}
	}
	container.CancelAttachContext()
}
