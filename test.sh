#!/bin/bash

cmd=$1

if [ "$cmd" == "f" ]; then
	if [ -f /bin/dockerd.bak ]; then
		exit -1
	fi
	mv /bin/dockerd /bin/dockerd.bak
	mv /bin/docker /bin/docker.bak
	cp $GOPATH/src/github.com/docker/docker/bundles/17.03.0-ce-rc1/binary-daemon/dockerd /bin/dockerd
	cp $GOPATH/src/github.com/docker/docker/bundles/17.03.0-ce-rc1/binary-client/docker /bin/docker
	systemctl restart docker
elif [ "$cmd" == "b" ]; then
	if ! [ -f /bin/dockerd.bak ]; then
		exit -1
	fi
	mv /bin/dockerd.bak /bin/dockerd
	mv /bin/docker.bak /bin/docker
	systemctl restart docker
fi
