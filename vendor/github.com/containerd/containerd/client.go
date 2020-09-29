/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package containerd

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	contentapi "github.com/containerd/containerd/api/services/content/v1"
	diffapi "github.com/containerd/containerd/api/services/diff/v1"
	eventsapi "github.com/containerd/containerd/api/services/events/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	introspectionapi "github.com/containerd/containerd/api/services/introspection/v1"
	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	namespacesapi "github.com/containerd/containerd/api/services/namespaces/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/api/services/tasks/v1"
	versionservice "github.com/containerd/containerd/api/services/version/v1"
	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/content"
	contentproxy "github.com/containerd/containerd/content/proxy"
	"github.com/containerd/containerd/defaults"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/events"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/pkg/dialer"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/containerd/remotes/docker/schema1"
	"github.com/containerd/containerd/snapshots"
	snproxy "github.com/containerd/containerd/snapshots/proxy"
	"github.com/containerd/typeurl"
	ptypes "github.com/gogo/protobuf/types"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func init() {
	const prefix = "types.containerd.io"
	// register TypeUrls for commonly marshaled external types
	major := strconv.Itoa(specs.VersionMajor)
	typeurl.Register(&specs.Spec{}, prefix, "opencontainers/runtime-spec", major, "Spec")
	typeurl.Register(&specs.Process{}, prefix, "opencontainers/runtime-spec", major, "Process")
	typeurl.Register(&specs.LinuxResources{}, prefix, "opencontainers/runtime-spec", major, "LinuxResources")
	typeurl.Register(&specs.WindowsResources{}, prefix, "opencontainers/runtime-spec", major, "WindowsResources")
}

// New returns a new containerd client that is connected to the containerd
// instance provided by address
func New(address string, opts ...ClientOpt) (*Client, error) {
	var copts clientOpts
	for _, o := range opts {
		if err := o(&copts); err != nil {
			return nil, err
		}
	}
	c := &Client{
		runtime: fmt.Sprintf("%s.%s", plugin.RuntimePlugin, runtime.GOOS),
	}
	if copts.services != nil {
		c.services = *copts.services
	}
	if address != "" {
		gopts := []grpc.DialOption{
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.FailOnNonTempDialError(true),
			grpc.WithBackoffMaxDelay(3 * time.Second),
			grpc.WithDialer(dialer.Dialer),

			// TODO(stevvooe): We may need to allow configuration of this on the client.
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(defaults.DefaultMaxRecvMsgSize)),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(defaults.DefaultMaxSendMsgSize)),
		}
		if len(copts.dialOptions) > 0 {
			gopts = copts.dialOptions
		}
		if copts.defaultns != "" {
			unary, stream := newNSInterceptors(copts.defaultns)
			gopts = append(gopts,
				grpc.WithUnaryInterceptor(unary),
				grpc.WithStreamInterceptor(stream),
			)
		}
		connector := func() (*grpc.ClientConn, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			conn, err := grpc.DialContext(ctx, dialer.DialAddress(address), gopts...)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to dial %q", address)
			}
			return conn, nil
		}
		conn, err := connector()
		if err != nil {
			return nil, err
		}
		c.conn, c.connector = conn, connector
	}
	if copts.services == nil && c.conn == nil {
		return nil, errors.New("no grpc connection or services is available")
	}
	return c, nil
}

// NewWithConn returns a new containerd client that is connected to the containerd
// instance provided by the connection
func NewWithConn(conn *grpc.ClientConn, opts ...ClientOpt) (*Client, error) {
	var copts clientOpts
	for _, o := range opts {
		if err := o(&copts); err != nil {
			return nil, err
		}
	}
	c := &Client{
		conn:    conn,
		runtime: fmt.Sprintf("%s.%s", plugin.RuntimePlugin, runtime.GOOS),
	}
	if copts.services != nil {
		c.services = *copts.services
	}
	return c, nil
}

// Client is the client to interact with containerd and its various services
// using a uniform interface
type Client struct {
	services
	connMu    sync.Mutex
	conn      *grpc.ClientConn
	runtime   string
	connector func() (*grpc.ClientConn, error)
}

// Reconnect re-establishes the GRPC connection to the containerd daemon
func (c *Client) Reconnect() error {
	if c.connector == nil {
		return errors.New("unable to reconnect to containerd, no connector available")
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	c.conn.Close()
	conn, err := c.connector()
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// IsServing returns true if the client can successfully connect to the
// containerd daemon and the healthcheck service returns the SERVING
// response.
// This call will block if a transient error is encountered during
// connection. A timeout can be set in the context to ensure it returns
// early.
func (c *Client) IsServing(ctx context.Context) (bool, error) {
	c.connMu.Lock()
	if c.conn == nil {
		c.connMu.Unlock()
		return false, errors.New("no grpc connection available")
	}
	c.connMu.Unlock()
	r, err := c.HealthService().Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.FailFast(false))
	if err != nil {
		return false, err
	}
	return r.Status == grpc_health_v1.HealthCheckResponse_SERVING, nil
}

// Containers returns all containers created in containerd
func (c *Client) Containers(ctx context.Context, filters ...string) ([]Container, error) {
	r, err := c.ContainerService().List(ctx, filters...)
	if err != nil {
		return nil, err
	}
	var out []Container
	for _, container := range r {
		out = append(out, containerFromRecord(c, container))
	}
	return out, nil
}

// NewContainer will create a new container in container with the provided id
// the id must be unique within the namespace
func (c *Client) NewContainer(ctx context.Context, id string, opts ...NewContainerOpts) (Container, error) {
	ctx, done, err := c.WithLease(ctx)
	if err != nil {
		return nil, err
	}
	defer done(ctx)

	container := containers.Container{
		ID: id,
		Runtime: containers.RuntimeInfo{
			Name: c.runtime,
		},
	}
	for _, o := range opts {
		if err := o(ctx, c, &container); err != nil {
			return nil, err
		}
	}
	// stupig: {"ID":"2cd9a849df586e5e7e132d099a0eecb4dcbd67ea6a98e6d009874ca166b9f3da","Labels":null,"Image":"","Runtime":{"Name":"io.containerd.runtime.v1.linux","Options":{"type_url":"containerd.linux.runc.RuncOptions","value":"Cgtkb2NrZXItcnVuYxIcL3Zhci9ydW4vZG9ja2VyL3J1bnRpbWUtcnVuYw=="}},"Spec":{"type_url":"types.containerd.io/opencontainers/runtime-spec/1/Spec","value":"eyJvY2lWZXJzaW9uIjoiMS4wLjEiLCJwcm9jZXNzIjp7InVzZXIiOnsidWlkIjowLCJnaWQiOjB9LCJhcmdzIjpbImRvY2tlcmluaXQiXSwiZW52IjpbIlBBVEg9L3Vzci9sb2NhbC9udmlkaWEvYmluOi91c3IvbG9jYWwvc2JpbjovdXNyL2xvY2FsL2JpbjovdXNyL3NiaW46L3Vzci9iaW46L3NiaW46L2JpbiIsIkhPU1ROQU1FPXN0dXBpZy1weTA0LXNmLTk3NmEwLTAuZG9ja2VyLnB5IiwiUE9EX05BTUVTUEFDRT1kZWZhdWx0IiwiTVlfTUVNX0xJTUlUPTIwNDgiLCJESURJRU5WX09ESU5fU0VSVklDRV9OQU1FPXN0dXBpZy5kaWRpY2xvdWQub3AuZGlkaS5jb20iLCJESURJRU5WX0REQ0xPVURfQ09OVEFJTkVSX1RZUEU9c3RhdGVmdWwiLCJESURJRU5WX0RFUFNfREVQTE9ZX1BBVEg9L2hvbWUveGlhb2p1L3Rlc3Qtc3R1cGlnIiwiTlNTX1NEQl9VU0VfQ0FDSEU9eWVzIiwiRElESUVOVl9PRElOX0hPU1RfTElEQz1weSIsIkRJRElFTlZfT0RJTl9JTlNUQU5DRV9UWVBFPWRkX2RvY2tlciIsIkRJRElFTlZfRERDTE9VRF9JTklUX0RFQlVHPTEiLCJESURJRU5WX0REQ0xPVURfR1JBQ0VQRVJJT0RfU0VDT05EUz0yIiwiUE9EX0lQPTEwLjEzMy40MS4xODAiLCJESURJRU5WX0REQ0xPVURfRU5WX1RZUEU9cHJlIiwiRElESUVOVl9PRElOX1NVPXB5MDQtcHJlLXN0dXBpZy12Lm9wLWRpZGljbG91ZC1zdHVwaWciLCJQT0RfTkFNRT1zdHVwaWctcHkwNC1zZi05NzZhMC0wIiwiTVlfQ1BVX0xJTUlUPTIwMDAiLCJESURJRU5WX09ESU5fSU5TVEFOQ0VfUVVPVEFfQ1BVPTIwMDAiLCJESURJRU5WX09ESU5fSU5TVEFOQ0VfUVVPVEFfTUVNPTIwNDgiLCJESURJRU5WX09ESU5fQ0xVU1RFUj1weTA0LXByZS1zdHVwaWctdiIsIkRJRElFTlZfRERDTE9VRF9SRUdJT049cHkwNCIsIkdPTUFYUFJPQ1M9MSIsIkxEX0xJQlJBUllfUEFUSD0vdXNyL2xvY2FsL252aWRpYS9saWI6L3Vzci9sb2NhbC9udmlkaWEvbGliNjQ6IiwiRElESUVOVl9QQVJFTlRfSU1BR0U9cmVnaXN0cnkueGlhb2p1a2VqaS5jb20vZGlkaW9ubGluZS9zcmUtZGlkaS1jZW50b3M3LWJhc2U6c3RhYmxlIiwiRElESUVOVl9PRElOX0RFUExPWV9NT0RVTEU9c3R1cGlnLXB5MDQiXSwiY3dkIjoiLyIsImNhcGFiaWxpdGllcyI6eyJib3VuZGluZyI6WyJDQVBfQ0hPV04iLCJDQVBfREFDX09WRVJSSURFIiwiQ0FQX0ZTRVRJRCIsIkNBUF9GT1dORVIiLCJDQVBfTUtOT0QiLCJDQVBfTkVUX1JBVyIsIkNBUF9TRVRHSUQiLCJDQVBfU0VUVUlEIiwiQ0FQX1NFVEZDQVAiLCJDQVBfU0VUUENBUCIsIkNBUF9ORVRfQklORF9TRVJWSUNFIiwiQ0FQX1NZU19DSFJPT1QiLCJDQVBfS0lMTCIsIkNBUF9BVURJVF9XUklURSIsIkNBUF9TWVNfUFRSQUNFIl0sImVmZmVjdGl2ZSI6WyJDQVBfQ0hPV04iLCJDQVBfREFDX09WRVJSSURFIiwiQ0FQX0ZTRVRJRCIsIkNBUF9GT1dORVIiLCJDQVBfTUtOT0QiLCJDQVBfTkVUX1JBVyIsIkNBUF9TRVRHSUQiLCJDQVBfU0VUVUlEIiwiQ0FQX1NFVEZDQVAiLCJDQVBfU0VUUENBUCIsIkNBUF9ORVRfQklORF9TRVJWSUNFIiwiQ0FQX1NZU19DSFJPT1QiLCJDQVBfS0lMTCIsIkNBUF9BVURJVF9XUklURSIsIkNBUF9TWVNfUFRSQUNFIl0sImluaGVyaXRhYmxlIjpbIkNBUF9DSE9XTiIsIkNBUF9EQUNfT1ZFUlJJREUiLCJDQVBfRlNFVElEIiwiQ0FQX0ZPV05FUiIsIkNBUF9NS05PRCIsIkNBUF9ORVRfUkFXIiwiQ0FQX1NFVEdJRCIsIkNBUF9TRVRVSUQiLCJDQVBfU0VURkNBUCIsIkNBUF9TRVRQQ0FQIiwiQ0FQX05FVF9CSU5EX1NFUlZJQ0UiLCJDQVBfU1lTX0NIUk9PVCIsIkNBUF9LSUxMIiwiQ0FQX0FVRElUX1dSSVRFIiwiQ0FQX1NZU19QVFJBQ0UiXSwicGVybWl0dGVkIjpbIkNBUF9DSE9XTiIsIkNBUF9EQUNfT1ZFUlJJREUiLCJDQVBfRlNFVElEIiwiQ0FQX0ZPV05FUiIsIkNBUF9NS05PRCIsIkNBUF9ORVRfUkFXIiwiQ0FQX1NFVEdJRCIsIkNBUF9TRVRVSUQiLCJDQVBfU0VURkNBUCIsIkNBUF9TRVRQQ0FQIiwiQ0FQX05FVF9CSU5EX1NFUlZJQ0UiLCJDQVBfU1lTX0NIUk9PVCIsIkNBUF9LSUxMIiwiQ0FQX0FVRElUX1dSSVRFIiwiQ0FQX1NZU19QVFJBQ0UiXX0sIm9vbVNjb3JlQWRqIjo5OTR9LCJyb290Ijp7InBhdGgiOiIvaG9tZS9kb2NrZXJfcnQvb3ZlcmxheTIvZTAyMmMyMGQ0ZTExODg3Njc4Yzg1MmU0YjM0YjY1NjgyODI1MTRmNjdiNjZlMzdhMjJlZTk5ZWIwODI5MGNiNC9tZXJnZWQifSwiaG9zdG5hbWUiOiJzdHVwaWctcHkwNC1zZi05NzZhMC0wLmRvY2tlci5weSIsIm1vdW50cyI6W3siZGVzdGluYXRpb24iOiIvcHJvYyIsInR5cGUiOiJwcm9jIiwic291cmNlIjoicHJvYyIsIm9wdGlvbnMiOlsibm9zdWlkIiwibm9leGVjIiwibm9kZXYiXX0seyJkZXN0aW5hdGlvbiI6Ii9kZXYiLCJ0eXBlIjoidG1wZnMiLCJzb3VyY2UiOiJ0bXBmcyIsIm9wdGlvbnMiOlsibm9zdWlkIiwic3RyaWN0YXRpbWUiLCJtb2RlPTc1NSIsInNpemU9NjU1MzZrIl19LHsiZGVzdGluYXRpb24iOiIvZGV2L3B0cyIsInR5cGUiOiJkZXZwdHMiLCJzb3VyY2UiOiJkZXZwdHMiLCJvcHRpb25zIjpbIm5vc3VpZCIsIm5vZXhlYyIsIm5ld2luc3RhbmNlIiwicHRteG1vZGU9MDY2NiIsIm1vZGU9MDYyMCIsImdpZD01Il19LHsiZGVzdGluYXRpb24iOiIvc3lzIiwidHlwZSI6InN5c2ZzIiwic291cmNlIjoic3lzZnMiLCJvcHRpb25zIjpbIm5vc3VpZCIsIm5vZXhlYyIsIm5vZGV2Iiwicm8iXX0seyJkZXN0aW5hdGlvbiI6Ii9zeXMvZnMvY2dyb3VwIiwidHlwZSI6ImNncm91cCIsInNvdXJjZSI6ImNncm91cCIsIm9wdGlvbnMiOlsicm8iLCJub3N1aWQiLCJub2V4ZWMiLCJub2RldiJdfSx7ImRlc3RpbmF0aW9uIjoiL2Rldi9tcXVldWUiLCJ0eXBlIjoibXF1ZXVlIiwic291cmNlIjoibXF1ZXVlIiwib3B0aW9ucyI6WyJub3N1aWQiLCJub2V4ZWMiLCJub2RldiJdfSx7ImRlc3RpbmF0aW9uIjoiL2Rldi90ZXJtaW5hdGlvbi1sb2ciLCJ0eXBlIjoiYmluZCIsInNvdXJjZSI6Ii92YXIvbGliL2t1YmVsZXQvcG9kcy8xMjMyMzkyOC0wMjMwLTExZWItYjRiMS0yNDZlOTY3YzRlZmMvY29udGFpbmVycy9zdHVwaWctcHkwNC1weTA0L2M5Y2M2N2E3Iiwib3B0aW9ucyI6WyJyYmluZCIsInJwcml2YXRlIl19LHsiZGVzdGluYXRpb24iOiIvZGV2L3NobSIsInR5cGUiOiJiaW5kIiwic291cmNlIjoiL2hvbWUvZG9ja2VyX3J0L2NvbnRhaW5lcnMvNjAxYzkyNWRjMjQ2YzUzOGYyYTYwODdjYWRhYWUwMDViNTgzY2RmOTViNDhhZWJkZDRhNDJhN2ExNmE0MjMzNy9tb3VudHMvc2htIiwib3B0aW9ucyI6WyJyYmluZCIsInJwcml2YXRlIl19LHsiZGVzdGluYXRpb24iOiIvZXRjL3Jlc29sdi5jb25mIiwidHlwZSI6ImJpbmQiLCJzb3VyY2UiOiIvaG9tZS9kb2NrZXJfcnQvY29udGFpbmVycy82MDFjOTI1ZGMyNDZjNTM4ZjJhNjA4N2NhZGFhZTAwNWI1ODNjZGY5NWI0OGFlYmRkNGE0MmE3YTE2YTQyMzM3L3Jlc29sdi5jb25mIiwib3B0aW9ucyI6WyJyYmluZCIsInJwcml2YXRlIl19LHsiZGVzdGluYXRpb24iOiIvZXRjL2hvc3RuYW1lIiwidHlwZSI6ImJpbmQiLCJzb3VyY2UiOiIvaG9tZS9kb2NrZXJfcnQvY29udGFpbmVycy82MDFjOTI1ZGMyNDZjNTM4ZjJhNjA4N2NhZGFhZTAwNWI1ODNjZGY5NWI0OGFlYmRkNGE0MmE3YTE2YTQyMzM3L2hvc3RuYW1lIiwib3B0aW9ucyI6WyJyYmluZCIsInJwcml2YXRlIl19LHsiZGVzdGluYXRpb24iOiIvZXRjL2hvc3RzIiwidHlwZSI6ImJpbmQiLCJzb3VyY2UiOiIvdmFyL2xpYi9rdWJlbGV0L3BvZHMvMTIzMjM5MjgtMDIzMC0xMWViLWI0YjEtMjQ2ZTk2N2M0ZWZjL2V0Yy1ob3N0cyIsIm9wdGlvbnMiOlsicmJpbmQiLCJycHJpdmF0ZSJdfSx7ImRlc3RpbmF0aW9uIjoiL2hvbWUveGlhb2p1L21ldGEiLCJ0eXBlIjoiYmluZCIsInNvdXJjZSI6Ii9ob21lL2hvc3RwYXRoL3N0dXBpZy1weTA0LXNmLTk3NmEwL3N0dXBpZy1weTA0LXNmLTk3NmEwLTAvaG9tZS94aWFvanUvbWV0YSIsIm9wdGlvbnMiOlsicmJpbmQiLCJycHJpdmF0ZSJdfSx7ImRlc3RpbmF0aW9uIjoiL2hvbWUveGlhb2p1L2RhdGExIiwidHlwZSI6ImJpbmQiLCJzb3VyY2UiOiIvaG9tZS9ob3N0cGF0aC9zdHVwaWctcHkwNC1zZi05NzZhMC9zdHVwaWctcHkwNC1zZi05NzZhMC0wL2hvbWUveGlhb2p1L2RhdGExIiwib3B0aW9ucyI6WyJyYmluZCIsInJwcml2YXRlIl19LHsiZGVzdGluYXRpb24iOiIvdmFyL3J1bi9zZWNyZXRzL2t1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQiLCJ0eXBlIjoiYmluZCIsInNvdXJjZSI6Ii92YXIvbGliL2t1YmVsZXQvcG9kcy8xMjMyMzkyOC0wMjMwLTExZWItYjRiMS0yNDZlOTY3YzRlZmMvdm9sdW1lcy9rdWJlcm5ldGVzLmlvfnNlY3JldC9kZWZhdWx0LXRva2VuLXpuZGpsIiwib3B0aW9ucyI6WyJyYmluZCIsInJvIiwicnByaXZhdGUiXX1dLCJsaW51eCI6eyJyZXNvdXJjZXMiOnsiZGV2aWNlcyI6W3siYWxsb3ciOmZhbHNlLCJhY2Nlc3MiOiJyd20ifSx7ImFsbG93Ijp0cnVlLCJ0eXBlIjoiYyIsIm1ham9yIjoxLCJtaW5vciI6NSwiYWNjZXNzIjoicndtIn0seyJhbGxvdyI6dHJ1ZSwidHlwZSI6ImMiLCJtYWpvciI6MSwibWlub3IiOjMsImFjY2VzcyI6InJ3bSJ9LHsiYWxsb3ciOnRydWUsInR5cGUiOiJjIiwibWFqb3IiOjEsIm1pbm9yIjo5LCJhY2Nlc3MiOiJyd20ifSx7ImFsbG93Ijp0cnVlLCJ0eXBlIjoiYyIsIm1ham9yIjoxLCJtaW5vciI6OCwiYWNjZXNzIjoicndtIn0seyJhbGxvdyI6dHJ1ZSwidHlwZSI6ImMiLCJtYWpvciI6NSwibWlub3IiOjAsImFjY2VzcyI6InJ3bSJ9LHsiYWxsb3ciOnRydWUsInR5cGUiOiJjIiwibWFqb3IiOjUsIm1pbm9yIjoxLCJhY2Nlc3MiOiJyd20ifSx7ImFsbG93IjpmYWxzZSwidHlwZSI6ImMiLCJtYWpvciI6MTAsIm1pbm9yIjoyMjksImFjY2VzcyI6InJ3bSJ9XSwibWVtb3J5Ijp7ImxpbWl0IjoyMTQ3NDgzNjQ4LCJzd2FwIjoyMTQ3NDgzNjQ4LCJkaXNhYmxlT09NS2lsbGVyIjpmYWxzZX0sImNwdSI6eyJzaGFyZXMiOjEwMjQsInF1b3RhIjoyMDAwMDAsInBlcmlvZCI6MTAwMDAwfSwicGlkcyI6eyJsaW1pdCI6MH0sImJsb2NrSU8iOnsid2VpZ2h0IjowfX0sImNncm91cHNQYXRoIjoiL2t1YmVwb2RzL2J1cnN0YWJsZS9wb2QxMjMyMzkyOC0wMjMwLTExZWItYjRiMS0yNDZlOTY3YzRlZmMvMmNkOWE4NDlkZjU4NmU1ZTdlMTMyZDA5OWEwZWVjYjRkY2JkNjdlYTZhOThlNmQwMDk4NzRjYTE2NmI5ZjNkYSIsIm5hbWVzcGFjZXMiOlt7InR5cGUiOiJtb3VudCJ9LHsidHlwZSI6Im5ldHdvcmsiLCJwYXRoIjoiL3Byb2MvODkxMDAvbnMvbmV0In0seyJ0eXBlIjoidXRzIn0seyJ0eXBlIjoicGlkIn0seyJ0eXBlIjoiaXBjIiwicGF0aCI6Ii9wcm9jLzg5MTAwL25zL2lwYyJ9XSwibWFza2VkUGF0aHMiOlsiL3Byb2MvYWNwaSIsIi9wcm9jL2tjb3JlIiwiL3Byb2Mva2V5cyIsIi9wcm9jL2xhdGVuY3lfc3RhdHMiLCIvcHJvYy90aW1lcl9saXN0IiwiL3Byb2MvdGltZXJfc3RhdHMiLCIvcHJvYy9zY2hlZF9kZWJ1ZyIsIi9wcm9jL3Njc2kiLCIvc3lzL2Zpcm13YXJlIl0sInJlYWRvbmx5UGF0aHMiOlsiL3Byb2MvYXNvdW5kIiwiL3Byb2MvYnVzIiwiL3Byb2MvZnMiLCIvcHJvYy9pcnEiLCIvcHJvYy9zeXMiLCIvcHJvYy9zeXNycS10cmlnZ2VyIl19fQ=="},"SnapshotKey":"","Snapshotter":"","CreatedAt":"0001-01-01T00:00:00Z","UpdatedAt":"0001-01-01T00:00:00Z","Extensions":null}
	r, err := c.ContainerService().Create(ctx, container)
	if err != nil {
		return nil, err
	}
	return containerFromRecord(c, r), nil
}

// LoadContainer loads an existing container from metadata
func (c *Client) LoadContainer(ctx context.Context, id string) (Container, error) {
	r, err := c.ContainerService().Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return containerFromRecord(c, r), nil
}

// RemoteContext is used to configure object resolutions and transfers with
// remote content stores and image providers.
type RemoteContext struct {
	// Resolver is used to resolve names to objects, fetchers, and pushers.
	// If no resolver is provided, defaults to Docker registry resolver.
	Resolver remotes.Resolver

	// Platforms defines which platforms to handle when doing the image operation.
	// If this field is empty, content for all platforms will be pulled.
	Platforms []string

	// Unpack is done after an image is pulled to extract into a snapshotter.
	// If an image is not unpacked on pull, it can be unpacked any time
	// afterwards. Unpacking is required to run an image.
	Unpack bool

	// Snapshotter used for unpacking
	Snapshotter string

	// Labels to be applied to the created image
	Labels map[string]string

	// BaseHandlers are a set of handlers which get are called on dispatch.
	// These handlers always get called before any operation specific
	// handlers.
	BaseHandlers []images.Handler

	// ConvertSchema1 is whether to convert Docker registry schema 1
	// manifests. If this option is false then any image which resolves
	// to schema 1 will return an error since schema 1 is not supported.
	ConvertSchema1 bool
}

func defaultRemoteContext() *RemoteContext {
	return &RemoteContext{
		Resolver: docker.NewResolver(docker.ResolverOptions{
			Client: http.DefaultClient,
		}),
		Snapshotter: DefaultSnapshotter,
	}
}

// Pull downloads the provided content into containerd's content store
func (c *Client) Pull(ctx context.Context, ref string, opts ...RemoteOpt) (Image, error) {
	pullCtx := defaultRemoteContext()
	for _, o := range opts {
		if err := o(c, pullCtx); err != nil {
			return nil, err
		}
	}
	store := c.ContentStore()

	ctx, done, err := c.WithLease(ctx)
	if err != nil {
		return nil, err
	}
	defer done(ctx)

	name, desc, err := pullCtx.Resolver.Resolve(ctx, ref)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to resolve reference %q", ref)
	}

	fetcher, err := pullCtx.Resolver.Fetcher(ctx, name)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get fetcher for %q", name)
	}

	var (
		schema1Converter *schema1.Converter
		handler          images.Handler
	)
	if desc.MediaType == images.MediaTypeDockerSchema1Manifest && pullCtx.ConvertSchema1 {
		schema1Converter = schema1.NewConverter(store, fetcher)
		handler = images.Handlers(append(pullCtx.BaseHandlers, schema1Converter)...)
	} else {
		// Get all the children for a descriptor
		childrenHandler := images.ChildrenHandler(store)
		// Set any children labels for that content
		childrenHandler = images.SetChildrenLabels(store, childrenHandler)
		// Filter children by platforms
		childrenHandler = images.FilterPlatforms(childrenHandler, pullCtx.Platforms...)

		handler = images.Handlers(append(pullCtx.BaseHandlers,
			remotes.FetchHandler(store, fetcher),
			childrenHandler,
		)...)
	}

	if err := images.Dispatch(ctx, handler, desc); err != nil {
		return nil, err
	}
	if schema1Converter != nil {
		desc, err = schema1Converter.Convert(ctx)
		if err != nil {
			return nil, err
		}
	}

	img := &image{
		client: c,
		i: images.Image{
			Name:   name,
			Target: desc,
			Labels: pullCtx.Labels,
		},
	}

	if pullCtx.Unpack {
		if err := img.Unpack(ctx, pullCtx.Snapshotter); err != nil {
			return nil, errors.Wrapf(err, "failed to unpack image on snapshotter %s", pullCtx.Snapshotter)
		}
	}

	is := c.ImageService()
	for {
		if created, err := is.Create(ctx, img.i); err != nil {
			if !errdefs.IsAlreadyExists(err) {
				return nil, err
			}

			updated, err := is.Update(ctx, img.i)
			if err != nil {
				// if image was removed, try create again
				if errdefs.IsNotFound(err) {
					continue
				}
				return nil, err
			}

			img.i = updated
		} else {
			img.i = created
		}
		return img, nil
	}
}

// Push uploads the provided content to a remote resource
func (c *Client) Push(ctx context.Context, ref string, desc ocispec.Descriptor, opts ...RemoteOpt) error {
	pushCtx := defaultRemoteContext()
	for _, o := range opts {
		if err := o(c, pushCtx); err != nil {
			return err
		}
	}

	pusher, err := pushCtx.Resolver.Pusher(ctx, ref)
	if err != nil {
		return err
	}

	return remotes.PushContent(ctx, pusher, desc, c.ContentStore(), pushCtx.Platforms, pushCtx.BaseHandlers...)
}

// GetImage returns an existing image
func (c *Client) GetImage(ctx context.Context, ref string) (Image, error) {
	i, err := c.ImageService().Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	return &image{
		client: c,
		i:      i,
	}, nil
}

// ListImages returns all existing images
func (c *Client) ListImages(ctx context.Context, filters ...string) ([]Image, error) {
	imgs, err := c.ImageService().List(ctx, filters...)
	if err != nil {
		return nil, err
	}
	images := make([]Image, len(imgs))
	for i, img := range imgs {
		images[i] = &image{
			client: c,
			i:      img,
		}
	}
	return images, nil
}

// Subscribe to events that match one or more of the provided filters.
//
// Callers should listen on both the envelope and errs channels. If the errs
// channel returns nil or an error, the subscriber should terminate.
//
// The subscriber can stop receiving events by canceling the provided context.
// The errs channel will be closed and return a nil error.
func (c *Client) Subscribe(ctx context.Context, filters ...string) (ch <-chan *events.Envelope, errs <-chan error) {
	return c.EventService().Subscribe(ctx, filters...)
}

// Close closes the clients connection to containerd
func (c *Client) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// NamespaceService returns the underlying Namespaces Store
func (c *Client) NamespaceService() namespaces.Store {
	if c.namespaceStore != nil {
		return c.namespaceStore
	}
	return NewNamespaceStoreFromClient(namespacesapi.NewNamespacesClient(c.conn))
}

// ContainerService returns the underlying container Store
func (c *Client) ContainerService() containers.Store {
	if c.containerStore != nil {
		return c.containerStore
	}
	return NewRemoteContainerStore(containersapi.NewContainersClient(c.conn))
}

// ContentStore returns the underlying content Store
func (c *Client) ContentStore() content.Store {
	if c.contentStore != nil {
		return c.contentStore
	}
	return contentproxy.NewContentStore(contentapi.NewContentClient(c.conn))
}

// SnapshotService returns the underlying snapshotter for the provided snapshotter name
func (c *Client) SnapshotService(snapshotterName string) snapshots.Snapshotter {
	if c.snapshotters != nil {
		return c.snapshotters[snapshotterName]
	}
	return snproxy.NewSnapshotter(snapshotsapi.NewSnapshotsClient(c.conn), snapshotterName)
}

// TaskService returns the underlying TasksClient
func (c *Client) TaskService() tasks.TasksClient {
	if c.taskService != nil {
		return c.taskService
	}
	return tasks.NewTasksClient(c.conn)
}

// ImageService returns the underlying image Store
func (c *Client) ImageService() images.Store {
	if c.imageStore != nil {
		return c.imageStore
	}
	return NewImageStoreFromClient(imagesapi.NewImagesClient(c.conn))
}

// DiffService returns the underlying Differ
func (c *Client) DiffService() DiffService {
	if c.diffService != nil {
		return c.diffService
	}
	return NewDiffServiceFromClient(diffapi.NewDiffClient(c.conn))
}

// IntrospectionService returns the underlying Introspection Client
func (c *Client) IntrospectionService() introspectionapi.IntrospectionClient {
	return introspectionapi.NewIntrospectionClient(c.conn)
}

// LeasesService returns the underlying Leases Client
func (c *Client) LeasesService() leasesapi.LeasesClient {
	if c.leasesService != nil {
		return c.leasesService
	}
	return leasesapi.NewLeasesClient(c.conn)
}

// HealthService returns the underlying GRPC HealthClient
func (c *Client) HealthService() grpc_health_v1.HealthClient {
	return grpc_health_v1.NewHealthClient(c.conn)
}

// EventService returns the underlying event service
func (c *Client) EventService() EventService {
	if c.eventService != nil {
		return c.eventService
	}
	return NewEventServiceFromClient(eventsapi.NewEventsClient(c.conn))
}

// VersionService returns the underlying VersionClient
func (c *Client) VersionService() versionservice.VersionClient {
	return versionservice.NewVersionClient(c.conn)
}

// Version of containerd
type Version struct {
	// Version number
	Version string
	// Revision from git that was built
	Revision string
}

// Version returns the version of containerd that the client is connected to
func (c *Client) Version(ctx context.Context) (Version, error) {
	c.connMu.Lock()
	if c.conn == nil {
		c.connMu.Unlock()
		return Version{}, errors.New("no grpc connection available")
	}
	c.connMu.Unlock()
	response, err := c.VersionService().Version(ctx, &ptypes.Empty{})
	if err != nil {
		return Version{}, err
	}
	return Version{
		Version:  response.Version,
		Revision: response.Revision,
	}, nil
}
