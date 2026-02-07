package target

import (
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/syepes/network_exporter/pkg/tcp"
)

// TCPPing Object
type TCPPing struct {
	logger            *slog.Logger
	name              string
	host              string
	ip                string
	srcAddr           string
	port              string
	interval          time.Duration
	timeout           time.Duration
	count             int
	maxConcurrentJobs int
	labels            map[string]string
	result            *tcp.TCPPingResult
	stop              chan struct{}
	wg                sync.WaitGroup
	sync.RWMutex
}

// NewTCPPing starts a new monitoring goroutine
func NewTCPPing(logger *slog.Logger, startupDelay time.Duration, name string, host string, ip string, srcAddr string, port string, interval time.Duration, timeout time.Duration, count int, labels map[string]string, maxConcurrentJobs int) (*TCPPing, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	t := &TCPPing{
		logger:            logger,
		name:              name,
		host:              host,
		ip:                ip,
		srcAddr:           srcAddr,
		port:              port,
		interval:          interval,
		timeout:           timeout,
		count:             count,
		maxConcurrentJobs: maxConcurrentJobs,
		labels:            labels,
		stop:              make(chan struct{}),
		result: &tcp.TCPPingResult{
			DestAddr: host,
			DestIp:   ip,
			DestPort: port,
		},
	}
	t.wg.Add(1)
	go t.run(startupDelay)
	return t, nil
}

func (t *TCPPing) run(startupDelay time.Duration) {
	if startupDelay > 0 {
		select {
		case <-time.After(startupDelay):
		case <-t.stop:
			t.wg.Done()
			return
		}
	}

	waitChan := make(chan struct{}, t.maxConcurrentJobs)

	// Execute first probe immediately (after jitter delay)
	select {
	case <-t.stop:
		t.wg.Done()
		return
	default:
		waitChan <- struct{}{}
		go func() {
			t.tcpPing()
			<-waitChan
		}()
	}

	tick := time.NewTicker(t.interval)
	defer tick.Stop()

	for {
		select {
		case <-t.stop:
			t.wg.Done()
			return
		case <-tick.C:
			waitChan <- struct{}{}
			go func() {
				t.tcpPing()
				<-waitChan
			}()
		}
	}
}

// Stop gracefully stops the monitoring
func (t *TCPPing) Stop() {
	close(t.stop)
	t.wg.Wait()
}

func (t *TCPPing) tcpPing() {
	data, err := tcp.TCPPing(t.host, t.ip, t.srcAddr, t.port, t.count, t.timeout)
	if err != nil {
		t.logger.Error("TCP Ping failed", "type", "TCPPing", "func", "tcpPing", "err", err)
	}

	t.Lock()
	defer t.Unlock()
	data.SntSummary += t.result.SntSummary
	data.SntFailSummary += t.result.SntFailSummary
	data.SntTimeSummary += t.result.SntTimeSummary
	t.result = data

	bytes, err2 := json.Marshal(t.result)
	if err2 != nil {
		t.logger.Error("Failed to marshal result", "type", "TCPPing", "func", "tcpPing", "err", err2)
	}
	t.logger.Debug("TCP Ping result", "type", "TCPPing", "func", "tcpPing", "result", string(bytes))
}

// Compute returns the results of the TCP Ping metrics
func (t *TCPPing) Compute() *tcp.TCPPingResult {
	t.RLock()
	defer t.RUnlock()

	if t.result == nil {
		return nil
	}
	return t.result
}

// Name returns name
func (t *TCPPing) Name() string {
	t.RLock()
	defer t.RUnlock()
	return t.name
}

// Host returns host
func (t *TCPPing) Host() string {
	t.RLock()
	defer t.RUnlock()
	return t.host
}

// Ip returns ip
func (t *TCPPing) Ip() string {
	t.RLock()
	defer t.RUnlock()
	return t.ip
}

// Labels returns labels
func (t *TCPPing) Labels() map[string]string {
	t.RLock()
	defer t.RUnlock()
	return t.labels
}
