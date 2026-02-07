package monitor

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/syepes/network_exporter/config"
	"github.com/syepes/network_exporter/pkg/common"
	"github.com/syepes/network_exporter/pkg/tcp"
	"github.com/syepes/network_exporter/target"
)

// TCPPing manages the goroutines responsible for collecting TCP Ping data
type TCPPing struct {
	logger            *slog.Logger
	sc                *config.SafeConfig
	resolver          *config.Resolver
	interval          time.Duration
	timeout           time.Duration
	count             int
	ipv6              bool
	maxConcurrentJobs int
	targets           map[string]*target.TCPPing
	mtx               sync.RWMutex
}

// NewTCPPing creates and configures a new Monitoring TCP Ping instance
func NewTCPPing(logger *slog.Logger, sc *config.SafeConfig, resolver *config.Resolver, ipv6 bool, maxConcurrentJobs int) *TCPPing {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return &TCPPing{
		logger:            logger,
		sc:                sc,
		resolver:          resolver,
		interval:          sc.Cfg.TCPPing.Interval.Duration(),
		timeout:           sc.Cfg.TCPPing.Timeout.Duration(),
		count:             sc.Cfg.TCPPing.Count,
		ipv6:              ipv6,
		maxConcurrentJobs: maxConcurrentJobs,
		targets:           make(map[string]*target.TCPPing),
	}
}

// Stop brings the monitoring gracefully to a halt
func (p *TCPPing) Stop() {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	for id := range p.targets {
		p.removeTarget(id)
	}
}

// AddTargets adds newly added targets from the configuration
func (p *TCPPing) AddTargets() {
	p.logger.Debug("Current Targets", "type", "TCPPing", "func", "AddTargets", "count", len(p.targets), "configured", countTargets(p.sc, "TCPPing"))

	targetActiveTmp := []string{}
	for _, v := range p.targets {
		targetActiveTmp = common.AppendIfMissing(targetActiveTmp, v.Name())
	}

	targetConfigTmp := []string{}
	for _, v := range p.sc.Cfg.Targets {
		if v.Type == "TCPPing" {
			conn := strings.Split(v.Host, ":")
			if len(conn) != 2 {
				p.logger.Warn("Skipping target, could not identify host", "type", "TCPPing", "func", "AddTargets", "host", v.Host, "name", v.Name)
				continue
			}
			ipAddrs, err := common.DestAddrs(context.Background(), conn[0], p.resolver.Resolver, p.resolver.Timeout, p.ipv6)
			if err != nil || len(ipAddrs) == 0 {
				p.logger.Warn("Skipping resolve target", "type", "TCPPing", "func", "AddTargets", "host", v.Host, "err", err)
			}
			for _, ipAddr := range ipAddrs {
				targetConfigTmp = common.AppendIfMissing(targetConfigTmp, v.Name+" "+ipAddr)
			}
		}
	}

	targetAdd := common.CompareList(targetActiveTmp, targetConfigTmp)
	p.logger.Debug("Target names to add", "type", "TCPPing", "func", "AddTargets", "targets", targetAdd)

	// Build a lookup map to avoid O(n²) complexity
	targetLookup := make(map[string]bool)
	for _, t := range targetAdd {
		targetLookup[t] = true
	}

	for _, target := range p.sc.Cfg.Targets {
		if target.Type != "TCPPing" {
			continue
		}

		conn := strings.Split(target.Host, ":")
		if len(conn) != 2 {
			p.logger.Warn("Skipping target, could not identify host", "type", "TCPPing", "func", "AddTargets", "host", target.Host, "name", target.Name)
			continue
		}

		ipAddrs, err := common.DestAddrs(context.Background(), conn[0], p.resolver.Resolver, p.resolver.Timeout, p.ipv6)
		if err != nil || len(ipAddrs) == 0 {
			p.logger.Warn("Skipping resolve target", "type", "TCPPing", "func", "AddTargets", "name", target.Name, "err", err)
			continue
		}

		for _, ipAddr := range ipAddrs {
			targetName := target.Name + " " + ipAddr
			if !targetLookup[targetName] {
				continue
			}
			// Add jitter to prevent thundering herd (0-10% of interval)
			jitter := time.Duration(rand.Int63n(int64(p.interval / 10)))
			err := p.AddTargetDelayed(targetName, conn[0], ipAddr, target.SourceIp, conn[1], target.Labels.Kv, jitter)
			if err != nil {
				p.logger.Warn("Skipping target", "type", "TCPPing", "func", "AddTargets", "host", target.Host, "ip", ipAddr, "err", err)
			}
		}
	}
}

// AddTarget adds a target to the monitored list
func (p *TCPPing) AddTarget(name string, host string, ip string, srcAddr string, port string, labels map[string]string) (err error) {
	return p.AddTargetDelayed(name, host, ip, srcAddr, port, labels, 0)
}

// AddTargetDelayed is AddTarget with a startup delay
func (p *TCPPing) AddTargetDelayed(name string, host string, ip string, srcAddr string, port string, labels map[string]string, startupDelay time.Duration) (err error) {
	p.logger.Info("Adding Target", "type", "TCPPing", "func", "AddTargetDelayed", "name", name, "host", host, "ip", ip, "port", port, "delay", startupDelay)

	p.mtx.Lock()
	defer p.mtx.Unlock()

	target, err := target.NewTCPPing(p.logger, startupDelay, name, host, ip, srcAddr, port, p.interval, p.timeout, p.count, labels, p.maxConcurrentJobs)
	if err != nil {
		return err
	}
	p.removeTarget(name)
	p.targets[name] = target
	return nil
}

// DelTargets deletes/stops the removed targets from the configuration
func (p *TCPPing) DelTargets() {
	p.logger.Debug("Current Targets", "type", "TCPPing", "func", "DelTargets", "count", len(p.targets), "configured", countTargets(p.sc, "TCPPing"))

	targetActiveTmp := []string{}
	for _, v := range p.targets {
		if v != nil {
			targetActiveTmp = common.AppendIfMissing(targetActiveTmp, v.Name())
		}
	}

	// Build a set of target names that are still in config (regardless of DNS resolution)
	targetNamesInConfig := make(map[string]bool)
	for _, v := range p.sc.Cfg.Targets {
		if v.Type == "TCPPing" {
			targetNamesInConfig[v.Name] = true
		}
	}

	targetConfigTmp := []string{}
	for _, v := range p.sc.Cfg.Targets {
		if v.Type == "TCPPing" {
			conn := strings.Split(v.Host, ":")
			if len(conn) != 2 {
				p.logger.Warn("Skipping target, could not identify host", "type", "TCPPing", "func", "DelTargets", "host", v.Host, "name", v.Name)
				continue
			}
			ipAddrs, err := common.DestAddrs(context.Background(), conn[0], p.resolver.Resolver, p.resolver.Timeout, p.ipv6)
			if err != nil || len(ipAddrs) == 0 {
				p.logger.Warn("DNS resolution failed, preserving existing targets", "type", "TCPPing", "func", "DelTargets", "host", v.Host, "err", err)
				// On DNS failure, preserve any active targets with matching name prefix
				for _, activeName := range targetActiveTmp {
					if strings.HasPrefix(activeName, v.Name+" ") {
						targetConfigTmp = common.AppendIfMissing(targetConfigTmp, activeName)
					}
				}
				continue
			}
			for _, ipAddr := range ipAddrs {
				targetConfigTmp = common.AppendIfMissing(targetConfigTmp, v.Name+" "+ipAddr)
			}
		}
	}

	targetDelete := common.CompareList(targetConfigTmp, targetActiveTmp)
	for _, targetName := range targetDelete {
		for _, t := range p.targets {
			if t == nil {
				continue
			}
			if t.Name() == targetName {
				// Only delete if the target name prefix is no longer in config
				namePart := strings.SplitN(targetName, " ", 2)[0]
				if !targetNamesInConfig[namePart] {
					p.RemoveTarget(targetName)
				}
			}
		}
	}
}

// RemoveTarget removes a target from the monitoring list
func (p *TCPPing) RemoveTarget(key string) {
	p.logger.Info("Removing Target", "type", "TCPPing", "func", "RemoveTarget", "target", key)
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.removeTarget(key)
}

// Stops monitoring a target and removes it from the list (if the list includes the target)
func (p *TCPPing) removeTarget(key string) {
	target, found := p.targets[key]
	if !found {
		return
	}
	target.Stop()
	delete(p.targets, key)
}

// CheckActiveTargets reads target if IP was changed (DNS record)
func (p *TCPPing) CheckActiveTargets() (err error) {
	p.logger.Debug("Current Targets", "type", "TCPPing", "func", "CheckActiveTargets", "count", len(p.targets), "configured", countTargets(p.sc, "TCPPing"))

	targetActiveTmp := make(map[string]string)
	for _, v := range p.targets {
		targetActiveTmp[v.Name()+" "+v.Ip()] = v.Ip()
	}

	for targetName, targetIp := range targetActiveTmp {
		for _, target := range p.sc.Cfg.Targets {
			if target.Type != "TCPPing" {
				continue
			}
			if !strings.HasPrefix(targetName, target.Name+" ") {
				continue
			}
			ipAddrs, err := common.DestAddrs(context.Background(), strings.Split(target.Host, ":")[0], p.resolver.Resolver, p.resolver.Timeout, p.ipv6)
			if err != nil || len(ipAddrs) == 0 {
				p.logger.Warn("DNS resolution failed, keeping existing target", "type", "TCPPing", "func", "CheckActiveTargets", "host", target.Host, "err", err)
				continue
			}

			if !common.ContainsString(ipAddrs, targetIp) {
				p.RemoveTarget(targetName)

				conn := strings.Split(target.Host, ":")
				if len(conn) != 2 {
					p.logger.Warn("Skipping target, could not identify host", "type", "TCPPing", "func", "CheckActiveTargets", "host", target.Host, "name", target.Name)
					continue
				}
				for _, ipAddr := range ipAddrs {
					// Add jitter to prevent thundering herd (0-10% of interval)
					jitter := time.Duration(rand.Int63n(int64(p.interval / 10)))
					err := p.AddTargetDelayed(target.Name+" "+ipAddr, conn[0], ipAddr, target.SourceIp, conn[1], target.Labels.Kv, jitter)
					if err != nil {
						p.logger.Warn("Skipping target", "type", "TCPPing", "func", "CheckActiveTargets", "host", target.Host, "err", err)
					}
				}
			}
		}
	}
	return nil
}

// ExportMetrics collects the metrics for each monitored target and returns it as a simple map
func (p *TCPPing) ExportMetrics() map[string]*tcp.TCPPingResult {
	m := make(map[string]*tcp.TCPPingResult)

	p.mtx.RLock()
	defer p.mtx.RUnlock()

	for _, target := range p.targets {
		name := target.Name()
		metrics := target.Compute()

		if metrics != nil {
			m[name] = metrics
		}
	}
	return m
}

// ExportLabels target labels
func (p *TCPPing) ExportLabels() map[string]map[string]string {
	l := make(map[string]map[string]string)

	p.mtx.RLock()
	defer p.mtx.RUnlock()

	for _, target := range p.targets {
		name := target.Name()
		labels := target.Labels()

		if labels != nil {
			l[name] = labels
		}
	}
	return l
}
