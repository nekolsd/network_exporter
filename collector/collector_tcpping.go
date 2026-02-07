package collector

import (
	"fmt"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/syepes/network_exporter/monitor"
	"github.com/syepes/network_exporter/pkg/tcp"
)

var (
	tcppingLabelNames         = []string{"name", "target", "target_ip", "source_ip", "port"}
	tcppingStatusDesc         = prometheus.NewDesc("tcpping_status", "TCP Ping Status", tcppingLabelNames, nil)
	tcppingRttDesc            = prometheus.NewDesc("tcpping_rtt_seconds", "TCP Round Trip Time in seconds", append(tcppingLabelNames, "type"), nil)
	tcppingSntSummaryDesc     = prometheus.NewDesc("tcpping_rtt_snt_count", "TCP Ping packet sent count", tcppingLabelNames, nil)
	tcppingSntFailSummaryDesc = prometheus.NewDesc("tcpping_rtt_snt_fail_count", "TCP Ping packet sent fail count", tcppingLabelNames, nil)
	tcppingSntTimeSummaryDesc = prometheus.NewDesc("tcpping_rtt_snt_seconds", "TCP Ping packet sent time total", tcppingLabelNames, nil)
	tcppingLossDesc           = prometheus.NewDesc("tcpping_loss_percent", "TCP Ping packet loss in percent", tcppingLabelNames, nil)
	tcppingTargetsDesc        = prometheus.NewDesc("tcpping_targets", "Number of active TCP Ping targets", nil, nil)
	tcppingStateDesc          = prometheus.NewDesc("tcpping_up", "TCP Ping exporter state", nil, nil)
	tcppingMutex              = &sync.Mutex{}
	// Descriptor cache for custom labels
	tcppingDescCache      = make(map[string]*tcppingDescriptorSet)
	tcppingDescCacheMutex sync.RWMutex
)

// tcppingDescriptorSet holds all descriptors for a specific label set
type tcppingDescriptorSet struct {
	status         *prometheus.Desc
	rtt            *prometheus.Desc
	sntSummary     *prometheus.Desc
	sntFailSummary *prometheus.Desc
	sntTimeSummary *prometheus.Desc
	loss           *prometheus.Desc
}

// getTCPPingDescriptors returns cached or creates new descriptors for a label set
func getTCPPingDescriptors(labels prometheus.Labels) *tcppingDescriptorSet {
	cacheKey := fmt.Sprintf("%v", labels)

	tcppingDescCacheMutex.RLock()
	if descSet, exists := tcppingDescCache[cacheKey]; exists {
		tcppingDescCacheMutex.RUnlock()
		return descSet
	}
	tcppingDescCacheMutex.RUnlock()

	tcppingDescCacheMutex.Lock()
	defer tcppingDescCacheMutex.Unlock()

	if descSet, exists := tcppingDescCache[cacheKey]; exists {
		return descSet
	}

	descSet := &tcppingDescriptorSet{
		status:         prometheus.NewDesc("tcpping_status", "TCP Ping Status", tcppingLabelNames, labels),
		rtt:            prometheus.NewDesc("tcpping_rtt_seconds", "TCP Round Trip Time in seconds", append(tcppingLabelNames, "type"), labels),
		sntSummary:     prometheus.NewDesc("tcpping_rtt_snt_count", "TCP Ping packet sent count", tcppingLabelNames, labels),
		sntFailSummary: prometheus.NewDesc("tcpping_rtt_snt_fail_count", "TCP Ping packet sent fail count", tcppingLabelNames, labels),
		sntTimeSummary: prometheus.NewDesc("tcpping_rtt_snt_seconds", "TCP Ping packet sent time total", tcppingLabelNames, labels),
		loss:           prometheus.NewDesc("tcpping_loss_percent", "TCP Ping packet loss in percent", tcppingLabelNames, labels),
	}
	tcppingDescCache[cacheKey] = descSet
	return descSet
}

// TCPPingCollector prom
type TCPPingCollector struct {
	Monitor *monitor.TCPPing
	metrics map[string]*tcp.TCPPingResult
	labels  map[string]map[string]string
}

// Describe prom
func (p *TCPPingCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- tcppingStatusDesc
	ch <- tcppingRttDesc
	ch <- tcppingLossDesc
	ch <- tcppingTargetsDesc
	ch <- tcppingStateDesc
}

// Collect prom
func (p *TCPPingCollector) Collect(ch chan<- prometheus.Metric) {
	tcppingMutex.Lock()
	defer tcppingMutex.Unlock()

	if m := p.Monitor.ExportMetrics(); len(m) > 0 {
		p.metrics = m
	}

	if l := p.Monitor.ExportLabels(); len(l) > 0 {
		p.labels = l
	}

	if len(p.metrics) > 0 {
		ch <- prometheus.MustNewConstMetric(tcppingStateDesc, prometheus.GaugeValue, 1)
	} else {
		ch <- prometheus.MustNewConstMetric(tcppingStateDesc, prometheus.GaugeValue, 0)
	}

	targets := []string{}
	for target, metric := range p.metrics {
		targets = append(targets, target)
		l := strings.SplitN(strings.SplitN(target, " ", 2)[0], " ", 2) // get name without ip and create slice
		l = append(l, metric.DestAddr)
		l = append(l, metric.DestIp)
		l = append(l, metric.SrcIp)
		l = append(l, metric.DestPort)
		l2 := prometheus.Labels(p.labels[target])

		// Get cached descriptors for this label set
		descs := getTCPPingDescriptors(l2)

		if metric.Success {
			ch <- prometheus.MustNewConstMetric(descs.status, prometheus.GaugeValue, 1, l...)
		} else {
			ch <- prometheus.MustNewConstMetric(descs.status, prometheus.GaugeValue, 0, l...)
		}

		ch <- prometheus.MustNewConstMetric(descs.rtt, prometheus.GaugeValue, metric.BestTime.Seconds(), append(l, "best")...)
		ch <- prometheus.MustNewConstMetric(descs.rtt, prometheus.GaugeValue, metric.AvgTime.Seconds(), append(l, "mean")...)
		ch <- prometheus.MustNewConstMetric(descs.rtt, prometheus.GaugeValue, metric.WorstTime.Seconds(), append(l, "worst")...)
		ch <- prometheus.MustNewConstMetric(descs.rtt, prometheus.GaugeValue, metric.SumTime.Seconds(), append(l, "sum")...)
		ch <- prometheus.MustNewConstMetric(descs.rtt, prometheus.GaugeValue, metric.SquaredDeviationTime.Seconds(), append(l, "sd")...)
		ch <- prometheus.MustNewConstMetric(descs.rtt, prometheus.GaugeValue, metric.UncorrectedSDTime.Seconds(), append(l, "usd")...)
		ch <- prometheus.MustNewConstMetric(descs.rtt, prometheus.GaugeValue, metric.CorrectedSDTime.Seconds(), append(l, "csd")...)
		ch <- prometheus.MustNewConstMetric(descs.rtt, prometheus.GaugeValue, metric.RangeTime.Seconds(), append(l, "range")...)
		ch <- prometheus.MustNewConstMetric(descs.sntSummary, prometheus.GaugeValue, float64(metric.SntSummary), l...)
		ch <- prometheus.MustNewConstMetric(descs.sntFailSummary, prometheus.GaugeValue, float64(metric.SntFailSummary), l...)
		ch <- prometheus.MustNewConstMetric(descs.sntTimeSummary, prometheus.GaugeValue, metric.SntTimeSummary.Seconds(), l...)
		ch <- prometheus.MustNewConstMetric(descs.loss, prometheus.GaugeValue, metric.DropRate, l...)
	}
	ch <- prometheus.MustNewConstMetric(tcppingTargetsDesc, prometheus.GaugeValue, float64(len(targets)))
}
