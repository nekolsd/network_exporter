package tcp

import (
	"math"
	"time"

	"github.com/syepes/network_exporter/pkg/common"
)

// TCPPing performs multiple TCP connection attempts and calculates RTT statistics
func TCPPing(destAddr string, ip string, srcAddr string, port string, count int, timeout time.Duration) (*TCPPingResult, error) {
	var result TCPPingResult
	result.DestAddr = destAddr
	result.DestIp = ip
	result.DestPort = port
	result.SrcIp = "0.0.0.0"

	var allTimes []time.Duration
	var succSum int
	var sumTime time.Duration
	var bestTime, worstTime time.Duration
	var success bool
	var lastErr error

	for i := 0; i < count; i++ {
		portResult, err := Port(destAddr, ip, srcAddr, port, timeout)
		if err != nil || !portResult.Success {
			if err != nil {
				lastErr = err
			}
			continue
		}

		success = true
		succSum++
		result.SrcIp = portResult.SrcIp

		elapsed := portResult.ConTime
		allTimes = append(allTimes, elapsed)

		if worstTime == time.Duration(0) || elapsed > worstTime {
			worstTime = elapsed
		}
		if bestTime == time.Duration(0) || elapsed < bestTime {
			bestTime = elapsed
		}
		sumTime += elapsed
	}

	result.Success = success
	if count > 0 {
		result.DropRate = float64(count-succSum) / float64(count)
	}
	result.SumTime = sumTime
	if succSum > 0 {
		result.AvgTime = sumTime / time.Duration(succSum)
	}
	result.BestTime = bestTime
	result.WorstTime = worstTime
	result.SquaredDeviationTime = time.Duration(math.Sqrt(common.TimeSquaredDeviation(allTimes)))
	result.UncorrectedSDTime = time.Duration(common.TimeUncorrectedDeviation(allTimes))
	result.CorrectedSDTime = time.Duration(common.TimeCorrectedDeviation(allTimes))
	result.RangeTime = time.Duration(common.TimeRange(allTimes))
	result.SntSummary = count
	result.SntFailSummary = count - succSum
	result.SntTimeSummary = time.Duration(common.TimeRange(allTimes))

	return &result, lastErr
}
