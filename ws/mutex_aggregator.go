package main

import (
	"sync"
	"time"
)

// Mutex-based aggregator that reports the global average temperature periodically
//
// Report the averagage temperature across all `k` weatherstations every `averagePeriod`
// seconds by sending a `WeatherReport` struct to the `out` channel. The aggregator should
// terminate upon receiving a singnal on the `quit` channel.
//
// Note! To receive credit, mutexAggregator must implement a mutex based solution.

func mutexAggregator(
	k int,
	averagePeriod float64,
	getWeatherData func(int, int) WeatherReport,
	out chan WeatherReport,
	quit chan struct{},
) {
	var lock = sync.Mutex{}
	var batch_global int = 0
	var total float64 = 0.0
	var count float64 = 0.0

	// create a timer to ensure we don't wait longer than averagePeriod
	timer := time.NewTicker(time.Duration(averagePeriod * float64(time.Second)))

	rpc_caller := func(batch int) {
		for i := 0; i < k; i++ {
			go func(i int, batch int) {
				// get the weather data from the weather station
				data := getWeatherData(i, batch)
				lock.Lock()
				if data.Batch == batch_global {
					total += data.Value
					count++
				}
				lock.Unlock()
			}(i, batch)
		}
	}

	// go calculate_total()
	rpc_caller(batch_global)

	// loop to receive data from the weather stations
	for {
		select {
		case <-timer.C:

			lock.Lock()
			avg := total / count
			lock.Unlock()

			out <- WeatherReport{Value: avg, Id: -1, Batch: batch_global}

			lock.Lock()
			batch_global++
			total = 0.0
			count = 0.0
			lock.Unlock()

			rpc_caller(batch_global)
		case <-quit:
			return
		}
	}

}
