package main

import (
	"sync"
	"time"
)

func channelAggregatorTest(
	k int,
	averagePeriod float64,
	getWeatherData func(int, int) WeatherReport,
	out chan WeatherReport,
	quit chan struct{},
) {
	var lock = sync.Mutex{}
	var batch int = 0
	var total float64 = 0.0
	var count float64 = 0.0
	report := make(chan WeatherReport, k)
	// create a timer to ensure we don't wait longer than averagePeriod
	timer := time.NewTicker(time.Duration(averagePeriod * float64(time.Second)))

	rpc_caller := func(batch int) {
		for i := 0; i < k; i++ {
			go func(i int, batch int) {
				// get the weather data from the weather station
				report <- getWeatherData(i, batch)
			}(i, batch)
		}
	}

	calculate_total := func() {
		for data := range report {
			lock.Lock()
			if data.Batch == batch {
				total += data.Value
				count++
			}
			lock.Unlock()
		}
	}

	go calculate_total()
	rpc_caller(batch)

	// loop to receive data from the weather stations
	for {
		select {
		case <-timer.C:

			lock.Lock()
			avg := total / count
			lock.Unlock()
			out <- WeatherReport{Value: avg, Id: -1, Batch: batch}
			lock.Lock()
			batch++
			lock.Unlock()

			total = 0.0
			count = 0.0
			rpc_caller(batch)
		case <-quit:
			return
		}
	}

}
