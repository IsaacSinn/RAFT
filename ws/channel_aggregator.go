package main

import (
	"time"
)

func channelAggregator(
	k int,
	averagePeriod float64,
	getWeatherData func(int, int) WeatherReport,
	out chan WeatherReport,
	quit chan struct{},
) {
	var batch int = 0
	var total float64 = 0.0
	var count float64 = 0.0
	report := make(chan WeatherReport, k)

	// loop through each weather station
	for i := 0; i < k; i++ {
		go func(i int, batch int) {
			// get the weather data from the weather station
			report <- getWeatherData(i, batch)
		}(i, batch)
	}

	// create a timer to ensure we don't wait longer than averagePeriod
	timer := time.NewTicker(time.Duration(averagePeriod * float64(time.Second)))

	// loop to receive data from the weather stations
	for {
		select {
		case data := <-report:
			if data.Batch == batch {
				total += data.Value
				count++
			}
		case <-timer.C:
			// fmt.Println("Sending batch number", batch, "to the out channel.")
			// break out of the loop if the timer expires
			// compute the avg and send it to the out channel
			avg := total / count
			out <- WeatherReport{Value: avg, Id: -1, Batch: batch}
			batch++

			total = 0.0
			count = 0.0

			for i := 0; i < k; i++ {
				go func(i int, batch int) {
					// get the weather data from the weather station
					report <- getWeatherData(i, batch)

					// fmt.Println("Getting data from weather station", i, "for batch", batch)
				}(i, batch)
			}
		case <-quit:
			return
		}
	}

}
