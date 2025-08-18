package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
	"strconv"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)	
	}
}

type CityInfo struct {
    min float32
    max float32
		total float32
		count int
}

type CityMap map[string]CityInfo

var mapper = make(CityMap)

func updateMap(city string, temp float32) {
	value1, exists1 := mapper[city]
	if exists1 {
		value1.min = min(value1.min, temp)
		value1.max = max(value1.max, temp)
		value1.count += 1
		value1.total += temp
	} else {
		mapper[city] = CityInfo{
			min: temp,
			max: temp,
			total: temp,
			count: 1,
		}
	}
}

func main() {
	start := time.Now()
	file, err := os.Open("../measurements.txt")
	check(err)
	defer file.Close()
	
	scanner := bufio.NewScanner(file)
	check(scanner.Err())

	for scanner.Scan() {
		line := scanner.Text()
		city, temp, hasColon := strings.Cut(line,";")
		if !hasColon {
			continue
		}

		t, e := strconv.ParseFloat(temp, 32)
		check(e)
		updateMap(city, float32(t))
	}
	fmt.Println(mapper)
	elapsed := time.Since(start) // Calculate elapsed time
	fmt.Printf("Execution took %s\n", elapsed)
}