package main

import (
	// "bufio"
	// "bytes"
	"fmt"
	// "io"
	"log"
	"os"
	// "strconv"
	// "strings"
	"time"
	"math"
	"sort"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)	
	}
}

type CityInfo struct {
    min int
    max int
		total int
		count int
}

type CityMap map[string]CityInfo

var mapper = make(CityMap)

func updateMap(city string, temp int) {
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

func parseBufferToDigit(b byte) int {
	return int(b - 0x30)
}

func tempParse(number *[5]byte, length int) int {
	if number[0] == 0x2d { // '-'
		if length == 5 {
			return -(parseBufferToDigit(number[1])*100 +
				parseBufferToDigit(number[2])*10 +
				parseBufferToDigit(number[4]))
		}
		// length == 4
		return -(parseBufferToDigit(number[1])*10 +
			parseBufferToDigit(number[3]))
	} else {
		if length == 3 {
			return parseBufferToDigit(number[0])*10 +
				parseBufferToDigit(number[2])
		}
		// length == 4
		return parseBufferToDigit(number[0])*100 +
			parseBufferToDigit(number[1])*10 +
			parseBufferToDigit(number[3])
	}
}

func nameParse(nameBytes *[100]byte, idx int) string {
	return string((*nameBytes)[:idx])
}

func round(num float64) float64 {
	return math.Round(num*10) / 10.0
}

func printCompiledResults(aggregations CityMap) {
	stations := make([]string, 0, len(aggregations))
	for k := range aggregations {
		stations = append(stations, k)
	}
	sort.Strings(stations)

	result := "{"
	for i, station := range stations {
		data := aggregations[station]
		avg := float64(data.total) / 10.0 / float64(data.count)
		entry := fmt.Sprintf("%s=%f/%f/%f",
			station,
			round(float64(data.min)/10.0),
			round(avg),
			round(float64(data.max)/10.0),
		)
		if i > 0 {
			result += ", "
		}
		result += entry
	}
	result += "}"

	fmt.Println(result)

}

func main() {
	start := time.Now()
	file, err := os.Open("../../measurements.txt")
	check(err)
	defer file.Close()

	b := make([]byte, 8192)
	nameMode := true
	var temp [5]byte
	var name [100]byte
	nameIdx := 0
	tempIdx := 0

	for {
		n, _ := file.Read(b)
		if n == 0 {
			break
		}
		for i := 0; i < n; i += 1 {
			readByte := b[i]
			switch readByte {
				case 0x3b: // ;
					nameMode = false
				case 0x0a: // LE
					// process items here
					updateMap(nameParse(&name, nameIdx), tempParse(&temp, tempIdx))
					nameIdx = 0
					tempIdx = 0
					nameMode = true
				default:
					if nameMode {
						name[nameIdx] = readByte
						nameIdx += 1
					} else {
						temp[tempIdx] = readByte
						tempIdx += 1
					}
			}
		}
	}
	printCompiledResults(mapper)
	elapsed := time.Since(start)
	fmt.Printf("Execution took %s\n", elapsed)
}