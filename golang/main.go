package main

import (
	// "bufio"
	"bytes"
	"fmt"
	// "io"
	"log"
	"os"
	// "strconv"
	// "strings"
	"time"
	"math"
	"sync"
	"sort"
	"runtime"
)

type CityInfo struct {
	min int
	max int
	total int
	count int
}

type CityMap map[string]CityInfo

type FileChunkEdges struct {
	start int
	end int
}

func check(err error) {
	if err != nil {
		log.Fatal(err)	
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

func processValue(start int, end int, wg* sync.WaitGroup, outchan chan CityMap) {
	defer wg.Done()
	file, _ := os.Open("../../measurements.txt")
	defer file.Close()
	b := make([]byte, 8192)
	nameMode := true
	var temp [5]byte
	var name [100]byte
	nameIdx := 0
	tempIdx := 0

	readFrom := start

	var mapper = make(CityMap)

	updateMap := func(city string, temp int) {
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

	for ;readFrom < end; {
		n, _ := file.ReadAt(b, int64(readFrom))
		if n == 0 {
			break
		}
		if (readFrom + n) > end {
			n = end - readFrom
		}
		readFrom += n
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
	outchan <- mapper
}

func readFile(linesChan chan FileChunkEdges, wg* sync.WaitGroup) {
	defer wg.Done()

	file, _ := os.Open("../../measurements.txt")
	defer file.Close()
	fileInfo, err := file.Stat()
	check(err)

	fileSize := fileInfo.Size()
	cpuCount := runtime.NumCPU() + 2
	each := int(float64(fileSize / int64(cpuCount)))
	fmt.Println(fileSize, cpuCount)
	
	start, end := 0, each
	buf := make([]byte, 107)
	for i := 0; i < cpuCount; i += 1 {
		file.ReadAt(buf, int64(end))
		indexOfEnd := bytes.IndexByte(buf, 10)
		if ((end + indexOfEnd) > int(fileSize)) {
			end = (int(fileSize) - indexOfEnd)
		}
		linesChan <- FileChunkEdges{ start: start, end: end + indexOfEnd }
		start = end + indexOfEnd + 1
		end = start + each
	}
	close(linesChan)
}

func main() {
	start := time.Now()
	ch := make(chan FileChunkEdges, 14)
	outch := make(chan CityMap, 14)
	var wg sync.WaitGroup

	wg.Add(1)
	go readFile(ch, &wg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for elem := range ch {
			wg.Add(1)
      go processValue(elem.start, elem.end, &wg, outch)
    }
	}()

	var globalMap = make(CityMap) 

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for localMap := range outch {
			i += 1
			for city, info := range localMap {
				if existing, ok := globalMap[city]; ok {
					if info.min < existing.min {
						existing.min = info.min
					}
					if info.max > existing.max {
						existing.max = info.max
					}
					existing.total += info.total
					existing.count += info.count
					globalMap[city] = existing
				} else {
					globalMap[city] = info
				}
			}
			if i == 14 {
				close(outch)
			}
		}
	}()

	// go processValue(ch, &wg)
	wg.Wait()

	
	printCompiledResults(globalMap)
	elapsed := time.Since(start)
	fmt.Printf("Execution took %s\n", elapsed)
}