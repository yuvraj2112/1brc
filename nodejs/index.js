console.time("Process time: ");
const {
  Worker,
  isMainThread,
  workerData,
  parentPort,
} = require("worker_threads");

const fsp = require("node:fs/promises");
const fs = require("node:fs");
const os = require("os");

const MAX_LINE_LENGTH = 100 + 1 + 5 + 1;

const fileName = process.argv[2];

const calculateOffset = async (start, end, fileHandle) => {
  const { buffer } = await fileHandle.read({
    buffer: Buffer.alloc(MAX_LINE_LENGTH),
    length: MAX_LINE_LENGTH,
    position: end,
  });
  const diff = buffer.indexOf(10);
  return { start, end, diff };
};

const fileSetup = async () => {
  const stats = fs.statSync(fileName);
  const fileSizeInBytes = stats.size;
  console.log(fileSizeInBytes);

  const cpuInfo = os.cpus();
  const numberOfCores = cpuInfo.length;

  const threadCount = numberOfCores;
  const each = Math.floor(fileSizeInBytes / threadCount);

  const fh = await fsp.open(fileName);
  let byte = 0;
  let count = 1;
  const calcOffsets = [];
  while (count <= threadCount) {
    const start = byte;
    let end = each * count;
    end = end + (count == threadCount ? fileSizeInBytes - end : 0);
    calcOffsets.push(calculateOffset(start, end, fh));
    byte = end + 1;
    count += 1;
  }

  let lastDiff = 0;
  const diffList = await Promise.all(calcOffsets);
  const finalOffs = diffList.map(({ start, end, diff }) => {
    const offs = {
      start: start + lastDiff,
      end: end + diff,
    };
    lastDiff = diff;
    return offs;
  });

  await fh.close();

  return finalOffs;
};

if (isMainThread) {
  (async () => {
    const finalOffsets = await fileSetup();
    const threads = new Set();
    const agg = new Map();
    for (let slice of finalOffsets) {
      threads.add(
        new Worker(__filename, {
          workerData: { file: fileName, ...slice },
        })
      );
    }

    for (let worker of threads) {
      worker.on("error", (err) => {
        throw err;
      });
      worker.on("exit", () => {
        threads.delete(worker);
        if (threads.size === 0) {
          printCompiledResults(agg);
          console.timeEnd("Process time: ");
        }
      });
      worker.on("message", (map) => {
        // serialise to avoid race conditions
        map.forEach((value, key) => {
          const prev = agg.get(key);
          if (prev) {
            const newVal = {
              min: Math.min(prev.min, value.min),
              max: Math.max(prev.max, value.max),
              sum: prev.sum + value.sum,
              count: prev.count + value.count,
            };
            agg.set(key, newVal);
          } else {
            agg.set(key, value);
          }
        });
      });
    }
  })();
} else {
  const { start, end, file } = workerData;
  const aggregations = new Map();
  const stream = fs.createReadStream(file, { start, end });

  const stationName = Buffer.allocUnsafe(100);
  const number = Buffer.allocUnsafe(5);
  let stationMode = true;
  let stationLen = 0;
  let numLen = 0;

  const processLine = (station, temperature) => {
    const existing = aggregations.get(station);
    if (existing) {
      existing.min = Math.min(existing.min, temperature);
      existing.max = Math.max(existing.max, temperature);
      existing.sum += temperature;
      existing.count++;
    } else {
      aggregations.set(station, {
        min: temperature,
        max: temperature,
        sum: temperature,
        count: 1,
      });
    }
  };

  // number was ascii code e.g 48 for 0
  // we subtract 48 to normalize the number
  const parseBufferToDigit = (num) => {
    return num - 0x30;
  };

  const parseNumber = (length) => {
    // -99.9 to 99.9
    // - : -99.9 to -0.0 - 5 to 4
    // + : 0.0 to 99.9 - 3 to 4
    if (number[0] === 0x2d) {
      if (length === 5) {
        return -(
          parseBufferToDigit(number[1]) * 100 +
          parseBufferToDigit(number[2]) * 10 +
          parseBufferToDigit(number[4])
        );
      } else {
        // 4
        return -(
          parseBufferToDigit(number[1]) * 10 +
          parseBufferToDigit(number[3])
        );
      }
    } else {
      if (length === 3) {
        return (
          parseBufferToDigit(number[0]) * 10 + parseBufferToDigit(number[2])
        );
      } else {
        // 4
        return (
          parseBufferToDigit(number[0]) * 100 +
          parseBufferToDigit(number[1]) * 10 +
          parseBufferToDigit(number[3])
        );
      }
    }
  };

  const processChunk = (chunk) => {
    const chunkSize = chunk.length;
    for (let i = 0; i < chunkSize; i++) {
      const c = chunk[i];
      if (c == 0x3b) {
        // ;
        stationMode = false;
      } else if (c == 0x0a) {
        // LF
        // process collected station and number
        // const stationStr = stationName.toString("utf8", 0, stationLen);
        // const temp = parseNumber(numLen); // parseFloat(number.toString("ascii", 0, numLen));
        processLine(
          stationName.toString("utf8", 0, stationLen),
          parseNumber(numLen)
        );

        stationMode = true;
        stationLen = 0;
        numLen = 0;
      } else {
        if (stationMode) {
          stationName[stationLen] = c;
          ++stationLen;
        } else {
          number[numLen] = c;
          ++numLen;
        }
      }
    }
  };

  stream.on("data", (chunk) => {
    processChunk(chunk);
  });

  stream.on("end", () => {
    // console.log("Processed in stream: ", aggregations.size);
    parentPort.postMessage(aggregations);
  });
}

/**
 * @param {Map} aggregations
 *
 * @returns {void}
 */
function printCompiledResults(aggregations) {
  const sortedStations = Array.from(aggregations.keys()).sort();

  let result =
    "{" +
    sortedStations
      .map((station) => {
        const data = aggregations.get(station);
        return `${station}=${round(data.min / 10)}/${round(
          data.sum / 10 / data.count
        )}/${round(data.max / 10)}`;
      })
      .join(", ") +
    "}";

  console.log(result);
}

/**
 * @example
 * round(1.2345) // "1.2"
 * round(1.55) // "1.6"
 * round(1) // "1.0"
 *
 * @param {number} num
 *
 * @returns {string}
 */
function round(num) {
  const fixed = Math.round(10 * num) / 10;

  return fixed.toFixed(1);
}
