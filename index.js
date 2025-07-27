const {
  Worker,
  isMainThread,
  workerData,
  parentPort,
} = require("worker_threads");

const readline = require("node:readline");
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
    console.time("diskRead");

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
          console.timeEnd("diskRead");
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

  const processLine = (stationName, temperature) => {
    const existing = aggregations.get(stationName);
    if (existing) {
      existing.min = Math.min(existing.min, temperature);
      existing.max = Math.max(existing.max, temperature);
      existing.sum += temperature;
      existing.count++;
    } else {
      aggregations.set(stationName, {
        min: temperature,
        max: temperature,
        sum: temperature,
        count: 1,
      });
    }
  };

  const processChunk = (chunk) => {
    const chunkSize = chunk.length;
    for (let i = 0; i < chunkSize; i++) {
      if (chunk[i] == 0x3a) {
        // ;
        stationMode = false;
      } else if (chunk[i] == 0x0a) {
        // next line
        // process station and number
        const stationStr = stationName.toString("utf8", 0, stationLen);
        const temp = parseFloat(number.toString("ascii", 0, numLen));
        stationMode = true;
        stationLen = 0;
        numLen = 0;

        processLine(stationStr, temp);
      } else {
        if (stationMode) {
          stationLen++;
          stationName[i] = chunk[i];
        } else {
          numLen++;
          number[i] = chunk[i];
        }
      }
    }
  };

  stream.on("data", (chunk) => {
    processChunk(chunk);
    // const last = chunk.slice(chunk.length - 50);
    // console.log(last);
    // console.log(last.toString());
    // stream.destroy();
    // let chunkBuffer = Buffer.alloc(0);
    // while (null != (chunkBuffer = stream.read())) {
    //   chunkBuffer = Buffer.concat([leftOver, chunkBuffer ?? Buffer.alloc(0)]);
    //   let lastLFIndex = 0;
    //   let curLFIndex = 0;
    //   while (-1 < (curLFIndex = chunkBuffer.indexOf(0x0a, lastLFIndex))) {
    //     const buf = chunkBuffer.slice(lastLFIndex, curLFIndex);
    //     processLine(buf);
    //     lastLFIndex = curLFIndex + 1;
    //   }
    //   leftOver = chunkBuffer.slice(lastLFIndex);
    // }
    // console.log("----------------------chunkBuffer");
    // console.log(chunkBuffer.toString());
    // // console.log(chunkBuffer);
    // // console.log(chunkBuffer.indexOf(0x3b));
    // // console.log(chunkBuffer.indexOf(0x0a));
    // // console.log(chunkBuffer.slice(0, 13));
    // // console.log(chunkBuffer.slice(14).toString());
    // console.log("----------------------chunkBuffer");
    // stream.destroy();
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
  console.log("Received in pcr: ", aggregations.size);
  // console.log(aggregations);
  return;
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
