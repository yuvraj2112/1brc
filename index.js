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
    const finalOffsets = await fileSetup();
    const threads = new Set();
    const agg = new Map();
    for (let slice of finalOffsets) {
      threads.add(
        new Worker(__filename, { workerData: { file: fileName, ...slice } })
      );
    }

    for (let worker of threads) {
      worker.on("error", (err) => {
        throw err;
      });
      worker.on("exit", () => {
        threads.delete(worker);
        console.log(`Thread exiting, ${threads.size} running...`);
        if (threads.size === 0) {
          console.log("All threads ended");
          printCompiledResults(agg);
        }
      });
      worker.on("message", (map) => {
        // serialise to avoid race conditions
        console.log("Resolved: ", map.size);
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
  const stream = fs.createReadStream(file, { start, end });
  const lineStream = readline.createInterface(stream);

  const aggregations = new Map();

  lineStream.on("line", (line) => {
    const [stationName, temperatureStr] = line.split(";");
    // use integers for computation to avoid loosing precision
    const temperature = Math.floor(parseFloat(temperatureStr) * 10);
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
  });

  lineStream.on("close", () => {
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
  console.log(aggregations);
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
