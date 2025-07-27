const fs = require("fs");

const filePath = process.argv[2];
const bufferSize = 64 * 1024; // 64 KB
const buffer = Buffer.alloc(bufferSize);

const fd = fs.openSync(filePath, "r");

let totalBytesRead = 0;
let bytesRead = 0;

console.time("diskRead");

do {
  bytesRead = fs.readSync(fd, buffer, 0, bufferSize, null); // null = read sequentially
  totalBytesRead += bytesRead;
} while (bytesRead > 0);

console.timeEnd("diskRead");

fs.closeSync(fd);

console.log(`Read ${totalBytesRead} bytes`);
console.log(`Read speed: ${(totalBytesRead / (1024 * 1024)).toFixed(2)} MB`);
