# 🌡️ 1BRC - 1 Billion Row Challenge (Node.js)

A performance-focused attempt at solving the **1 Billion Row Challenge** using **Node.js**.

---

## 🚀 Getting Started

This repo is designed to be used with the [1BRC base repo](https://github.com/gunnarmorling/1brc) for data generation and benchmarking setup.

> ⚠️ I used **Node.js v22**.

### 1️⃣ Clone This Repo

```bash
git clone https://github.com/yuvraj2112/1br_nodejs
cd 1br_nodejs
```

### 2️⃣ Generate Data (Follow 1BRC Instructions)

Follow the setup guide in the original [1BRC repository - Running the challenge](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#running-the-challenge) to generate the `measurements.txt` file using their generator.

Make sure the file is available in your working directory.

### 3️⃣ Run the Solution

```bash
node index.js measurements.txt
```

> This uses all available logical cores by default (via worker_threads), and will output aggregated results and processing time.

---

## 📚 Full Write-Up

I shared a full breakdown of this experiment, from baseline to optimizations, profiling to byte-level hacks — along with what didn’t work.

👉 [Read the dev.to article here](https://dev.to/yuvraj2112/1brc-in-nodejs-from-12-minutes-to-35-seconds-15mp)

---

## ⚙️ System Specs Used

- OS: Windows 11
- CPU: Intel i5 12th Gen – 10 Cores / 12 Threads
- RAM: 8 GB
- Disk: 256 GB NVMe SSD
- Node.js: v22.x

---

## 📜 License

MIT
