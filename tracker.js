const admin = require("firebase-admin");
const { exec } = require("child_process");
const util = require("util");
const express = require("express");

const execPromise = util.promisify(exec);

// ======================================
// FIREBASE INIT
// ======================================

const serviceAccount = JSON.parse(
  process.env.FIREBASE_SERVICE_ACCOUNT
);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

// ======================================
// CONFIG
// ======================================

const PYTHON_PATH = "python3";
const MAX_VIDEOS = 120;
const PORT = process.env.PORT || 10000;
const TRACK_INTERVAL = 60 * 60 * 1000; // 1 hour

// ======================================
// EXPRESS SERVER (KEEP RENDER ALIVE)
// ======================================

const app = express();

app.get("/", (req, res) => {
  res.send("Tracker running");
});

app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});

// ======================================
// TIME
// ======================================

function getVietnamTime() {
  return new Date(
    new Date().toLocaleString("en-US", {
      timeZone: "Asia/Ho_Chi_Minh"
    })
  );
}

// ======================================
// SCRAPE USER
// ======================================

async function scrapeUser(username) {

  console.log("Scraping:", username);

  try {

    const command =
      `${PYTHON_PATH} -m yt_dlp ` +
      `--playlist-end ${MAX_VIDEOS} ` +
      `--dump-json ` +
      `--ignore-errors ` +
      `--no-warnings ` +
      `--impersonate chrome ` +
      `https://www.tiktok.com/@${username}`;

    const { stdout, stderr } = await execPromise(command, {
      maxBuffer: 1024 * 1024 * 200
    });

    if (stderr) {
      console.log("yt-dlp stderr:", stderr);
    }

    if (!stdout) {
      console.log("No videos found");
      return;
    }

    const lines = stdout.trim().split("\n");

    const now = getVietnamTime();

    const dateKey = now.toISOString().split("T")[0];
    const hourKey = now.getHours().toString();

    const batch = db.batch();

    let count = 0;

    for (const line of lines) {

      if (!line) continue;

      const video = JSON.parse(line);

      const videoRef = db
        .collection("koc_users")
        .doc(username)
        .collection("videos")
        .doc(video.id);

      batch.set(videoRef, {
        id: video.id,
        desc: video.description || "",
        create_time: video.timestamp || null,
        thumbnail: video.thumbnail || "",
        uploader: username
      }, { merge: true });

      const snapshotRef = videoRef
        .collection("daily")
        .doc(dateKey)
        .collection("hours")
        .doc(hourKey);

      batch.set(snapshotRef, {
        view_count: video.view_count || 0,
        timestamp: now
      });

      count++;
    }

    await batch.commit();

    console.log(`Saved ${count} videos for ${username}`);

  }
  catch (err) {

    console.log("SCRAPE ERROR:", err.message);

    if (err.stderr)
      console.log("stderr:", err.stderr);

  }

}

// ======================================
// TRACKER
// ======================================

let isRunning = false;

async function runTracker() {

  if (isRunning) {
    console.log("Tracker already running, skip");
    return;
  }

  isRunning = true;

  console.log("\n=== TRACKER START:", getVietnamTime(), "===\n");

  try {

    const usersSnap = await db.collection("koc_users").get();

    console.log("Total users:", usersSnap.size);

    for (const userDoc of usersSnap.docs) {

      await scrapeUser(userDoc.id);

      // delay tránh bị block
      await new Promise(r => setTimeout(r, 5000));

    }

    console.log("\n=== TRACKER DONE ===\n");

  }
  catch (err) {

    console.log("TRACKER ERROR:", err);

  }

  isRunning = false;

}

// ======================================
// LOOP (RENDER SAFE)
// ======================================

function trackerLoop() {

  runTracker();

  setTimeout(trackerLoop, TRACK_INTERVAL);

}

// ======================================
// START
// ======================================

console.log("Tracker started");

trackerLoop();
