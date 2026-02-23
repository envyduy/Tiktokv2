const admin = require("firebase-admin");
const { exec } = require("child_process");
const util = require("util");
const express = require("express");

const execPromise = util.promisify(exec);

const serviceAccount = JSON.parse(
  process.env.FIREBASE_SERVICE_ACCOUNT
);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const PYTHON_PATH = "python";
const MAX_VIDEOS = 120;
const PORT = process.env.PORT || 10000;

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

    const { stdout } = await execPromise(
      `${PYTHON_PATH} -m yt_dlp --playlist-end ${MAX_VIDEOS} --dump-json https://www.tiktok.com/@${username}`,
      { maxBuffer: 1024 * 1024 * 200 }
    );

    const lines = stdout.trim().split("\n");

    const now = getVietnamTime();

    const dateKey = now.toISOString().split("T")[0];
    const hourKey = now.getHours().toString();

    const batch = db.batch();

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

    }

    await batch.commit();

    console.log(`✓ Saved ${lines.length} videos for ${username}`);

  }
  catch (err) {

    console.log("Scrape error:", err.message);

  }

}


// ======================================
// TRACKER
// ======================================

let isRunning = false;

async function runTracker() {

  if (isRunning) {
    console.log("Tracker already running, skip...");
    return;
  }

  isRunning = true;

  console.log("\n=== Tracker run:", getVietnamTime(), "===\n");

  try {

    const usersSnap = await db.collection("koc_users").get();

    console.log(`Found ${usersSnap.size} users`);

    for (const userDoc of usersSnap.docs) {

      await scrapeUser(userDoc.id);

    }

    console.log("\n=== Tracker cycle done ===\n");

  }
  catch (err) {

    console.log("Tracker error:", err.message);

  }

  isRunning = false;

}


// ======================================
// START TRACKER (NO SLEEP)
// ======================================

function startTracker() {

  console.log("Tracker started");

  // run immediately
  runTracker();

  // run every 1 hour
  setInterval(runTracker, 60 * 60 * 1000);

}

startTracker();
