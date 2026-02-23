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
const FieldValue = admin.firestore.FieldValue;

const MAX_VIDEOS = 120;
const PORT = process.env.PORT || 10000;
const SECRET_KEY = process.env.SECRET_KEY; // dùng biến môi trường

const app = express();

app.get("/", (req, res) => {
  res.send("Tracker running");
});

app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});


// ============================
// TIME
// ============================

function getVietnamTime() {
  return new Date(
    new Date().toLocaleString("en-US", {
      timeZone: "Asia/Ho_Chi_Minh"
    })
  );
}


// ============================
// SLEEP
// ============================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}


// ============================
// SCRAPE USER
// ============================

async function scrapeUser(username) {

  console.log("Scraping:", username);

  const profileUrl = `https://www.tiktok.com/@${username}`;

  let stdout;

  try {

    const result = await execPromise(
      `yt-dlp --playlist-end ${MAX_VIDEOS} --dump-json ${profileUrl}`,
      { maxBuffer: 1024 * 1024 * 200 }
    );

    stdout = result.stdout;

  } catch (error) {

    console.log("Scrape failed:", error.message);
    return;

  }

  if (!stdout) return;

  const lines = stdout.trim().split("\n");

  const now = getVietnamTime();
  const dateKey = now.toISOString().split("T")[0];
  const hourKey = now.getHours().toString();

  let batch = db.batch();
  let operationCount = 0;

  for (const line of lines) {

    if (!line) continue;

    let video;

    try {
      video = JSON.parse(line);
    } catch {
      continue;
    }

    if (!video.id) continue;

    const videoUrl =
      video.webpage_url ||
      `https://www.tiktok.com/@${username}/video/${video.id}`;

    const videoRef = db
      .collection("koc_users")
      .doc(username)
      .collection("videos")
      .doc(video.id);

    batch.set(videoRef, {
      id: video.id,
      url: videoUrl,
      desc: video.description || "",
      create_time: video.timestamp || null,
      thumbnail: video.thumbnail || "",
      uploader: username,
      username: username,
      last_updated: FieldValue.serverTimestamp()
    }, { merge: true });

    operationCount++;

    const snapshotRef = videoRef
      .collection("daily")
      .doc(dateKey)
      .collection("hours")
      .doc(hourKey);

    batch.set(snapshotRef, {
      view_count: video.view_count || 0,
      like_count: video.like_count || 0,
      comment_count: video.comment_count || 0,
      repost_count: video.repost_count || 0,
      timestamp: FieldValue.serverTimestamp()
    });

    operationCount++;

    if (operationCount >= 400) {
      await batch.commit();
      batch = db.batch();
      operationCount = 0;
    }

  }

  if (operationCount > 0) {
    await batch.commit();
  }

  console.log("Saved videos for", username);

}


// ============================
// RUN TRACKER
// ============================

async function runTracker() {

  console.log("Tracker run:", getVietnamTime());

  const usersSnap = await db.collection("koc_users").get();

  for (const userDoc of usersSnap.docs) {

    await scrapeUser(userDoc.id);

    await sleep(3000);

  }

  console.log("Tracker completed");

}


// ============================
// MANUAL TRIGGER API
// ============================

let isRunning = false;

app.get("/run-tracker", async (req, res) => {

  // 🔐 check key
  if (req.query.key !== SECRET_KEY) {
    return res.status(403).json({
      success: false,
      message: "Unauthorized"
    });
  }

  // 🚫 chống spam
  if (isRunning) {
    return res.json({
      success: false,
      message: "Tracker already running"
    });
  }

  isRunning = true;

  try {

    await runTracker();

    res.json({
      success: true,
      message: "Tracker completed"
    });

  } catch (e) {

    console.log(e);

    res.status(500).json({
      success: false,
      message: "Tracker error"
    });

  }

  isRunning = false;

});
