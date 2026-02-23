const admin = require("firebase-admin");
const { exec } = require("child_process");
const util = require("util");
const express = require("express");

const execPromise = util.promisify(exec);

// ============================
// FIREBASE INIT
// ============================

const serviceAccount = JSON.parse(
  process.env.FIREBASE_SERVICE_ACCOUNT
);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;


// ============================
// CONFIG
// ============================

const MAX_VIDEOS = 120;
const PORT = process.env.PORT || 10000;
const SECRET_KEY = process.env.SECRET_KEY;

const RETRY_COUNT = 3;
const RETRY_DELAY = 8000;
const USER_DELAY = 8000;

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
// EXEC WITH TIMEOUT + RETRY
// ============================

async function execYtDlp(profileUrl, retry = RETRY_COUNT) {

  try {

    console.log("Running yt-dlp:", profileUrl);

    const result = await execPromise(
      `yt-dlp --quiet --no-warnings --playlist-end ${MAX_VIDEOS} --dump-json ${profileUrl}`,
      {
        maxBuffer: 1024 * 1024 * 200,
        timeout: 1000 * 60 * 3
      }
    );

    if (!result.stdout || result.stdout.trim() === "") {
      throw new Error("Empty stdout");
    }

    return result.stdout;

  } catch (error) {

    console.log("yt-dlp error:", error.message);

    if (retry > 0) {

      console.log("Retrying in", RETRY_DELAY / 1000, "seconds...");

      await sleep(RETRY_DELAY);

      return execYtDlp(profileUrl, retry - 1);

    }

    console.log("yt-dlp failed completely");

    return null;

  }

}


// ============================
// SCRAPE USER
// ============================

async function scrapeUser(username) {

  try {

    console.log("Scraping:", username);

    const profileUrl = `https://www.tiktok.com/@${username}`;

    const stdout = await execYtDlp(profileUrl);

    if (!stdout) {

      console.log("No data:", username);

      return;

    }

    const lines = stdout.trim().split("\n");

    const now = getVietnamTime();

    const dateKey = now.toISOString().split("T")[0];
    const hourKey = now.getHours().toString().padStart(2, "0");

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


      // SAVE VIDEO

      batch.set(videoRef, {

        id: video.id,

        url: videoUrl,

        desc: video.description || "",

        create_time: video.timestamp || null,

        thumbnail: video.thumbnail || "",

        username: username,

        last_view_count: video.view_count || 0,

        last_like_count: video.like_count || 0,

        last_comment_count: video.comment_count || 0,

        last_repost_count: video.repost_count || 0,

        last_updated: FieldValue.serverTimestamp()

      }, { merge: true });

      operationCount++;


      // SAVE HOURLY SNAPSHOT

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

    console.log("SUCCESS:", username);

  } catch (error) {

    console.log("Scrape error:", username, error.message);

  }

}


// ============================
// RUN TRACKER
// ============================

async function runTracker() {

  console.log("Tracker started:", getVietnamTime());

  const usersSnap = await db.collection("koc_users").get();

  for (const userDoc of usersSnap.docs) {

    await scrapeUser(userDoc.id);

    await sleep(USER_DELAY);

  }

  console.log("Tracker completed");

}


// ============================
// API TRIGGER
// ============================

let isRunning = false;

app.get("/run-tracker", async (req, res) => {

  if (req.query.key !== SECRET_KEY) {

    return res.status(403).json({
      success: false,
      message: "Unauthorized"
    });

  }

  if (isRunning) {

    return res.json({
      success: false,
      message: "Already running"
    });

  }

  isRunning = true;

  try {

    await runTracker();

    res.json({
      success: true
    });

  } catch (error) {

    console.log(error);

    res.status(500).json({
      success: false
    });

  }

  isRunning = false;

});
