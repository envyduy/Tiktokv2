const express = require("express");
const admin = require("firebase-admin");
const { exec } = require("child_process");
const util = require("util");

const execPromise = util.promisify(exec);

const app = express();


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

const PYTHON_PATH = "python";
const MAX_VIDEOS = 120;


// ======================================
// GET VIETNAM TIME
// ======================================

function getVietnamTime() {
  return new Date(
    new Date().toLocaleString("en-US", {
      timeZone: "Asia/Ho_Chi_Minh"
    })
  );
}


// ======================================
// SLEEP
// ======================================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}


// ======================================
// SCRAPE USER
// ======================================

async function scrapeUser(username) {

  try {

    console.log(`Scraping ${username}`);

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


      const thumbnail =
        video.thumbnail ||
        (video.thumbnails?.length
          ? video.thumbnails[video.thumbnails.length - 1].url
          : `https://p16-sign.tiktokcdn.com/obj/tos-maliva-p-0068/${video.id}.jpeg`);


      // save video info
      batch.set(videoRef, {

        id: video.id,
        desc: video.description || "",
        create_time: video.timestamp || null,
        thumbnail,
        url: video.webpage_url || `https://www.tiktok.com/@${username}/video/${video.id}`,
        uploader: username

      }, { merge: true });


      // save hourly snapshot
      const snapshotRef = videoRef
        .collection("daily")
        .doc(dateKey)
        .collection("hours")
        .doc(hourKey);

      batch.set(snapshotRef, {

        view_count: video.view_count || 0,
        like_count: video.like_count || 0,
        comment_count: video.comment_count || 0,
        timestamp: now

      });

    }

    await batch.commit();

    console.log(`Saved ${lines.length} videos`);

  }
  catch (err) {

    console.error(`Error scraping ${username}`, err.message);

  }

}


// ======================================
// ANALYZE GROWTH
// ======================================

async function analyzeUser(username) {

  try {

    const today = getVietnamTime().toISOString().split("T")[0];

    const videosSnap = await db
      .collection("koc_users")
      .doc(username)
      .collection("videos")
      .get();

    const growth = [];

    for (const videoDoc of videosSnap.docs) {

      const hoursSnap = await videoDoc.ref
        .collection("daily")
        .doc(today)
        .collection("hours")
        .orderBy("timestamp", "desc")
        .limit(2)
        .get();

      if (hoursSnap.size < 2) continue;

      const latest = hoursSnap.docs[0].data();
      const previous = hoursSnap.docs[1].data();

      growth.push({

        videoId: videoDoc.id,
        delta: latest.view_count - previous.view_count

      });

    }

    growth.sort((a, b) => b.delta - a.delta);

    console.log("Top 10 growth:", growth.slice(0, 10));

  }
  catch (err) {

    console.error("Analyze error", err.message);

  }

}


// ======================================
// RUN TRACKER
// ======================================

async function runTracker() {

  console.log("\n===============================");
  console.log("Tracker run:", getVietnamTime());
  console.log("===============================\n");

  const usersSnap = await db.collection("koc_users").get();

  if (usersSnap.empty) {

    console.log("No users");

    return;

  }

  for (const userDoc of usersSnap.docs) {

    const username = userDoc.id;

    await scrapeUser(username);

    await analyzeUser(username);

    await db.collection("koc_users")
      .doc(username)
      .update({
        last_scraped: getVietnamTime()
      });

    await sleep(5000);

  }

  console.log("Tracker cycle done\n");

}


// ======================================
// EXPRESS SERVER (RENDER NEED THIS)
// ======================================

app.get("/", (req, res) => {

  res.send("Tracker running");

});


// ======================================
// START SERVER
// ======================================

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {

  console.log("Server running on port", PORT);

});


// ======================================
// START TRACKER LOOP (EVERY 1 HOUR)
// ======================================

// run immediately
runTracker();

// run every hour
setInterval(runTracker, 60 * 60 * 1000);
