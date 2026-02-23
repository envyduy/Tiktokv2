const admin = require("firebase-admin");
const { exec } = require("child_process");
const util = require("util");

const execPromise = util.promisify(exec);

const serviceAccount = JSON.parse(
  process.env.FIREBASE_SERVICE_ACCOUNT
);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const PYTHON_PATH = "python3"; // Railway sẽ dùng python global
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
// SLEEP FUNCTION
// ======================================

function sleep(ms) {

  return new Promise(resolve => setTimeout(resolve, ms));

}


// ======================================
// SCRAPE USER
// ======================================

async function scrapeUser(username) {

  try {

    console.log(`\nScraping: ${username}`);

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
        (video.thumbnails && video.thumbnails.length > 0
          ? video.thumbnails[video.thumbnails.length - 1].url
          : `https://p16-sign.tiktokcdn.com/obj/tos-maliva-p-0068/${video.id}.jpeg`);


      batch.set(videoRef, {

        id: video.id,
        desc: video.description || "",
        create_time: video.timestamp || null,
        thumbnail: thumbnail,
        url: video.webpage_url || `https://www.tiktok.com/@${username}/video/${video.id}`,
        uploader: username

      }, { merge: true });


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

    console.log(`Saved ${lines.length} videos snapshot`);

  }
  catch (err) {

    console.error(`Scrape error (${username}):`, err.message);

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

    console.log(`Top growth (${username}):`, growth.slice(0, 3));

  }
  catch (err) {

    console.error("Analyze error:", err.message);

  }

}


// ======================================
// RUN TRACKER
// ======================================

async function runTracker() {

  try {

    console.log("\n===============================");
    console.log("Tracker running:", getVietnamTime());
    console.log("===============================\n");

    const usersSnap = await db.collection("koc_users").get();

    if (usersSnap.empty) {

      console.log("No users found");

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

      await sleep(3000); // tránh spam

    }

    console.log("\nTracker cycle finished\n");

  }
  catch (err) {

    console.error("Tracker error:", err.message);

  }

}


// ======================================
// INFINITE LOOP SCHEDULER
// ======================================

async function startInfiniteTracker() {

  console.log("Infinite tracker started\n");

  while (true) {

    try {

      await runTracker();

    }
    catch (err) {

      console.error("Loop error:", err.message);

    }

    console.log("Sleeping 1 hour...\n");

    await sleep(60 * 60 * 1000);

  }

}


// ======================================
// START APP
// ======================================

startInfiniteTracker();
