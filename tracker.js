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

const PYTHON_PATH = "python";
const MAX_VIDEOS = 120;


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

function sleep(ms) {

  return new Promise(resolve => setTimeout(resolve, ms));

}


// ======================================
// SCRAPE USER
// ======================================

async function scrapeUser(username) {

  try {

    console.log(`\nScraping ${username}`);

    const { stdout } = await execPromise(
      `${PYTHON_PATH} -m yt_dlp --playlist-end ${MAX_VIDEOS} --dump-json https://www.tiktok.com/@${username}`,
      { maxBuffer: 1024 * 1024 * 200 }
    );

    const lines = stdout.split("\n");

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

      const snapshotRef = videoRef
        .collection("daily")
        .doc(dateKey)
        .collection("hours")
        .doc(hourKey);

      const thumbnail =
        video.thumbnail ||
        `https://p16-sign.tiktokcdn.com/obj/tos-maliva-p-0068/${video.id}.jpeg`;

      batch.set(videoRef, {

        id: video.id,
        desc: video.description || "",
        create_time: video.timestamp || null,
        thumbnail,
        url: video.webpage_url,
        uploader: username

      }, { merge: true });

      batch.set(snapshotRef, {

        view_count: video.view_count || 0,
        like_count: video.like_count || 0,
        comment_count: video.comment_count || 0,
        timestamp: now

      });

      count++;

    }

    await batch.commit();

    console.log(`Saved ${count} videos`);

  }
  catch (err) {

    console.error(`Scrape error ${username}:`, err.message);

  }

}


// ======================================
// ANALYZE TOP GROWTH TODAY
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
        .orderBy("timestamp")
        .get();

      if (hoursSnap.size < 2) continue;

      const first = hoursSnap.docs[0].data();
      const last = hoursSnap.docs[hoursSnap.size - 1].data();

      growth.push({

        videoId: videoDoc.id,
        growth: last.view_count - first.view_count

      });

    }

    growth.sort((a,b)=>b.growth-a.growth);

    console.log(`\nTop 10 growth ${username}`);

    console.table(growth.slice(0,10));

  }
  catch(err){

    console.error("Analyze error:", err.message);

  }

}


// ======================================
// TRACK ALL USERS
// ======================================

async function runTracker() {

  console.log("\n======================");
  console.log("Tracker:", getVietnamTime());
  console.log("======================");

  const usersSnap = await db.collection("koc_users").get();

  if (usersSnap.empty){

    console.log("No users");
    return;

  }

  for (const userDoc of usersSnap.docs){

    const username = userDoc.id;

    try{

      await scrapeUser(username);

      await analyzeUser(username);

      await db.collection("koc_users")
        .doc(username)
        .update({
          last_scraped: getVietnamTime()
        });

    }
    catch(err){

      console.error("User failed:", username);

    }

    await sleep(5000);

  }

  console.log("\nCycle complete");

}


// ======================================
// INFINITE LOOP
// ======================================

async function start(){

  console.log("Tracker started");

  while(true){

    await runTracker();

    console.log("Sleeping 1 hour...\n");

    await sleep(60*60*1000);

  }

}

start();
