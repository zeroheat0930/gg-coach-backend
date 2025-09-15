const {onSchedule} = require("firebase-functions/v2/scheduler");
const {initializeApp} = require("firebase-admin/app");
const {getFirestore} = require("firebase-admin/firestore");
const axios = require("axios");

initializeApp();
const db = getFirestore();

exports.collectRecentMatchIds = onSchedule({
  schedule: "every 1 hours",
  region: "asia-northeast3",
  timeoutSeconds: 300,
  memory: "256MiB",
  secrets: ["PUBG_API_KEY"],
}, async (event) => {
  console.log("Starting to collect recent match IDs...");

  try {
    const apiKey = process.env.PUBG_API_KEY;
    if (!apiKey) throw new Error("PUBG_API_KEY secret is not set.");

    const platform = "steam";
    const samplesUrl = `https://api.pubg.com/shards/${platform}/samples`;

    const response = await axios.get(samplesUrl, {
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Accept": "application/vnd.api+json",
      },
    });

    // ### 여기가 수정된 부분입니다 ###
    if (response.status === 200) {
      const matchIds = response.data.data.relationships.matches.data
          .map((m) => m.id);

      await db.collection("samples").doc("recent_matches").set({
        updatedAt: new Date(),
        matchIds: matchIds,
      });

      console.log(`Successfully collected ${matchIds.length} match IDs.`);
    } else {
      // 200(성공)이 아닐 때만 에러를 발생시킵니다.
      throw new Error(`Failed to fetch samples: ${response.status}`);
    }
    return null;
  } catch (error) {
    console.error("Error collecting match IDs:", error);
    return null;
  }
});
