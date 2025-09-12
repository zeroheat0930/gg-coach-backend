const {onSchedule} = require("firebase-functions/v2/scheduler");
const {initializeApp} = require("firebase-admin/app");
const {getFirestore} = require("firebase-admin/firestore");
const axios = require("axios");

initializeApp();
const db = getFirestore();

// defineString은 더 이상 사용하지 않습니다.

exports.updateWeaponMeta = onSchedule({
  schedule: "every 24 hours",
  region: "asia-northeast3",
  timeoutSeconds: 540,
  memory: "1GiB",
  secrets: ["PUBG_API_KEY"], // 함수가 이 비밀 키를 사용하도록 명시
}, async (event) => {
  console.log("Starting weapon meta update function (v2)...");

  try {
    // ### 여기가 수정된 부분입니다 ###
    // v2 secrets 방식에서는 process.env로 비밀 키에 접근합니다.
    const apiKey = process.env.PUBG_API_KEY;
    if (!apiKey) {
      throw new Error("PUBG_API_KEY secret is not set or available.");
    }

    const platformRegion = "pc-kakao";
    const seasonId = "division.bro.official.pc-2018-37";
    const leaderboardUrl =
      `https://api.pubg.com/shards/${platformRegion}` +
      `/leaderboards/${seasonId}/squad-fpp`;

    const headers = {
      "Authorization": `Bearer ${apiKey}`,
      "Accept": "application/vnd.api+json",
    };

    // playerHeaders는 이제 필요 없으니 삭제하고, headers를 재사용합니다.

    const leaderboardResponse = await axios.get(leaderboardUrl, {headers});

    const leaderboardData = leaderboardResponse.data;
    const playerIds = leaderboardData.data.relationships.players.data
        .map((p) => p.id);
    console.log(`Found ${playerIds.length} rankers.`);

    const matchIdsToAnalyze = new Set();
    for (const playerId of playerIds.slice(50, 70)) {
      const playerUrl =
        `https://api.pubg.com/shards/${platformRegion}/players/${playerId}`;
      const playerResponse = await axios.get(playerUrl, {headers});
      const matchData =
        playerResponse.data.data.relationships.matches.data;
      if (matchData && matchData.length > 0) {
        matchIdsToAnalyze.add(matchData[0].id);
      }
      if (matchIdsToAnalyze.size >= 10) break;
      await new Promise((resolve) => setTimeout(resolve, 6100));
    }
    console.log(`Found ${matchIdsToAnalyze.size} matches to analyze.`);

    const weaponCounts = {};
    for (const matchId of matchIdsToAnalyze) {
      const matchUrl =
        `https://api.pubg.com/shards/${platformRegion}/matches/${matchId}`;
      const matchResponse = await axios.get(matchUrl, {
        headers: {"Accept": "application/vnd.api+json"},
      });

      const assets = matchResponse.data.data.relationships.assets.data;
      if (assets && assets.length > 0) {
        const telemetryAsset = matchResponse.data.included
            .find((inc) => inc.id === assets[0].id);
        if (telemetryAsset) {
          const telemetryUrl = telemetryAsset.attributes.URL;
          const telemetryResponse = await axios.get(telemetryUrl, {
            headers: {"Accept-Encoding": "gzip"},
          });
          const telemetryEvents = telemetryResponse.data;
          for (const event of telemetryEvents) {
            if (
              event._T === "LogItemPickup" &&
              event.item &&
              event.item.category === "Weapon"
            ) {
              const weaponName = event.item.itemId;
              weaponCounts[weaponName] =
                (weaponCounts[weaponName] || 0) + 1;
            }
          }
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 6100));
    }
    console.log(
        `Finished analyzing weapons. Found ` +
        `${Object.keys(weaponCounts).length} types.`,
    );

    const topWeapons = Object.entries(weaponCounts)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10)
        .map(([name, count], index) => ({
          rank: index + 1,
          weaponId: name,
          pickCount: count,
        }));

    await db.collection("global_stats").doc("weapon_meta").set({
      updatedAt: new Date(),
      topWeapons: topWeapons,
    });

    console.log("Successfully updated weapon meta in Firestore!");
    return null;
  } catch (error) {
    console.error("Error executing function:", error);
    return null;
  }
});
