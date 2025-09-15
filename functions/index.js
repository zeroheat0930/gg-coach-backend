// 자동 배포 테스트를 위한 주석
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

    console.log("Fetching recent matches for rankers in batches...");
    const matchIdsToAnalyze = new Set();
    const samplePlayerIds = allPlayerIds.slice(50, 150); // 100명의 랭커를 샘플링
    
    // 10명씩 묶어서 한 번에 요청
    for (let i = 0; i < samplePlayerIds.length; i += 10) {
      const batchIds = samplePlayerIds.slice(i, i + 10);
      const playerUrl = `https://api.pubg.com/shards/${platformRegion}/players?filter[playerIds]=${batchIds.join(",")}`;
      const playerResponse = await axios.get(playerUrl, {headers});
      
      const playersData = playerResponse.data.data;
      for (const player of playersData) {
        const matchData = player.relationships.matches.data;
        if (matchData && matchData.length > 0) {
          matchIdsToAnalyze.add(matchData[0].id);
        }
      }
      // 각 일괄 요청 사이에 6.1초 대기
      await new Promise((resolve) => setTimeout(resolve, 6100));
    }
    console.log(`Found ${matchIdsToAnalyze.size} unique matches to analyze.`);

    if (matchIdsToAnalyze.size === 0) {
        throw new Error("Could not find any recent matches from the rankers sample.");
    }

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
