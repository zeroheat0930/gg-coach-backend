const {onSchedule} = require("firebase-functions/v2/scheduler");
const {initializeApp} = require("firebase-admin/app");
const {getFirestore} = require("firebase-admin/firestore");
const axios = require("axios");

initializeApp();
const db = getFirestore();

exports.collectMatchData = onSchedule({
  schedule: "every 1 hours",
  region: "asia-northeast3",
  timeoutSeconds: 540,
  memory: "1GiB",
  secrets: ["PUBG_API_KEY"],
}, async (event) => {
  console.log("Starting to collect full match data...");

  try {
    const apiKey = process.env.PUBG_API_KEY;
    if (!apiKey) throw new Error("PUBG_API_KEY secret is not set.");

    const platform = "steam";
    const samplesUrl = `https://api.pubg.com/shards/${platform}/samples`;
    const headers = {
      "Authorization": `Bearer ${apiKey}`,
      "Accept": "application/vnd.api+json",
    };

    const samplesResponse = await axios.get(samplesUrl, {headers});
    if (samplesResponse.status !== 200) {
      const errorMsg = `Failed to fetch samples: ${samplesResponse.status}`;
      throw new Error(errorMsg);
    }
    const matchIds = samplesResponse.data.data.relationships.matches.data
        .map((m) => m.id);
    console.log(`Collected ${matchIds.length} recent match IDs.`);

    let newMatchesSaved = 0;
    const allWeaponCounts = {};

    for (const matchId of matchIds.slice(0, 15)) {
      const matchRef = db.collection("matches").doc(matchId);
      const doc = await matchRef.get();
      if (doc.exists) {
        console.log(`Match ${matchId} already exists. Skipping.`);
        continue;
      }

      const matchUrl =
        `https://api.pubg.com/shards/${platform}/matches/${matchId}`;
      const matchResponse = await axios.get(matchUrl, {
        headers: {"Accept": "application/vnd.api+json"},
      });

      if (matchResponse.status === 200) {
        const matchData = matchResponse.data;
        const attributes = matchData.data.attributes;
        const included = matchData.included;
        const rosters = included.filter((inc) => inc.type === "roster");
        const participants = included
            .filter((inc) => inc.type === "participant");
        const winningRoster =
          rosters.find((r) => r.attributes.won === "true");
        let winningTeamMembers = [];
        if (winningRoster && winningRoster.relationships) {
          const winnerIds = winningRoster.relationships.participants
              .data.map((p) => p.id);
          winningTeamMembers = participants
              .filter((p) => winnerIds.includes(p.id))
              .map((p) => p.attributes.stats.name);
        }

        await matchRef.set({
          map: attributes.mapName || "Unknown",
          gameMode: attributes.gameMode || "Unknown",
          duration: attributes.duration || 0,
          createdAt: new Date(attributes.createdAt),
          isRanked: attributes.isRanked || false,
          totalTeams: rosters.length,
          winningTeam: winningTeamMembers,
        });

        for (const p of participants) {
          const stats = p.attributes.stats;
          const pId = stats.playerId;
          if (pId) {
            const pRef = matchRef.collection("participants").doc(pId);
            await pRef.set({
              nickname: stats.name || "Unknown",
              rank: stats.winPlace || 0,
              kills: stats.kills || 0,
              damage: stats.damageDealt || 0,
              assists: stats.assists || 0,
              DBNOs: stats.DBNOs || 0,
              headshotKills: stats.headshotKills || 0,
              longestKill: stats.longestKill || 0,
              timeSurvived: stats.timeSurvived || 0,
              revives: stats.revives || 0,
              heals: stats.heals || 0,
              boosts: stats.boosts || 0,
            });
          }
        }

        const assets = matchData.data.relationships.assets.data;
        if (assets && assets.length > 0) {
          const telemetryAsset = included.find((inc) => inc.id === assets[0].id);
          if (telemetryAsset && telemetryAsset.attributes) {
            const telemetryUrl = telemetryAsset.attributes.URL;
            const telemetryResponse = await axios.get(telemetryUrl, {
              headers: {"Accept-Encoding": "gzip"},
            });
            const telemetryEvents = telemetryResponse.data;
            for (const event of telemetryEvents) {
              if (
                event._T === "LogItemPickup" &&
                event.item && event.item.category === "Weapon"
              ) {
                const weaponName = event.item.itemId;
                allWeaponCounts[weaponName] =
                  (allWeaponCounts[weaponName] || 0) + 1;
              }
            }
          }
        }
        newMatchesSaved++;
        const successMsg =
          `Successfully saved full data for match ${matchId}`;
        console.log(successMsg);
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    if (Object.keys(allWeaponCounts).length > 0) {
      const topWeapons = Object.entries(allWeaponCounts)
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
    }

    console.log(`Finished. Saved ${newMatchesSaved} new matches.`);
    return null;
  } catch (error) {
    console.error("Error executing function:", error);
    return null;
  }
});
