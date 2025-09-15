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
      throw new Error(`Failed to fetch samples: ${samplesResponse.status}`);
    }
    const matchIds = samplesResponse.data.data.relationships.matches.data
        .map((m) => m.id);
    console.log(`Collected ${matchIds.length} recent match IDs.`);

    let newMatchesSaved = 0;
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
        if (winningRoster) {
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

        for (const participant of participants) {
          const stats = participant.attributes.stats;
          const participantId = stats.playerId;
          if (participantId) {
            const participantRef =
              matchRef.collection("participants").doc(participantId);
            await participantRef.set({
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
              walkDistance: stats.walkDistance || 0,
              rideDistance: stats.rideDistance || 0,
              swimDistance: stats.swimDistance || 0,
              teamKills: stats.teamKills || 0,
              vehicleDestroys: stats.vehicleDestroys || 0,
            });
          }
        }
        newMatchesSaved++;
        console.log(`Successfully saved full data for match ${matchId}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    console.log(`Finished. Saved ${newMatchesSaved} new matches.`);
    return null;
  } catch (error) {
    console.error("Error executing function:", error);
    return null;
  }
});
