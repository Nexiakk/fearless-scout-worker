/**
 * run-scan.cjs
 *
 * Unified scouting worker for GitHub Actions.
 * Handles ALL scouting tasks in a single run:
 *   1. op.gg SoloQ scraping (fast aggregate stats)
 *   2. Riot API SoloQ scan (per-game data with role detection)
 *   3. Leaguepedia competitive (pro player champion pools)
 *   4. Team import (Grid.gg → Leaguepedia fallback)
 *
 * Supports resume mode: checks import_log + existing data before each step.
 *
 * Environment variables:
 *   RIOT_API_KEY       — Riot API key
 *   TURSO_SCOUTING_URL — Turso scouting DB URL
 *   TURSO_SCOUTING_TOKEN
 *   JOB_ID             — Unique job ID
 *   WORKSPACE_ID       — Firestore workspace ID
 *   MODE               — "fresh" | "resume"
 *   PLAYERS_JSON       — JSON array of player objects
 *   OPTIONS_JSON       — JSON { doSoloq, soloqMethod, doCompetitive, doTeam, dataRange }
 *   TEAM_JSON          — JSON { teamId, teamName, leaguepediaUrl, selectedTournaments }
 *   CALLBACK_URL       — Netlify function URL for progress reporting
 */

const { createClient } = require("@libsql/client");
const axios = require("axios");
const cheerio = require("cheerio");
const { ChampionNameMapper } = require("./champion-name-mapper.cjs");
const {
  buildGameRosterMap,
  matchParticipantToPlayer,
  findOpponentChampion: findOpponentChampionFromSummary,
} = require("./participant-matching.cjs");

// ─── Force stdout/stderr flushing for live GitHub Actions logs ─────────
if (process.stdout._handle && process.stdout._handle.setBlocking) {
  process.stdout._handle.setBlocking(true);
}
if (process.stderr._handle && process.stderr._handle.setBlocking) {
  process.stderr._handle.setBlocking(true);
}

// ─── Config from env ──────────────────────────────────────────────────
const RIOT_API_KEY = process.env.RIOT_API_KEY || "";
const TURSO_URL = process.env.TURSO_SCOUTING_URL || "";
const TURSO_TOKEN = process.env.TURSO_SCOUTING_TOKEN || "";
const JOB_ID = process.env.JOB_ID || "";
const WORKSPACE_ID = process.env.WORKSPACE_ID || "";
const MODE = process.env.MODE || "fresh";
const PLAYERS_JSON = process.env.PLAYERS_JSON || "[]";
const OPTIONS_JSON = process.env.OPTIONS_JSON || "{}";
const TEAM_JSON = process.env.TEAM_JSON || "{}";
const CALLBACK_URL = process.env.CALLBACK_URL || "";

const MAX_RETRIES = 5;
const LEAGUEPEDIA_BASE = "https://lol.fandom.com/wiki/Special:CargoExport";

if (!TURSO_URL || !JOB_ID || !WORKSPACE_ID) {
  console.error(
    "[Fatal] Missing required env vars: TURSO_URL, JOB_ID, WORKSPACE_ID",
  );
  process.exit(1);
}

// ─── Utilities ────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function regionToRouting(region) {
  const routing = {
    euw1: "europe",
    eun1: "europe",
    tr1: "europe",
    ru: "europe",
    na1: "americas",
    br1: "americas",
    la1: "americas",
    la2: "americas",
    kr: "asia",
    jp1: "asia",
    oc1: "sea",
    ph2: "sea",
    sg2: "sea",
    th2: "sea",
    vn2: "sea",
  };
  return routing[region] || "europe";
}

// Global champion name mapper — initialized at startup via Data Dragon API
// Replaces old hardcoded normalizeChampionName() approach.
// Handles all name variants dynamically.
let championMapper = null;

/**
 * Normalize a champion name to canonical display name using the dynamic mapper.
 * Falls back to returning the original name if mapper not initialized.
 */
function normalizeChampionName(name) {
  if (!name) return name;
  if (!championMapper || !championMapper.isInitialized()) return name;
  const championId = championMapper.toChampionId(name);
  // Get display name if we have it, otherwise return the normalized ID
  return championMapper.getDisplayName(championId) || championId;
}

// ─── Stage label mapping ──────────────────────────────────────────────

const STAGE_LABELS = {
  opgg: { label: "op.gg SoloQ", icon: "🔍" },
  "riot-api": { label: "Riot API SoloQ", icon: "⚔️" },
  competitive: { label: "Leaguepedia Competitive", icon: "🏆" },
  team: { label: "Team Import", icon: "👥" },
  grid: { label: "Grid.gg Team Import", icon: "🏟️" },
  aggregation: { label: "Post-import Aggregation", icon: "📊" },
  flags: { label: "Flag Computation", icon: "🚩" },
};

function getStageLabel(step) {
  if (!step) return { label: "Starting scan...", icon: "🚀" };
  const stage = STAGE_LABELS[step];
  return stage || { label: step, icon: "🔄" };
}

function createHumanLabel(step, playerName, currentPlayer, totalPlayers) {
  const stage = getStageLabel(step);
  if (playerName) {
    return `${stage.icon} ${stage.label}: ${playerName} (${currentPlayer || "?"}/${totalPlayers || "?"})`;
  }
  return `${stage.icon} ${stage.label}`;
}

// ─── Progress reporting ───────────────────────────────────────────────

// Lazy-loaded Firestore client for progress writes
let _firestoreDb = null;
let _startedAt = null; // Preserved across progress updates without a DB read-back

async function getFirestoreDb() {
  if (_firestoreDb) return _firestoreDb;
  try {
    const { getFirestore } = require("./firebase-admin");
    _firestoreDb = getFirestore();
    return _firestoreDb;
  } catch (err) {
    console.warn(`[Firestore] Init failed (non-fatal): ${err.message}`);
    return null;
  }
}

async function reportProgress(progress) {
  const step = progress.step || null;
  const stage = getStageLabel(step);

  // Build human-readable label
  // For overall job completion (no step), use a clear label instead of falling back to "Starting scan..."
  let stepLabel;
  if (progress.status === "completed" && step == null) {
    stepLabel = "✅ Scan complete";
  } else if (progress.status === "failed" && step == null) {
    stepLabel = "❌ Scan failed";
  } else {
    stepLabel =
      progress.stepLabel ||
      createHumanLabel(
        step,
        progress.playerName,
        progress.currentPlayer,
        progress.totalPlayers,
      );
  }

  // Build payload for callback
  const payload = {
    action: "scanProgress",
    jobId: JOB_ID,
    workspaceId: WORKSPACE_ID,
    stepLabel,
    ...progress,
  };

  // Report to callback (Netlify scanProgress function)
  if (CALLBACK_URL) {
    try {
      await fetch(CALLBACK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    } catch (error) {
      console.warn(`[Progress] Callback failed: ${error.message}`);
    }
  } else {
    console.log(`[Progress] ${JSON.stringify(payload)}`);
  }

  // Also write progress to Firestore scoutJobs/{jobId}
  try {
    const db = await getFirestoreDb();
    if (db) {
      const now = new Date();
      const expiresAt = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000); // 7-day TTL

      // Overall status only changes at job lifecycles (started → completed/failed at the very end)
      // Step-level completions/started use stepStatus so they don't trigger auto-dismiss
      const isJobTerminal = progress.step == null && (progress.status === "completed" || progress.status === "failed");
      const status = isJobTerminal
        ? progress.status
        : "running";

      const stepStatus =
        progress.step != null
          ? progress.status === "completed"
            ? "step_completed"
            : progress.status === "failed"
              ? "step_failed"
              : progress.status === "started"
                ? "step_active"
                : "step_active"
          : null;

      // Set startedAt once at the beginning, preserve via module-level variable
      if (progress.status === "started") {
        _startedAt = now.toISOString();
      }

      const docData = {
        workspaceId: WORKSPACE_ID,
        status,
        stepStatus,
        step: step || "init",
        stepLabel,
        playerName: progress.playerName || null,
        completed: progress.currentPlayer || 0,
        total: progress.totalPlayers || 0,
        message: stepLabel,
        error: progress.error || null,
        startedAt: _startedAt,
        updatedAt: now.toISOString(),
        expiresAt: expiresAt.toISOString(),
      };

      await db
        .collection("scoutJobs")
        .doc(JOB_ID)
        .set(docData, { merge: true });
    }
  } catch (error) {
    console.warn(`[Progress] Firestore write failed: ${error.message}`);
  }
}

// ─── Firestore version marker ───────────────────────────────────────
// Writes scoutingLastUpdated timestamp to _meta/scouting after scan
// so the frontend can invalidate cached data via version check.
async function writeScoutingVersionMarker() {
  try {
    const db = await getFirestoreDb();
    if (!db) return;
    await db
      .collection('workspaces')
      .doc(WORKSPACE_ID)
      .collection('_meta')
      .doc('scouting')
      .set({
        lastUpdated: new Date(),
      }, { merge: true });
    console.log(`[Version] Wrote scoutingLastUpdated marker for workspace ${WORKSPACE_ID}`);
  } catch (err) {
    console.warn(`[Version] Failed to write version marker: ${err.message}`);
  }
}

// ─── Firestore lock release ──────────────────────────────────────────
// Releases the scan lock from the workspace's _scouting/lock document
// so that new scans can be dispatched after completion or failure.
async function releaseFirestoreLock() {
  try {
    const db = await getFirestoreDb();
    if (!db) return;
    await db
      .collection('workspaces')
      .doc(WORKSPACE_ID)
      .collection('_scouting')
      .doc('lock')
      .delete();
    console.log(`[Lock] Released Firestore lock for workspace ${WORKSPACE_ID}`);
  } catch (err) {
    // If the lock document doesn't exist or was already deleted, that's fine
    if (!err.message?.includes('not-found') && !err.message?.includes('NOT_FOUND')) {
      throw err;
    }
  }
}

// ─── Resume helpers ───────────────────────────────────────────────────

async function isStepComplete(turso, stepType) {
  if (MODE !== "resume") return false;
  const result = await turso.execute({
    sql: `SELECT COUNT(*) as cnt FROM import_log WHERE workspace_id = ? AND import_type = ? AND status = 'success'`,
    args: [WORKSPACE_ID, stepType],
  });
  return (result.rows[0]?.cnt || 0) > 0;
}

async function logStep(
  turso,
  stepType,
  gameCount,
  status = "success",
  error = null,
) {
  await turso.execute({
    sql: `INSERT INTO import_log (workspace_id, import_type, game_count, status, error_message, imported_at)
          VALUES (?, ?, ?, ?, ?, datetime('now'))`,
    args: [WORKSPACE_ID, stepType, gameCount, status, error],
  });
}

async function hasPlayerSoloqData(turso, playerId) {
  const result = await turso.execute({
    sql: `SELECT COUNT(*) as cnt FROM soloq_games WHERE player_id = ? AND workspace_id = ?`,
    args: [playerId, WORKSPACE_ID],
  });
  return (result.rows[0]?.cnt || 0) > 0;
}

async function hasPlayerCompData(turso, playerId) {
  const result = await turso.execute({
    sql: `SELECT COUNT(*) as cnt FROM competitive_games WHERE player_id = ? AND workspace_id = ?`,
    args: [playerId, WORKSPACE_ID],
  });
  return (result.rows[0]?.cnt || 0) > 0;
}

/**
 * Check whether a team already has team_games rows for a tournament.
 * @param {string} source - Optional: restrict the check to one source
 * ('grid' | 'leaguepedia'). When omitted, any source counts — used for
 * legacy string tournament entries (pre-5.2 behavior).
 */
async function hasTeamData(turso, teamId, tournament, source) {
  let sql = `SELECT COUNT(*) as cnt FROM team_games WHERE team_id = ? AND tournament = ?`;
  const args = [teamId, tournament];
  if (source) {
    sql += ` AND source = ?`;
    args.push(source);
  }
  const result = await turso.execute({ sql, args });
  return (result.rows[0]?.cnt || 0) > 0;
}

// ─── Step 1: op.gg Scraping ──────────────────────────────────────────

async function scrapeOpgg(turso, player, options) {
  const { playerId, riotId, tag, region } = player;
  console.log(`::notice::[op.gg] Scraping: ${riotId}#${tag}`);

  // Only skip in resume mode — fresh scans always re-fetch (INSERT OR IGNORE handles duplicates)
  if (MODE === "resume" && await hasPlayerSoloqData(turso, playerId)) {
    console.log(`[op.gg] ${riotId}#${tag} already has SoloQ data, skipping (resume mode)`);
    return { gamesImported: 0, skipped: true };
  }

  // Build op.gg champions URL
  const server = (region || "euw1").replace(/\d+$/, "");
  const url = `https://www.op.gg/en/lol/summoners/${server}/${encodeURIComponent(riotId)}-${encodeURIComponent(tag)}/champions?queue_type=SOLORANKED`;

  try {
    const response = await axios.get(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      },
      timeout: 10000,
    });

    const $ = cheerio.load(response.data);
    const nextDataScript = $("script#__NEXT_DATA__").html();
    if (!nextDataScript) {
      console.log(`[op.gg] No __NEXT_DATA__ found for ${riotId}#${tag}`);
      return { gamesImported: 0 };
    }

    const nextData = JSON.parse(nextDataScript);
    const pageProps = nextData?.props?.pageProps;
    let championsData =
      pageProps?.champions ||
      pageProps?.championStats ||
      pageProps?.data?.champions ||
      pageProps?.summoner?.champions ||
      [];

    if (!Array.isArray(championsData) || championsData.length === 0) {
      console.log(
        `[op.gg] No champion data in __NEXT_DATA__ for ${riotId}#${tag}`,
      );
      return { gamesImported: 0 };
    }

    let totalGames = 0;
    for (const champ of championsData) {
      const name = normalizeChampionName(
        champ.championName || champ.name || champ.champion || "",
      );
      const wins = champ.wins || champ.win || 0;
      const losses = champ.losses || champ.loss || 0;
      const games = champ.games || champ.totalGames || wins + losses;
      if (!name || games === 0) continue;
      totalGames += games;

      // Extract KDA if available
      let avgKills = null,
        avgDeaths = null,
        avgAssists = null,
        avgGold = null,
        avgCs = null;
      if (champ.kda) {
        avgKills = champ.kda.kills || champ.kda.k || null;
        avgDeaths = champ.kda.deaths || champ.kda.d || null;
        avgAssists = champ.kda.assists || champ.kda.a || null;
      }

      // Store each game individually
      for (let g = 0; g < games; g++) {
        try {
          await turso.execute({
            sql: `INSERT OR IGNORE INTO soloq_games (player_id, workspace_id, source, game_date, champion, kills, deaths, assists, gold, cs, win, role)
                  VALUES (?, ?, 'opgg', ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
            args: [
              playerId,
              WORKSPACE_ID,
              new Date().toISOString().split("T")[0],
              name,
              Math.round(avgKills || 0),
              Math.round(avgDeaths || 0),
              Math.round(avgAssists || 0),
              Math.round(avgGold || 0),
              Math.round(avgCs || 0),
              wins / games > 0.5 ? 1 : 0,
            ],
          });
        } catch (err) {
          if (!err.message.includes("UNIQUE"))
            console.warn(`[op.gg] Insert error: ${err.message}`);
        }
      }
    }

    console.log(`[op.gg] ${riotId}#${tag}: ${totalGames} games imported`);
    return { gamesImported: totalGames };
  } catch (error) {
    console.error(
      `[op.gg] Scrape failed for ${riotId}#${tag}: ${error.message}`,
    );
    return { gamesImported: 0, error: error.message };
  }
}

// ─── Step 2: Riot API Scan ──────────────────────────────────────────

async function riotFetch(url) {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(url, {
        headers: { "X-Riot-Token": RIOT_API_KEY },
      });
      if (response.status === 429) {
        const retryAfter = parseInt(
          response.headers.get("Retry-After") || "3",
          10,
        );
        await sleep(retryAfter * 1000 + 100);
        continue;
      }
      if (response.status === 404) return null;
      if (response.status === 403) {
        console.error("[Riot] API key invalid");
        return null;
      }
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return await response.json();
    } catch (error) {
      if (attempt === MAX_RETRIES - 1) throw error;
      await sleep(2000);
    }
  }
  return null;
}

/**
 * Find the opponent champion from match data.
 */
function findOpponentChampion(matchParticipants, puuid) {
  const player = matchParticipants.find((p) => p.puuid === puuid);
  if (!player) return null;
  const teamId = player.teamId;
  const position = (player.teamPosition || "").toUpperCase();
  const enemy = matchParticipants.find(
    (p) => p.teamId !== teamId && p.teamPosition === position,
  );
  return enemy?.championName || null;
}

async function scanRiotApi(turso, player, startTimestamp, endTimestamp) {
  const {
    playerId,
    riotId,
    tag,
    region,
    assignedRole,
    puuid: storedPuuid,
  } = player;
  console.log(
    `[RiotAPI] Scanning: ${riotId}#${tag} (role filter: ${assignedRole || "none"})`,
  );

  if (MODE === "resume" && await hasPlayerSoloqData(turso, playerId)) {
    console.log(`[RiotAPI] ${riotId}#${tag} already has data, skipping (resume mode)`);
    return { gamesFound: 0, gamesImported: 0, skipped: true };
  }

  const routing = regionToRouting(region);
  let puuid;
  if (storedPuuid) {
    console.log(
      `[RiotAPI] Using stored PUUID for ${riotId}#${tag} (skipped API resolve)`,
    );
    puuid = storedPuuid;
  } else {
    console.log(
      `[RiotAPI] No stored PUUID for ${riotId}#${tag} — resolving from Riot API`,
    );
    const puuidUrl = `https://${routing}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/${encodeURIComponent(riotId)}/${encodeURIComponent(tag)}`;
    const accountData = await riotFetch(puuidUrl);
    if (!accountData?.puuid) {
      console.error(`[RiotAPI] Could not resolve PUUID for ${riotId}#${tag}`);
      return { gamesFound: 0, gamesImported: 0 };
    }
    puuid = accountData.puuid;
  }

  const params = new URLSearchParams({
    api_key: RIOT_API_KEY,
    count: "100",
    queue: "420",
  });
  if (startTimestamp) params.set("startTime", startTimestamp);
  if (endTimestamp) params.set("endTime", endTimestamp);
  const idsUrl = `https://${routing}.api.riotgames.com/lol/match/v5/matches/by-puuid/${puuid}/ids?${params}`;
  const matchIds = await riotFetch(idsUrl);
  if (!Array.isArray(matchIds) || matchIds.length === 0) {
    console.log(`[RiotAPI] No matches for ${riotId}#${tag}`);
    return { gamesFound: 0, gamesImported: 0 };
  }

  let gamesFound = 0,
    gamesMatched = 0,
    gamesImported = 0;
  for (let i = 0; i < matchIds.length; i++) {
    const matchData = await riotFetch(
      `https://${routing}.api.riotgames.com/lol/match/v5/matches/${matchIds[i]}`,
    );
    if (!matchData?.info?.participants) continue;

    const p = matchData.info.participants.find((part) => part.puuid === puuid);
    if (!p) continue;
    gamesFound++;

    const roleMap = {
      TOP: "Top",
      JUNGLE: "Jungle",
      MIDDLE: "Mid",
      BOTTOM: "Bot",
      UTILITY: "Support",
      SUPPORT: "Support",
    };
    const role = roleMap[(p.teamPosition || "").toUpperCase()] || null;

    if (assignedRole && role?.toLowerCase() !== assignedRole.toLowerCase()) {
      continue;
    }
    gamesMatched++;

    const opponentChampion = findOpponentChampion(
      matchData.info.participants,
      puuid,
    );
    const gameDate = new Date(matchData.info.gameCreation)
      .toISOString()
      .split("T")[0];

    try {
      await turso.execute({
        sql: `INSERT OR IGNORE INTO soloq_games (player_id, workspace_id, source, game_date, champion, opponent_champion, kills, deaths, assists, gold, cs, win, role, match_id)
              VALUES (?, ?, 'riot-api', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          playerId,
          WORKSPACE_ID,
          gameDate,
          p.championName,
          opponentChampion,
          p.kills || 0,
          p.deaths || 0,
          p.assists || 0,
          p.goldEarned || 0,
          (p.totalMinionsKilled || 0) + (p.neutralMinionsKilled || 0),
          p.win ? 1 : 0,
          role,
          matchIds[i],
        ],
      });
      gamesImported++;
    } catch (err) {
      if (!err.message.includes("UNIQUE"))
        console.warn(`[RiotAPI] Insert error: ${err.message}`);
    }

    if (i % 10 === 0 || i === matchIds.length - 1) {
      await reportProgress({
        step: "riot-api",
        playerId,
        playerName: riotId,
        status: "scanning",
        gamesFound,
        gamesImported,
        progress: (((i + 1) / matchIds.length) * 100).toFixed(1),
      });
    }
    if (i % 5 === 0) await sleep(150);
  }

  console.log(
    `[RiotAPI] ${riotId}#${tag}: ${gamesMatched}/${gamesFound} role-matched, ${gamesImported} imported`,
  );
  return { gamesFound, gamesMatched, gamesImported };
}

// ─── Step 3: Leaguepedia Competitive ──────────────────────────────────

async function fetchLeaguepediaCargo(params) {
  const url = `${LEAGUEPEDIA_BASE}?${new URLSearchParams(params).toString()}`;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await axios.get(url, { timeout: 15000 });
      if (response.data) return response.data;
    } catch (err) {
      if (attempt < 2) await sleep(2000);
    }
  }
  return null;
}

async function scanCompetitive(turso, player, startDate, endDate) {
  const {
    playerId,
    riotId,
    tag,
    playerOverrideName,
    leaguepediaUrl,
    leaguepediaSlug,
  } = player;
  // Wiki URLs use underscores for spaces; SP.Link / Team1 / Team2 store the
  // display name with spaces, so convert slug-derived values before matching.
  const lpName =
    leaguepediaSlug?.replace(/_/g, " ") ||
    playerOverrideName ||
    (leaguepediaUrl
      ? decodeURIComponent(leaguepediaUrl.match(/wiki\/([^/?]+)/)?.[1] || "").replace(/_/g, " ")
      : riotId);

  console.log(
    `::notice::[Competitive] Scanning player: ${lpName} (playerId: ${playerId})`,
  );

  if (MODE === "resume" && await hasPlayerCompData(turso, playerId)) {
    console.log(`[Competitive] ${lpName} already has data, skipping (resume mode)`);
    return { gamesImported: 0, skipped: true };
  }

  // 3-Table Join: SP (player), SPVs (lane opponent via UniqueRoleVs), SG (game metadata)
  // SPVs.Champion=ChampionVs — alias to avoid field collision with SP.Champion
  // SPVs.Team=TeamVs — alias to get opponent team name
  // SP.IngameRole=Role — alias for clarity
  // SG.Gamelength — game duration in "MM:SS" format
  // Build WHERE clause with optional date range filter (Sprint 5.3).
  // Date filter is opt-in via the startDate/endDate params; default
  // scope (last 12 months) is enforced in main() before calling this.
  let whereClause = `SP.Link = "${lpName}"`;
  if (startDate) whereClause += ` AND SG.DateTime_UTC >= "${startDate}"`;
  if (endDate) whereClause += ` AND SG.DateTime_UTC <= "${endDate}"`;

  const data = await fetchLeaguepediaCargo({
    tables: "ScoreboardPlayers=SP,ScoreboardPlayers=SPVs,ScoreboardGames=SG",
    fields: [
      "SP.GameId",
      "SP.Champion",
      "SPVs.Champion=ChampionVs",
      "SP.Link",
      "SP.Side",
      "SP.Team",
      "SPVs.Team=TeamVs",
      "SP.IngameRole=Role",
      "SG.Tournament",
      "SP.Kills",
      "SP.Deaths",
      "SP.Assists",
      "SP.CS",
      "SP.Gold",
      "SG.DateTime_UTC",
      "SG.Winner",
      "SG.Team1",
      "SG.Team2",
      "SP.DamageToChampions",
      "SP.VisionScore",
      "SG.Gamelength",
    ].join(","),
    where: whereClause,
    join_on: "SG.GameId=SP.GameId,SP.UniqueRoleVs=SPVs.UniqueRole",
    limit: "500",
    format: "json",
    order_by: "SG.DateTime_UTC DESC",
  });

  if (!data) {
    console.log(`[Competitive] No data from Leaguepedia for ${lpName}`);
    console.log(
      `[Competitive] Debug — empty response for query Player = "${lpName}"`,
    );
    return { gamesImported: 0, needsFallback: true, fallbackName: riotId };
  }

  let rows = [];
  if (typeof data === "string") {
    try {
      rows = JSON.parse(data);
    } catch {
      console.warn(
        `[Competitive] Failed to parse JSON response: ${data.substring(0, 200)}`,
      );
      return { gamesImported: 0 };
    }
  } else if (Array.isArray(data)) {
    rows = data;
  } else if (data?.response) {
    rows = data.response;
  }

  if (rows.length > 0) {
    console.log(
      `[Competitive] Raw response sample (first 3 rows):`,
      rows.slice(0, 3).map((r) => JSON.stringify(r)),
    );
  }

  if (!Array.isArray(rows) || rows.length === 0) {
    console.log(`[Competitive] No rows for ${lpName}`);
    console.log(
      `::notice::[Competitive] Zero results — check if Leaguepedia slug "${lpName}" is correct`,
    );
    return { gamesImported: 0, needsFallback: true, fallbackName: riotId };
  }

  console.log(
    `::notice::[Competitive] Found ${rows.length} competitive games for ${lpName}`,
  );

  let totalImported = 0;

  for (const row of rows) {
    const champion = normalizeChampionName(row.Champion || row.champion || "");
    if (!champion) continue;

    const gameId = row.GameId || "";
    const team = row.Team || "";
    const side = parseInt(row.Side || 0);
    const tournament = row.Tournament || "Unknown Tournament";
    // Winner=1 means Team1 won, Winner=2 means Team2 won.
    // Side=1 means player on Team1, Side=2 means player on Team2.
    const winner = parseInt(row.Winner || 0);
    const win = side && winner && side === winner ? 1 : 0;
    const kills = parseInt(row.Kills || 0);
    const deaths = parseInt(row.Deaths || 0);
    const assists = parseInt(row.Assists || 0);
    // DateTime_UTC comes back as "DateTime UTC" (space) from CargoExport
    const gameDate =
      row["DateTime UTC"] ||
      row.DateTime_UTC ||
      row.date ||
      new Date().toISOString().split("T")[0];
    // New fields from expanded 3-table join
    const cs = parseInt(row.CS || 0);
    const gold = parseInt(row.Gold || 0);
    const damage = parseInt(row.DamageToChampions || 0);
    const vision = parseInt(row.VisionScore || 0);
    // Gamelength comes as "MM:SS" string — parse to seconds for storage
    const gamelengthStr = row.Gamelength || "";
    let duration = 0;
    if (gamelengthStr) {
      const parts = gamelengthStr.split(":");
      if (parts.length === 2) {
        duration = parseInt(parts[0]) * 60 + parseInt(parts[1]);
      }
    }

    // Derive side label and opponent team from Team1/Team2.
    // SG.Team1 = team on Blue side, SG.Team2 = team on Red side.
    // SP.Side = 1 means player is on Blue (Team1), 2 means player is on Red (Team2).
    const team1Name = row.Team1 || "";
    const team2Name = row.Team2 || "";
    const sideLabel = side === 1 ? "Blue" : side === 2 ? "Red" : "";
    const opponent =
      row.TeamVs || (side === 1 ? team2Name : side === 2 ? team1Name : "");

    // Role is now available from the 3-table join via SP.IngameRole=Role alias
    const mappedRole = row.Role || row.role || null;

    // Opponent champion from the SPVs self-join via SPVs.Champion=ChampionVs alias
    const opponentChampion = normalizeChampionName(row.ChampionVs || "");

    try {
      await turso.execute({
        sql: `INSERT OR IGNORE INTO competitive_games 
              (player_id, workspace_id, source, source_game_id, leaguepedia_game_id, game_date, champion, opponent_champion, tournament, opponent, team, win, kills, deaths, assists, cs, gold, vision, damage, duration, side, role)
              VALUES (?, ?, 'leaguepedia', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          playerId,
          WORKSPACE_ID,
          gameId,  // source_game_id
          gameId,  // leaguepedia_game_id
          gameDate,
          champion,
          opponentChampion,
          tournament,
          opponent,
          team,
          win,
          kills,
          deaths,
          assists,
          cs,
          gold,
          vision,
          damage,
          duration,
          sideLabel,
          mappedRole,
        ],
      });
      totalImported++;
    } catch (err) {
      if (!err.message.includes("UNIQUE"))
        console.warn(`[Competitive] Insert error: ${err.message}`);
    }

    await reportProgress({
      step: "competitive",
      playerId,
      playerName: lpName,
      status: "scanning",
      gamesImported: totalImported,
      progress: (((rows.indexOf(row) + 1) / rows.length) * 100).toFixed(1),
    });
  }

  console.log(
    `[Competitive] ${lpName}: ${totalImported} games imported with real per-game data`,
  );
  return { gamesImported: totalImported };
}

// ─── Step 4: Team Import ──────────────────────────────────────────────

async function getTursoClient(dbName) {
  return createClient({ url: TURSO_URL, authToken: TURSO_TOKEN });
}

/**
 * Normalize a selectedTournaments entry to a plain tournament name.
 * Entries may be objects ({ name, source, dateStart, dateEnd, ... }) as
 * produced by the Sprint 5.2 team tournament picker, or plain strings.
 */
function tournamentNameOf(entry) {
  if (!entry) return "";
  return typeof entry === "string" ? entry : entry.name || "";
}

/**
 * Effective fetch window for a tournament entry: intersection of the
 * global dataRange and the per-tournament dateStart/dateEnd (the picker
 * clips each tournament to the user's import window — §2.2).
 * Returns { startTimeGte, startTimeLte } ISO strings, or null bounds.
 */
function tournamentWindow(tournamentEntry, dataRange) {
  let start = dataRange?.startDate ? new Date(dataRange.startDate) : null;
  let end = dataRange?.endDate ? new Date(dataRange.endDate) : null;
  if (tournamentEntry && typeof tournamentEntry === "object") {
    if (tournamentEntry.dateStart) {
      const ts = new Date(tournamentEntry.dateStart);
      if (!start || ts > start) start = ts;
    }
    if (tournamentEntry.dateEnd) {
      const te = new Date(tournamentEntry.dateEnd);
      if (!end || te < end) end = te;
    }
  }
  return {
    startTimeGte: start ? start.toISOString() : null,
    startTimeLte: end ? end.toISOString() : null,
  };
}

/**
 * Import a single tournament for a team.
 *
 * Source routing (Sprint 5.6):
 *   - Entry source 'leaguepedia'  → LP team import only (ScoreboardGames).
 *   - Entry source 'grid' (or legacy string entries) → Grid first (when the
 *     team has a gridTeamId); when Grid produces no games for the tournament
 *     (no series found), fall back to the LP team import. §2.3a: both sources
 *     are peer-level — a tournament the Grid API doesn't cover still gets
 *     team_games rows from Leaguepedia.
 *
 * No import-time dedup across sources: Grid and LP rows use different ID
 * spaces (source_game_id = grid game id vs LP GameId) and coexist; the
 * display layer dedups (same rule as competitive_games).
 */
async function importTeamData(turso, team, tournamentEntry) {
  const tournamentName = tournamentNameOf(tournamentEntry);
  console.log(`[Team] Importing tournament: ${tournamentName}`);

  // Entry-level source from the Sprint 5.2 picker ({ name, source, ... }).
  // Legacy string entries behave like 'grid' (Grid first, LP fallback).
  const entrySource =
    tournamentEntry && typeof tournamentEntry === "object"
      ? tournamentEntry.source
      : null;

  // Resume guard — per-source: a grid-imported tournament doesn't block an
  // LP entry for the same name, and vice versa.
  if (await hasTeamData(turso, team.teamId, tournamentName, entrySource || undefined)) {
    console.log(`[Team] ${tournamentName} already imported (${entrySource || "any"}), skipping`);
    return { importedGames: 0, skipped: true };
  }

  const gridEligible = !!team.gridTeamId && entrySource !== "leaguepedia";

  if (gridEligible) {
    const result = await importTeamDataFromGrid(turso, team, tournamentEntry);
    const gridGames = result.importedGames || 0;
    if (gridGames > 0) {
      return { importedGames: gridGames, importedPlayers: result.importedPlayers || 0 };
    }
    // Grid produced nothing for this tournament (no series, or no games with
    // draft data) → fall back to the LP team import.
    console.log(
      `[Team] No Grid games for "${tournamentName}" (series found: ${result.seriesFound || 0}) — falling back to Leaguepedia`,
    );
  }

  const result = await importTeamDataFromLeaguepedia(turso, team, tournamentEntry);
  return { importedGames: result.importedGames || 0, source: "leaguepedia" };
}

/**
 * Import team data from Grid.gg API.
 * Fetches series, draft data, and Riot summary files for the team's tournaments.
 *
 * Participant matching (Sprint 5.4): participants are matched to our scouted
 * players by exact gridPlayerId via the series-state roster, falling back to
 * normalized name matching. Grid rows additionally carry side_label,
 * grid_series_id/grid_game_id and a filled opponent_champion.
 */
async function importTeamDataFromGrid(turso, team, tournamentEntry) {
  const tournamentName = tournamentNameOf(tournamentEntry);
  console.log(`[Grid] Importing ${team.teamName} from Grid.gg for tournament: ${tournamentName}`);
  await reportProgress({ step: "grid", status: "started", tournament: tournamentName });

  const grid = require("./grid-client.cjs");
  grid.initialize(championMapper);

  const players = JSON.parse(PLAYERS_JSON);

  // Fetch series for this team from Grid — scoped to the global dataRange
  // intersected with the per-tournament window (dateStart/dateEnd).
  const dataRange = JSON.parse(OPTIONS_JSON).dataRange || {};
  const window = tournamentWindow(tournamentEntry, dataRange);
  const options = {
    limit: 50,
  };
  if (window.startTimeGte) options.startTimeGte = window.startTimeGte;
  if (window.startTimeLte) options.startTimeLte = window.startTimeLte;

  const series = await grid.fetchSeriesByTeamId(team.gridTeamId, options);
  console.log(`[Grid] Found ${series.length} series for team ${team.teamName}`);

  // Filter series by tournament name if specified
  const filteredSeries = tournamentName
    ? series.filter(s => s.tournament?.name === tournamentName || s.tournament?.nameShortened === tournamentName)
    : series;

  console.log(`[Grid] ${filteredSeries.length} series match tournament "${tournamentName}"`);

  let importedGames = 0;
  let importedPlayers = 0;
  const seriesFound = filteredSeries.length;

  for (const s of filteredSeries) {
    await reportProgress({
      step: "grid",
      status: "scanning",
      seriesId: s.id,
      tournament: tournamentName,
      progress: ((filteredSeries.indexOf(s) + 1) / filteredSeries.length * 100).toFixed(1),
    });

    // Fetch draft data
    const seriesState = await grid.fetchSeriesDraftData(s.id);
    if (!seriesState || !seriesState.games) {
      console.log(`[Grid] No games for series ${s.id}, skipping`);
      continue;
    }

    // Per-game roster with GRID player IDs (Sprint 5.4) — used to match
    // participants by exact gridPlayerId instead of name substrings.
    const gameRoster = buildGameRosterMap(seriesState, championMapper);

    // Determine which team is "us" and which is opponent
    const ourTeamOnGrid = s.teams?.find(t => t.baseInfo?.id === team.gridTeamId);
    const opponentTeam = s.teams?.find(t => t.baseInfo?.id !== team.gridTeamId);
    const opponentName = opponentTeam?.baseInfo?.name || "Unknown";

    for (const game of seriesState.games) {
      if (!game.draftActions || game.draftActions.length === 0) continue;

      // Parse draft actions
      const events = grid.parseDraftActions([game]);

      // Separate picks/bans by side
      const bluePicks = events.filter(e => e.side === 'blue' && e.action_type === 'pick').map(e => e.champion_name);
      const redPicks = events.filter(e => e.side === 'red' && e.action_type === 'pick').map(e => e.champion_name);
      const blueBans = events.filter(e => e.side === 'blue' && e.action_type === 'ban').map(e => e.champion_name);
      const redBans = events.filter(e => e.side === 'red' && e.action_type === 'ban').map(e => e.champion_name);

      // Determine which side our team is on
      const ourTeam = game.teams?.find(t => t.id === team.gridTeamId);
      const ourSide = ourTeam?.side?.toLowerCase() || null;
      const won = ourTeam?.won ? 1 : 0;

      // Build picks/bans from our team's perspective
      const ourPicks = ourSide === 'blue' ? bluePicks : ourSide === 'red' ? redPicks : [];
      const ourBans = ourSide === 'blue' ? blueBans : ourSide === 'red' ? redBans : [];
      const oppPicks = ourSide === 'blue' ? redPicks : ourSide === 'red' ? bluePicks : [];
      const oppBans = ourSide === 'blue' ? redBans : ourSide === 'red' ? blueBans : [];

      // Build draft sequence JSON
      const draftSequence = JSON.stringify(events.map(e => ({
        game: e.game_sequence,
        side: e.side,
        type: e.action_type,
        champion: e.champion_name,
        pos: e.pick_position || e.ban_position,
      })));

      // Determine winner side
      let winnerSide = null;
      for (const t of game.teams || []) {
        if (t.won) { winnerSide = t.side?.toLowerCase(); break; }
      }

      // Insert into team_games
      const gameDate = s.startTimeScheduled ? s.startTimeScheduled.split('T')[0] : new Date().toISOString().split('T')[0];
      try {
        await turso.execute({
          sql: `INSERT OR IGNORE INTO team_games 
                (team_id, workspace_id, tournament, game_date, opponent, win, picks, bans, opp_picks, opp_bans, source, source_game_id,
                 blue_picks, red_picks, blue_bans, red_bans, blue_team, red_team, draft_sequence, game_number, grid_series_id, grid_game_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'grid', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          args: [
            team.teamId,
            WORKSPACE_ID,
            tournamentName || s.tournament?.name || "Unknown",
            gameDate,
            opponentName,
            won,
            JSON.stringify(ourPicks),
            JSON.stringify(ourBans),
            JSON.stringify(oppPicks),
            JSON.stringify(oppBans),
            String(game.id), // source_game_id — per-game id (was the series id,
                             // which made INSERT OR IGNORE drop all but one game
                             // per Bo3/Bo5 series — fixed in Sprint 5.6)
            JSON.stringify(bluePicks),
            JSON.stringify(redPicks),
            JSON.stringify(blueBans),
            JSON.stringify(redBans),
            s.teams?.[0]?.baseInfo?.name || null,
            s.teams?.[1]?.baseInfo?.name || null,
            draftSequence,
            game.sequenceNumber,
            String(s.id),
            game.id,
          ],
        });
        importedGames++;
      } catch (err) {
        if (!err.message.includes("UNIQUE"))
          console.warn(`[Grid] team_games insert error: ${err.message}`);
      }

      // Download Riot summary file for participant data
      await sleep(1500);
      const summaryData = await grid.downloadRiotFile(s.id, game.sequenceNumber, 'summary');
      if (!summaryData || !summaryData.participants) continue;

      const participants = grid.parseRiotSummaryFile(summaryData);

      // Build role-ordered champions for each side (top→jungle→mid→bot→support)
      const roleOrder = ['top', 'jungle', 'mid', 'bot', 'support'];
      const blueParticipants = participants.filter(p => p.side === 'blue');
      const redParticipants = participants.filter(p => p.side === 'red');
      const blueRoles = roleOrder
        .map(role => {
          const p = blueParticipants.find(pp => pp.role === role);
          return p ? { champion: p.championName, role: p.role } : null;
        })
        .filter(Boolean);
      const redRoles = roleOrder
        .map(role => {
          const p = redParticipants.find(pp => pp.role === role);
          return p ? { champion: p.championName, role: p.role } : null;
        })
        .filter(Boolean);

      // Update team_games row with duration and role data
      try {
        await turso.execute({
          sql: `UPDATE team_games SET duration = ?, blue_roles = ?, red_roles = ? WHERE grid_series_id = ? AND grid_game_id = ?`,
          args: [
            summaryData.gameDuration ? Math.round(summaryData.gameDuration) : null,
            JSON.stringify(blueRoles),
            JSON.stringify(redRoles),
            String(s.id),
            game.id,
          ],
        });
      } catch (err) {
        console.warn(`[Grid] team_games update error: ${err.message}`);
      }

      // Match participants to stored players: exact gridPlayerId via the
      // series-state roster first, normalized name match as fallback.
      let matched = 0;
      const unmatched = [];
      for (const p of participants) {
        const matchedPlayer = matchParticipantToPlayer(
          p,
          gameRoster.get(game.id),
          players,
        );

        if (matchedPlayer) {
          matched++;
          // Opponent champion at the same role on the other side (was NULL before Sprint 5.4)
          const opponentChampion = findOpponentChampionFromSummary(p, participants);

          // Insert into competitive_games for this player
          try {
            await turso.execute({
              sql: `INSERT OR IGNORE INTO competitive_games 
                    (player_id, workspace_id, source, source_game_id, game_date, champion, opponent_champion, tournament, opponent, win, kills, deaths, assists, cs, gold, side, side_label, role, duration, grid_series_id, grid_game_id)
                    VALUES (?, ?, 'grid', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              args: [
                matchedPlayer.playerId,
                WORKSPACE_ID,
                `${s.id}_g${game.sequenceNumber}_p${p.participantId}`, // source_game_id
                gameDate,
                p.championName,
                opponentChampion,
                tournamentName || s.tournament?.name || "Unknown",
                opponentName,
                p.win,
                p.kills,
                p.deaths,
                p.assists,
                p.cs,
                p.goldEarned,
                p.side,
                p.side, // side_label — side of the team_games row
                p.role,
                summaryData.gameDuration ? Math.round(summaryData.gameDuration) : null,
                String(s.id),
                game.id,
              ],
            });
            importedPlayers++;
          } catch (err) {
            if (!err.message.includes("UNIQUE"))
              console.warn(`[Grid] competitive_games insert error: ${err.message}`);
          }
        } else {
          unmatched.push(p.playerName || `participant_${p.participantId}`);
        }
      }

      if (unmatched.length > 0) {
        console.warn(
          `[Grid] Game ${game.sequenceNumber} (${s.id}): ${matched}/${participants.length} participants matched — unmatched: ${unmatched.join(", ")}`,
        );
      } else if (participants.length > 0) {
        console.log(`[Grid] Game ${game.sequenceNumber} (${s.id}): ${matched}/${participants.length} participants matched`);
      }
    }

    await sleep(1000); // Rate limiting between series
  }

  console.log(`[Grid] Imported ${importedGames} team games, ${importedPlayers} player game entries`);
  await reportProgress({
    step: "grid",
    status: "completed",
    totalImported: importedGames,
    totalPlayerEntries: importedPlayers,
  });

  return { importedGames, importedPlayers, seriesFound };
}

// ─── Step 4b: Leaguepedia team import (fallback for non-Grid tournaments) ──

/**
 * Parse a CargoExport response into an array of rows.
 * Handles: pre-parsed arrays (axios auto-JSON), JSON strings, { response }.
 */
function parseCargoJsonRows(data) {
  if (!data) return [];
  if (Array.isArray(data)) return data;
  if (typeof data === "string") {
    try {
      return JSON.parse(data);
    } catch {
      return [];
    }
  }
  if (Array.isArray(data.response)) return data.response;
  return [];
}

/**
 * Parse a Leaguepedia picks/bans cell into an array of champion-name strings.
 * Handles the formats seen in ScoreboardGames: pre-parsed JSON arrays of
 * strings (verified live 2026-08-02), arrays of { champion, role } objects
 * (legacy/edge data), comma-separated strings, and null/empty values.
 */
function normalizeChampionList(value) {
  if (!value) return [];
  let entries = value;
  if (typeof entries === "string") {
    try {
      entries = JSON.parse(entries);
    } catch {
      return entries
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
        .map((c) => normalizeChampionName(c));
    }
  }
  if (!Array.isArray(entries)) return [];
  return entries
    .map((e) => {
      if (typeof e === "string") return e;
      if (e && typeof e === "object") return e.champion || e.name || "";
      return "";
    })
    .filter(Boolean)
    .map((c) => normalizeChampionName(c));
}

/**
 * Parse Leaguepedia Gamelength ("MM:SS" string) into integer seconds.
 * Falls back to a plain integer (some older exports store seconds).
 */
function parseGamelength(value) {
  if (!value) return null;
  const str = String(value);
  const parts = str.split(":");
  if (parts.length === 2) {
    const mins = parseInt(parts[0]);
    const secs = parseInt(parts[1]);
    if (!isNaN(mins) && !isNaN(secs)) return mins * 60 + secs;
  }
  const secs = parseInt(str);
  return isNaN(secs) ? null : secs;
}

/**
 * Extract the game number within its match from an LP GameId. GameIds end
 * with the game number (`..._Finals_1_1` → 1, `..._Week 5_5_2` → 2).
 */
function parseGameNumber(gameId) {
  if (!gameId) return null;
  const last = String(gameId).split("_").pop();
  const n = parseInt(last);
  return isNaN(n) ? null : n;
}

/**
 * Import team drafts from Leaguepedia (ScoreboardGames) — the fallback for
 * tournaments Grid.gg doesn't cover (no gridTeamId, or no Grid series found).
 *
 * Scoped to the tournament entry name intersected with the global dataRange
 * (tournamentWindow applies the per-entry dateStart/dateEnd clipping — §2.2).
 * Writes team_games rows with source='leaguepedia' and source_game_id = LP's
 * GameId (unique per game). No import-time dedup vs Grid rows — the display
 * layer dedups across sources (same rule as competitive_games).
 *
 * LP data contract (verified live 2026-08-02):
 *   - ScoreboardGames needs an EXACT team-name match — resolved from the
 *     team's leaguepediaUrl wiki slug (underscores → spaces), falling back
 *     to the user-facing team name.
 *   - Team1 = team listed first (Blue per Leaguepedia convention), Team2 =
 *     Red. ScoreboardGames has no explicit side field.
 *   - Winner: 1 = Team1 won, 2 = Team2 won.
 *   - Gamelength: "MM:SS" string; Patch: float.
 *   - DateTime_UTC comes back as "DateTime UTC" (space) from CargoExport.
 *   - Team1Picks/Team2Picks/Team1Bans/Team2Bans: JSON arrays of champion
 *     names (CargoExport returns them pre-parsed).
 *
 * @returns {Promise<{ importedGames: number, skipped?: boolean }>}
 */
async function importTeamDataFromLeaguepedia(turso, team, tournamentEntry) {
  const tournamentName = tournamentNameOf(tournamentEntry);
  const teamName = team.teamName || "";
  console.log(
    `[Team][LP] Importing ${teamName} drafts from Leaguepedia for tournament: ${tournamentName}`,
  );
  await reportProgress({
    step: "team",
    status: "started",
    tournament: tournamentName,
    source: "leaguepedia",
  });

  // LP team name: prefer the team's Leaguepedia URL slug (ScoreboardGames
  // needs an exact team-name match; the slug IS the LP name — e.g. the wiki
  // page "WLGaming_Esports" stores "WLGaming Esports"). Fall back to the
  // user-facing team name.
  const lpTeamName = team.leaguepediaUrl
    ? decodeURIComponent(team.leaguepediaUrl.match(/wiki\/([^/?]+)/)?.[1] || "").replace(/_/g, " ") || teamName
    : teamName;
  if (!lpTeamName) {
    console.warn(
      "[Team][LP] No Leaguepedia team name (no leaguepediaUrl, no teamName) — skipping LP import",
    );
    return { importedGames: 0, skipped: true };
  }

  // Scope: global dataRange intersected with the per-tournament window.
  const dataRange = JSON.parse(OPTIONS_JSON).dataRange || {};
  const window = tournamentWindow(tournamentEntry, dataRange);

  const safeName = lpTeamName.replace(/"/g, '\\"');
  const safeTournament = tournamentName.replace(/"/g, '\\"');
  const whereParts = [`(SG.Team1 = "${safeName}" OR SG.Team2 = "${safeName}")`];
  if (safeTournament) whereParts.push(`SG.Tournament = "${safeTournament}"`);
  if (window.startTimeGte) whereParts.push(`SG.DateTime_UTC >= "${window.startTimeGte}"`);
  if (window.startTimeLte) whereParts.push(`SG.DateTime_UTC <= "${window.startTimeLte}"`);

  const data = await fetchLeaguepediaCargo({
    tables: "ScoreboardGames=SG",
    fields: [
      "SG.GameId",
      "SG.DateTime_UTC",
      "SG.Tournament",
      "SG.Team1",
      "SG.Team2",
      "SG.Winner",
      "SG.Gamelength",
      "SG.Patch",
      "SG.Team1Picks",
      "SG.Team2Picks",
      "SG.Team1Bans",
      "SG.Team2Bans",
    ].join(","),
    where: whereParts.join(" AND "),
    order_by: "SG.DateTime_UTC DESC",
    limit: "500",
    format: "json",
  });

  const rows = parseCargoJsonRows(data);
  if (rows.length === 0) {
    console.log(`[Team][LP] No Leaguepedia games for ${lpTeamName} in "${tournamentName}"`);
    console.log(
      `::notice::[Team][LP] Zero results — check the Leaguepedia team name ("${lpTeamName}") and that the tournament exists on Leaguepedia (Grid and LP tournament names can differ)`,
    );
    await reportProgress({
      step: "team",
      status: "completed",
      tournament: tournamentName,
      source: "leaguepedia",
      totalImported: 0,
    });
    return { importedGames: 0 };
  }

  console.log(`::notice::[Team][LP] Found ${rows.length} Leaguepedia games for ${lpTeamName} in "${tournamentName}"`);

  let importedGames = 0;
  for (const row of rows) {
    const isTeam1 = row.Team1 === lpTeamName;
    const gameDate = String(row["DateTime UTC"] || row.DateTime_UTC || "").slice(0, 10);
    const gameId = row.GameId || "";
    if (!gameDate || !gameId) continue;

    const ourPicks = normalizeChampionList(row[isTeam1 ? "Team1Picks" : "Team2Picks"]);
    const ourBans = normalizeChampionList(row[isTeam1 ? "Team1Bans" : "Team2Bans"]);
    const oppPicks = normalizeChampionList(row[isTeam1 ? "Team2Picks" : "Team1Picks"]);
    const oppBans = normalizeChampionList(row[isTeam1 ? "Team2Bans" : "Team1Bans"]);

    // Team1 = Blue, Team2 = Red (Leaguepedia convention).
    const bluePicks = normalizeChampionList(row.Team1Picks);
    const redPicks = normalizeChampionList(row.Team2Picks);
    const blueBans = normalizeChampionList(row.Team1Bans);
    const redBans = normalizeChampionList(row.Team2Bans);

    const winner = parseInt(row.Winner || 0);
    const win = isTeam1 ? (winner === 1 ? 1 : 0) : winner === 2 ? 1 : 0;
    const opponent = isTeam1 ? row.Team2 : row.Team1;

    try {
      await turso.execute({
        sql: `INSERT OR IGNORE INTO team_games
              (team_id, workspace_id, tournament, game_date, opponent, win, picks, bans, opp_picks, opp_bans, source, source_game_id,
               blue_picks, red_picks, blue_bans, red_bans, blue_team, red_team, game_number, patch, duration)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'leaguepedia', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          team.teamId,
          WORKSPACE_ID,
          tournamentName || row.Tournament || "Unknown",
          gameDate,
          opponent,
          win,
          JSON.stringify(ourPicks),
          JSON.stringify(ourBans),
          JSON.stringify(oppPicks),
          JSON.stringify(oppBans),
          gameId, // source_game_id — LP GameId, unique per game
          JSON.stringify(bluePicks),
          JSON.stringify(redPicks),
          JSON.stringify(blueBans),
          JSON.stringify(redBans),
          row.Team1 || null,
          row.Team2 || null,
          parseGameNumber(gameId),
          row.Patch != null ? String(row.Patch) : null,
          parseGamelength(row.Gamelength),
        ],
      });
      importedGames++;
    } catch (err) {
      if (!err.message.includes("UNIQUE"))
        console.warn(`[Team][LP] team_games insert error: ${err.message}`);
    }
  }

  console.log(`[Team][LP] Imported ${importedGames} Leaguepedia games for "${tournamentName}"`);
  await reportProgress({
    step: "team",
    status: "completed",
    tournament: tournamentName,
    source: "leaguepedia",
    totalImported: importedGames,
  });

  return { importedGames };
}

// ─── Step 5: Post-import aggregation ─────────────────────────────────

async function computeAggregates(turso, playerIds) {
  // Deprecated — precomputed tables removed in schema v2.
  // Raw-data queries used instead.
  console.log("[Aggregation] Skipped — using raw-data query architecture");
  await reportProgress({ step: "aggregation", status: "skipped" });
}

// ─── Main entry point ─────────────────────────────────────────────────

/**
 * Phase order (Sprint 5.3):
 *   1. op.gg SoloQ       (deprecated — will be removed in Sprint 6.1)
 *   2. Team Import (Grid)  — writes source='grid' rows in competitive_games;
 *                            falls back to Leaguepedia team import (source=
 *                            'leaguepedia' team_games rows) when the team has
 *                            no gridTeamId or a tournament has no Grid series
 *                            (Sprint 5.6)
 *   3. Leaguepedia Competitive — writes source='leaguepedia' rows
 *   4. Riot API SoloQ     — per-game role-filtered soloQ data
 *   5. Aggregation        — no-op (raw-query architecture)
 *   6. Flag Computation   — single-player flags (comparative not yet wired)
 *   7. Source Split Cache — scoutedPlayers/{pid}/sources (Sprint 5.5)
 *
 * Dual-source contract: Grid and Leaguepedia write to competitive_games
 * independently. They use different ID spaces (grid_game_id vs
 * leaguepedia_game_id), so the same game can have rows from both sources.
 * The UI surfaces source provenance per row. There is NO cross-source
 * dedup at the row level; INSERT OR IGNORE handles within-source dedup.
 *
 * Default scope: last 12 months. User-configurable via
 * OPTIONS_JSON.dataRange.{startDate,endDate}.
 */

async function main() {
  console.log("=== Full Scouting Scan (GitHub Actions) ===");
  console.log(`Job ID: ${JOB_ID}, Workspace: ${WORKSPACE_ID}, Mode: ${MODE}`);

  // Initialize champion name mapper from Data Dragon
  console.log("::group::Initializing Champion Name Mapper");
  championMapper = new ChampionNameMapper();
  try {
    await championMapper.initialize();
    console.log(`::notice::Champion name mapper initialized successfully`);
  } catch (error) {
    console.warn(
      `[Warning] Could not initialize champion name mapper: ${error.message}`,
    );
    console.warn(`[Warning] Falling back to passthrough name normalization`);
  }
  console.log("::endgroup::");

  const players = JSON.parse(PLAYERS_JSON);
  const options = JSON.parse(OPTIONS_JSON);
  const team = JSON.parse(TEAM_JSON);

  if (!Array.isArray(players) || players.length === 0) {
    console.error("[Fatal] No players provided");
    process.exit(1);
  }

  const turso = createClient({ url: TURSO_URL, authToken: TURSO_TOKEN });
  console.log(`Connected to Turso, ${players.length} players`);

  // Default scope: last 12 months when startDate isn't provided (Sprint 5.3).
  // Both Grid (via dataRange.startTimeGte/Lte) and LP (via the whereClause
  // built in scanCompetitive) honour the same window.
  const dataRange = options.dataRange || {};
  if (!dataRange.startDate) {
    dataRange.startDate = new Date(
      Date.now() - 365 * 24 * 60 * 60 * 1000,
    )
      .toISOString()
      .split("T")[0];
  }
  const startTimestamp = dataRange.startDate
    ? Math.floor(new Date(dataRange.startDate).getTime() / 1000)
    : null;
  const endTimestamp = dataRange.endDate
    ? Math.floor(new Date(dataRange.endDate).getTime() / 1000)
    : null;

  // Track total games across all steps for a single import_log row at the end
  let totalGamesAllSteps = 0;
  let totalErrorsAllSteps = 0;

  await reportProgress({ status: "started", totalPlayers: players.length });

  // ── Step 1: op.gg SoloQ ─────────────────────────────────────────
  console.log("::group::Step 1: op.gg SoloQ Scraping");
  if (options.doSoloq && options.soloqMethod === "opgg") {
    if (!(await isStepComplete(turso, "soloq_opgg"))) {
      console.log("\n=== Step 1: op.gg SoloQ ===");
      await reportProgress({ step: "opgg", status: "started" });

      let totalGames = 0,
        totalErrors = 0;
      for (let i = 0; i < players.length; i++) {
        await reportProgress({
          step: "opgg",
          currentPlayer: i + 1,
          totalPlayers: players.length,
          playerName: players[i].riotId,
        });
        const result = await scrapeOpgg(turso, players[i], options);
        totalGames += result.gamesImported;
        if (result.error) totalErrors++;
      }

      totalGamesAllSteps += totalGames;
      totalErrorsAllSteps += totalErrors;
      await reportProgress({ step: "opgg", status: "completed", totalGames });
    } else {
      console.log("[Resume] op.gg SoloQ already complete, skipping");
    }
  }

  // ── Step 2: Team Import (Grid) — runs BEFORE competitive so the
  //    Grid import writes source='grid' rows first; LP only adds
  //    source='leaguepedia' rows for games Grid doesn't cover.
  //    (Sprint 5.3 phase reorder)
  console.log("::endgroup::");
  console.log("::group::Step 2: Team Import");
  if (options.doTeam && team.teamId && team.selectedTournaments?.length > 0) {
    if (!(await isStepComplete(turso, "team_import"))) {
      console.log("\n=== Step 2: Team Import (Grid) ===");
      await reportProgress({ step: "team", status: "started" });

      let totalImported = 0;
      for (const tournament of team.selectedTournaments) {
        await reportProgress({ step: "team", tournament });
        const result = await importTeamData(turso, team, tournament);
        totalImported += result.importedGames || 0;
      }
      totalGamesAllSteps += totalImported;
      await reportProgress({
        step: "team",
        status: "completed",
        totalImported,
      });
    } else {
      console.log("[Resume] Team import already complete, skipping");
    }
  }

  // ── Step 3: Leaguepedia Competitive ──────────────────────────────
  //    Writes source='leaguepedia' rows. No cross-source dedup:
  //    Grid and LP use different ID spaces, so the same game can
  //    legitimately have rows from both. UI surfaces source per row.
  console.log("::endgroup::");
  console.log("::group::Step 3: Leaguepedia Competitive");
  if (options.doCompetitive) {
    if (!(await isStepComplete(turso, "competitive"))) {
      console.log("\n=== Step 3: Leaguepedia Competitive ===");
      await reportProgress({ step: "competitive", status: "started" });

      let totalImported = 0;
      for (let i = 0; i < players.length; i++) {
        await reportProgress({
          step: "competitive",
          currentPlayer: i + 1,
          totalPlayers: players.length,
          playerName: players[i].riotId,
        });
        let result = await scanCompetitive(
          turso,
          players[i],
          dataRange.startDate,
          dataRange.endDate,
        );

        if (result.needsFallback && result.fallbackName) {
          console.log(
            `[Competitive] Retrying with fallback name: ${result.fallbackName}`,
          );
          await sleep(1000);
          const fallbackPlayer = {
            ...players[i],
            leaguepediaSlug: null,
            playerOverrideName: result.fallbackName,
          };
          result = await scanCompetitive(
            turso,
            fallbackPlayer,
            dataRange.startDate,
            dataRange.endDate,
          );
        }

        totalImported += result.gamesImported || 0;

        await sleep(300);
      }
      totalGamesAllSteps += totalImported;
      await reportProgress({
        step: "competitive",
        status: "completed",
        totalImported,
      });
    } else {
      console.log("[Resume] Competitive already complete, skipping");
    }
  }

  // ── Step 4: Riot API SoloQ ───────────────────────────────────────
  console.log("::endgroup::");
  console.log("::group::Step 4: Riot API SoloQ");
  if (options.doSoloq && options.soloqMethod === "riot-api") {
    if (!(await isStepComplete(turso, "soloq_riot_api"))) {
      console.log("\n=== Step 4: Riot API SoloQ ===");
      await reportProgress({ step: "riot-api", status: "started" });

      if (!RIOT_API_KEY) {
        console.error("[Fatal] RIOT_API_KEY not set, cannot run Riot API scan");
      } else {
        let totalFound = 0,
          totalImported = 0;
        for (let i = 0; i < players.length; i++) {
          await reportProgress({
            step: "riot-api",
            currentPlayer: i + 1,
            totalPlayers: players.length,
            playerName: players[i].riotId,
          });
          const result = await scanRiotApi(
            turso,
            players[i],
            startTimestamp,
            endTimestamp,
          );
          totalFound += result.gamesFound || 0;
          totalImported += result.gamesImported || 0;
          await sleep(500);
        }
        totalGamesAllSteps += totalImported;
        await reportProgress({
          step: "riot-api",
          status: "completed",
          totalFound,
          totalImported,
        });
      }
    } else {
      console.log("[Resume] Riot API SoloQ already complete, skipping");
    }
  }

  // ── Step 5: Aggregation ──────────────────────────────────────────
  console.log("::endgroup::");
  console.log("::group::Step 5: Aggregation");
  console.log(
    "[Aggregation] Using raw-data query architecture — precomputed tables deprecated",
  );
  await reportProgress({ step: "aggregation", status: "completed" });

  // ── Step 6: Flag Computation ─────────────────────────────────────
  console.log("::endgroup::");
  if (options.doFlags !== false) {
    console.log("::group::Step 6: Flag Computation");
    try {
      const { computePlayerFlags, savePlayerFlags, DEFAULT_THRESHOLDS } = require('./compute-flags.cjs');
      const { getFirestore } = require('./firebase-admin');

      // Load custom thresholds from Firestore
      let thresholds = { ...DEFAULT_THRESHOLDS };
      try {
        const db = getFirestore();
        const thresholdDoc = await db.collection('workspaces').doc(workspaceId)
          .collection('_settings').doc('flagThresholds').get();
        if (thresholdDoc.exists) {
          thresholds = { ...DEFAULT_THRESHOLDS, ...thresholdDoc.data() };
        }
      } catch (err) {
        console.warn(`[Flags] Could not load thresholds: ${err.message}, using defaults`);
      }

      // Compute single-player flags for each player
      for (const player of players) {
        const playerFlags = await computePlayerFlags(turso, player.playerId, player.name || player.riotId || 'Unknown', thresholds);
        await savePlayerFlags(workspaceId, player.playerId, playerFlags);
      }

      console.log(`[Flags] Computed flags for ${players.length} players`);
      await reportProgress({ step: "flags", status: "completed" });
    } catch (err) {
      console.error(`[Flags] Error computing flags: ${err.message}`);
      totalErrorsAllSteps++;
      await reportProgress({ step: "flags", status: "failed", error: err.message });
    }
  } else {
    console.log("[Flags] Skipping flag computation (doFlags=false)");
  }

  // ── Step 7: Source split cache (Sprint 5.5) ──────────────────────
  // Writes scoutedPlayers/{playerId}/sources — the last-known Grid/LP
  // source split per player. Pure cache for fast UI rendering; non-fatal.
  if (options.doSourceSplits !== false) {
    console.log("::group::Step 7: Source Split Cache");
    try {
      const { writePlayerSourceSplits } = require('./source-split.cjs');
      const { getFirestore } = require('./firebase-admin');
      const db = getFirestore();
      const result = await writePlayerSourceSplits({
        turso,
        db,
        workspaceId,
        players,
        scope: {
          startDate: dataRange.startDate || null,
          endDate: dataRange.endDate || null,
        },
      });
      console.log(
        `[SourceSplit] Wrote source splits for ${result.written}/${players.length} players`,
      );
    } catch (err) {
      console.warn(`[SourceSplit] Skipped (non-fatal): ${err.message}`);
    }
    console.log("::endgroup::");
  }

  // ── Single import_log row for the entire run ─────────────────────
  await logStep(
    turso,
    "full_scan",
    totalGamesAllSteps,
    totalErrorsAllSteps > 0 ? "partial" : "success",
    totalErrorsAllSteps > 0 ? `${totalErrorsAllSteps} steps had errors` : null,
  );

  // ── Done ─────────────────────────────────────────────────────────
  console.log("\n=== Scan Complete! ===");
  console.log("::endgroup::");
  await reportProgress({
    status: "completed",
    completedAt: new Date().toISOString(),
  });

  // Write version marker so frontend caches invalidate
  await writeScoutingVersionMarker().catch(err => {
    console.warn(`[Cleanup] Failed to write version marker: ${err.message}`);
  });

  // Release Firestore lock so other scans can proceed
  await releaseFirestoreLock().catch(err => {
    console.warn(`[Cleanup] Failed to release lock: ${err.message}`);
  });
}

// ─── Exports (Sprint 5.4) ──────────────────────────────────────────────
// The team-import functions are exported so the test harness can drive the
// exact production code path without running main(). setChampionMapper is a
// test hook that lets the harness exercise the mapper-based normalization
// exactly like a real scan (main() initializes the mapper itself). When run
// directly (CI workflow), main() executes as before.
module.exports = {
  importTeamData,
  importTeamDataFromGrid,
  importTeamDataFromLeaguepedia,
  setChampionMapper(mapper) {
    championMapper = mapper;
  },
};

if (require.main === module) {
  main().catch(async (error) => {
    console.error("[Fatal]", error);
    console.log("::endgroup::");
    await reportProgress({ status: "failed", error: error.message }).catch(
      () => {},
    );
    // Release Firestore lock on failure too
    await releaseFirestoreLock().catch(() => {});
    process.exit(1);
  });
}
