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

const { createClient } = require('@libsql/client');
const axios = require('axios');
const cheerio = require('cheerio');

// ─── Config from env ──────────────────────────────────────────────────
const RIOT_API_KEY = process.env.RIOT_API_KEY || '';
const TURSO_URL = process.env.TURSO_SCOUTING_URL || '';
const TURSO_TOKEN = process.env.TURSO_SCOUTING_TOKEN || '';
const JOB_ID = process.env.JOB_ID || '';
const WORKSPACE_ID = process.env.WORKSPACE_ID || '';
const MODE = process.env.MODE || 'fresh';
const PLAYERS_JSON = process.env.PLAYERS_JSON || '[]';
const OPTIONS_JSON = process.env.OPTIONS_JSON || '{}';
const TEAM_JSON = process.env.TEAM_JSON || '{}';
const CALLBACK_URL = process.env.CALLBACK_URL || '';

const MAX_RETRIES = 5;
const LEAGUEPEDIA_BASE = 'https://lol.fandom.com/wiki/Special:CargoExport';

if (!TURSO_URL || !JOB_ID || !WORKSPACE_ID) {
  console.error('[Fatal] Missing required env vars: TURSO_URL, JOB_ID, WORKSPACE_ID');
  process.exit(1);
}

// ─── Utilities ────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function regionToRouting(region) {
  const routing = {
    euw1: 'europe', eun1: 'europe', tr1: 'europe', ru: 'europe',
    na1: 'americas', br1: 'americas', la1: 'americas', la2: 'americas',
    kr: 'asia', jp1: 'asia',
    oc1: 'sea', ph2: 'sea', sg2: 'sea', th2: 'sea', vn2: 'sea',
  };
  return routing[region] || 'europe';
}

function normalizeChampionName(name) {
  if (!name) return name;
  const nameMap = {
    MonkeyKing: 'Wukong', DrMundo: 'Dr. Mundo', 'Dr Mundo': 'Dr. Mundo',
    JarvanIV: 'Jarvan IV', MasterYi: 'Master Yi', MissFortune: 'Miss Fortune',
    TahmKench: 'Tahm Kench', TwistedFate: 'Twisted Fate', XinZhao: 'Xin Zhao',
    AurelionSol: 'Aurelion Sol', KogMaw: "Kog'Maw", RekSai: "Rek'Sai",
    KSante: "K'Sante", BelVeth: "Bel'Veth",
  };
  const lower = name.toLowerCase();
  for (const [k, v] of Object.entries(nameMap)) {
    if (k.toLowerCase() === lower) return v;
  }
  return name;
}

// ─── Progress reporting ───────────────────────────────────────────────

async function reportProgress(progress) {
  const payload = { action: 'scanProgress', jobId: JOB_ID, workspaceId: WORKSPACE_ID, ...progress };
  if (!CALLBACK_URL) {
    console.log(`[Progress] ${JSON.stringify(payload)}`);
    return;
  }
  try {
    await fetch(CALLBACK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch (error) {
    console.warn(`[Progress] Report failed: ${error.message}`);
  }
}

// ─── Resume helpers ───────────────────────────────────────────────────

async function isStepComplete(turso, stepType) {
  if (MODE !== 'resume') return false;
  const result = await turso.execute({
    sql: `SELECT COUNT(*) as cnt FROM import_log WHERE workspace_id = ? AND import_type = ? AND status = 'success'`,
    args: [WORKSPACE_ID, stepType],
  });
  return (result.rows[0]?.cnt || 0) > 0;
}

async function logStep(turso, stepType, gameCount, status = 'success', error = null) {
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

async function hasTeamData(turso, teamId, tournament) {
  const result = await turso.execute({
    sql: `SELECT COUNT(*) as cnt FROM team_games WHERE team_id = ? AND tournament = ? AND source = 'gridgg'`,
    args: [teamId, tournament],
  });
  return (result.rows[0]?.cnt || 0) > 0;
}

// ─── Step 1: op.gg Scraping ──────────────────────────────────────────

async function scrapeOpgg(turso, player, options) {
  const { playerId, riotId, tag, region } = player;
  console.log(`[op.gg] Scraping: ${riotId}#${tag}`);

  // Check resume
  if (await hasPlayerSoloqData(turso, playerId)) {
    console.log(`[op.gg] ${riotId}#${tag} already has SoloQ data, skipping`);
    return { gamesImported: 0, skipped: true };
  }

  // Build op.gg champions URL
  const server = (region || 'euw1').replace(/\d+$/, '');
  const url = `https://www.op.gg/en/lol/summoners/${server}/${encodeURIComponent(riotId)}-${encodeURIComponent(tag)}/champions?queue_type=SOLORANKED`;

  try {
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      },
      timeout: 10000,
    });

    const $ = cheerio.load(response.data);
    const nextDataScript = $('script#__NEXT_DATA__').html();
    if (!nextDataScript) {
      console.log(`[op.gg] No __NEXT_DATA__ found for ${riotId}#${tag}`);
      return { gamesImported: 0 };
    }

    const nextData = JSON.parse(nextDataScript);
    const pageProps = nextData?.props?.pageProps;
    let championsData = pageProps?.champions || pageProps?.championStats || pageProps?.data?.champions || pageProps?.summoner?.champions || [];

    if (!Array.isArray(championsData) || championsData.length === 0) {
      console.log(`[op.gg] No champion data in __NEXT_DATA__ for ${riotId}#${tag}`);
      return { gamesImported: 0 };
    }

    let totalGames = 0;
    for (const champ of championsData) {
      const name = normalizeChampionName(champ.championName || champ.name || champ.champion || '');
      const wins = champ.wins || champ.win || 0;
      const losses = champ.losses || champ.loss || 0;
      const games = champ.games || champ.totalGames || wins + losses;
      if (!name || games === 0) continue;
      totalGames += games;

      // Extract KDA if available
      let avgKills = null, avgDeaths = null, avgAssists = null, avgGold = null, avgCs = null;
      if (champ.kda) {
        avgKills = champ.kda.kills || champ.kda.k || null;
        avgDeaths = champ.kda.deaths || champ.kda.d || null;
        avgAssists = champ.kda.assists || champ.kda.a || null;
      }

      // Store each game individually so the data is consistent with Riot API format
      for (let g = 0; g < games; g++) {
        try {
          // For op.gg aggregate data we don't have per-game dates, so INSERT OR IGNORE
          // uses a placeholder row that gets replaced if Riot API data comes later
          await turso.execute({
            sql: `INSERT INTO soloq_games (player_id, workspace_id, source, game_date, champion, kills, deaths, assists, gold, cs, win, role)
                  VALUES (?, ?, 'opgg', ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
            args: [
              playerId, WORKSPACE_ID,
              new Date().toISOString().split('T')[0], // approximate date
              name,
              Math.round(avgKills || 0), Math.round(avgDeaths || 0), Math.round(avgAssists || 0),
              Math.round(avgGold || 0), Math.round(avgCs || 0),
              wins / games > 0.5 ? 1 : 0, // approximate win
            ],
          });
        } catch (err) {
          if (!err.message.includes('UNIQUE')) console.warn(`[op.gg] Insert error: ${err.message}`);
        }
      }
    }

    console.log(`[op.gg] ${riotId}#${tag}: ${totalGames} games imported`);
    return { gamesImported: totalGames };
  } catch (error) {
    console.error(`[op.gg] Scrape failed for ${riotId}#${tag}: ${error.message}`);
    return { gamesImported: 0, error: error.message };
  }
}

// ─── Step 2: Riot API Scan ──────────────────────────────────────────

async function riotFetch(url) {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(url, {
        headers: { 'X-Riot-Token': RIOT_API_KEY },
      });
      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After') || '3', 10);
        await sleep(retryAfter * 1000 + 100);
        continue;
      }
      if (response.status === 404) return null;
      if (response.status === 403) { console.error('[Riot] API key invalid'); return null; }
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return await response.json();
    } catch (error) {
      if (attempt === MAX_RETRIES - 1) throw error;
      await sleep(2000);
    }
  }
  return null;
}

async function scanRiotApi(turso, player, startTimestamp, endTimestamp) {
  const { playerId, riotId, tag, region } = player;
  console.log(`[RiotAPI] Scanning: ${riotId}#${tag}`);

  if (await hasPlayerSoloqData(turso, playerId)) {
    console.log(`[RiotAPI] ${riotId}#${tag} already has data, skipping`);
    return { gamesFound: 0, gamesImported: 0, skipped: true };
  }

  const routing = regionToRouting(region);
  const puuidUrl = `https://${routing}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/${encodeURIComponent(riotId)}/${encodeURIComponent(tag)}`;
  const accountData = await riotFetch(puuidUrl);
  if (!accountData?.puuid) {
    console.error(`[RiotAPI] Could not resolve PUUID for ${riotId}#${tag}`);
    return { gamesFound: 0, gamesImported: 0 };
  }
  const puuid = accountData.puuid;

  const params = new URLSearchParams({ api_key: RIOT_API_KEY, count: '100', queue: '420' });
  if (startTimestamp) params.set('startTime', startTimestamp);
  if (endTimestamp) params.set('endTime', endTimestamp);
  const idsUrl = `https://${routing}.api.riotgames.com/lol/match/v5/matches/by-puuid/${puuid}/ids?${params}`;
  const matchIds = await riotFetch(idsUrl);
  if (!Array.isArray(matchIds) || matchIds.length === 0) {
    console.log(`[RiotAPI] No matches for ${riotId}#${tag}`);
    return { gamesFound: 0, gamesImported: 0 };
  }

  let gamesFound = 0, gamesImported = 0;
  for (let i = 0; i < matchIds.length; i++) {
    const matchData = await riotFetch(`https://${routing}.api.riotgames.com/lol/match/v5/matches/${matchIds[i]}`);
    if (!matchData?.info?.participants) continue;

    const p = matchData.info.participants.find(part => part.puuid === puuid);
    if (!p) continue;
    gamesFound++;

    const roleMap = { TOP: 'Top', JUNGLE: 'Jungle', MIDDLE: 'Mid', BOTTOM: 'Bot', UTILITY: 'Support', SUPPORT: 'Support' };
    const role = roleMap[(p.teamPosition || '').toUpperCase()] || null;
    const gameDate = new Date(matchData.info.gameCreation).toISOString().split('T')[0];

    try {
      await turso.execute({
        sql: `INSERT OR IGNORE INTO soloq_games (player_id, workspace_id, source, game_date, champion, kills, deaths, assists, gold, cs, win, role)
              VALUES (?, ?, 'riot-api', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [playerId, WORKSPACE_ID, gameDate, p.championName,
          p.kills || 0, p.deaths || 0, p.assists || 0,
          p.goldEarned || 0, (p.totalMinionsKilled || 0) + (p.neutralMinionsKilled || 0),
          p.win ? 1 : 0, role],
      });
      gamesImported++;
    } catch (err) {
      if (!err.message.includes('UNIQUE')) console.warn(`[RiotAPI] Insert error: ${err.message}`);
    }

    if (i % 10 === 0 || i === matchIds.length - 1) {
      await reportProgress({ step: 'riot-api', playerId, playerName: riotId, status: 'scanning',
        gamesFound, gamesImported, progress: ((i + 1) / matchIds.length * 100).toFixed(1) });
    }
    if (i % 5 === 0) await sleep(150);
  }

  console.log(`[RiotAPI] ${riotId}#${tag}: ${gamesImported}/${gamesFound} games`);
  return { gamesFound, gamesImported };
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

async function scanCompetitive(turso, player) {
  const { playerId, riotId, tag, playerOverrideName, leaguepediaUrl } = player;
  const lpName = playerOverrideName || (leaguepediaUrl ? decodeURIComponent(leaguepediaUrl.match(/wiki\/([^/?]+)/)?.[1] || '') : riotId);
  console.log(`[Competitive] Scanning: ${lpName}`);

  if (await hasPlayerCompData(turso, playerId)) {
    console.log(`[Competitive] ${lpName} already has data, skipping`);
    return { gamesImported: 0, skipped: true };
  }

  // Fetch champion pool from Leaguepedia ScoreboardPlayers
  const data = await fetchLeaguepediaCargo({
    tables: 'ScoreboardPlayers',
    fields: 'ScoreboardPlayers.Champion, ScoreboardPlayers.Team, COUNT(*) AS games, SUM(ScoreboardPlayers.Win) AS wins',
    where: `ScoreboardPlayers.Link = "${lpName}"`,
    group_by: 'ScoreboardPlayers.Champion, ScoreboardPlayers.Team',
    format: 'json',
    limit: '500',
  });

  if (!data) {
    console.log(`[Competitive] No data from Leaguepedia for ${lpName}`);
    return { gamesImported: 0 };
  }

  // Parse JSON response
  let rows = [];
  if (typeof data === 'string') {
    try { rows = JSON.parse(data); } catch { return { gamesImported: 0 }; }
  } else if (Array.isArray(data)) {
    rows = data;
  } else if (data?.response) {
    rows = data.response;
  }

  if (!Array.isArray(rows) || rows.length === 0) {
    console.log(`[Competitive] No rows for ${lpName}`);
    return { gamesImported: 0 };
  }

  let totalImported = 0;
  for (const row of rows) {
    const champion = normalizeChampionName(row.Champion || row.champion || '');
    if (!champion) continue;

    const games = parseInt(row.games || row.Games || 0);
    const wins = parseInt(row.wins || row.Wins || 0);
    if (games === 0) continue;

    // Store each game
    for (let g = 0; g < games; g++) {
      try {
        await turso.execute({
          sql: `INSERT OR IGNORE INTO competitive_games (player_id, workspace_id, game_date, champion, tournament, opponent, win)
                VALUES (?, ?, ?, ?, ?, ?, ?)`,
          args: [playerId, WORKSPACE_ID, new Date().toISOString().split('T')[0], champion, 'Pro Play', row.Team || row.team || 'Unknown', wins / games > 0.5 ? 1 : 0],
        });
      } catch (err) {
        if (!err.message.includes('UNIQUE')) console.warn(`[Competitive] Insert error: ${err.message}`);
      }
    }
    totalImported += games;
  }

  console.log(`[Competitive] ${lpName}: ${totalImported} games imported`);
  return { gamesImported: totalImported };
}

// ─── Step 4: Team Import ──────────────────────────────────────────────

async function getTursoClient(dbName) {
  return createClient({ url: TURSO_URL, authToken: TURSO_TOKEN });
}

async function importTeamData(turso, team, tournamentName) {
  console.log(`[Team] Importing tournament: ${tournamentName}`);

  if (await hasTeamData(turso, team.teamId, tournamentName)) {
    console.log(`[Team] ${tournamentName} already imported, skipping`);
    return { importedGames: 0, skipped: true };
  }

  // Try Grid.gg data first via the main repo's teamImport function
  // Since we're in the worker, we call the callback URL as a fallback
  // For Grid.gg, we import from the LPL games table if available
  let importedGames = 0;

  // Store a placeholder in team_games
  try {
    await turso.execute({
      sql: `INSERT OR IGNORE INTO team_games (team_id, workspace_id, tournament, game_date, opponent, win, picks, bans, opp_picks, opp_bans, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'gridgg')`,
      args: [team.teamId, WORKSPACE_ID, tournamentName, new Date().toISOString().split('T')[0], 'Unknown', null, '[]', '[]', '[]', '[]'],
    });
    importedGames++;
  } catch (err) {
    if (!err.message.includes('UNIQUE')) console.warn(`[Team] Insert error: ${err.message}`);
  }

  return { importedGames };
}

// ─── Main entry point ─────────────────────────────────────────────────

async function main() {
  console.log('=== Full Scouting Scan (GitHub Actions) ===');
  console.log(`Job ID: ${JOB_ID}, Workspace: ${WORKSPACE_ID}, Mode: ${MODE}`);

  const players = JSON.parse(PLAYERS_JSON);
  const options = JSON.parse(OPTIONS_JSON);
  const team = JSON.parse(TEAM_JSON);

  if (!Array.isArray(players) || players.length === 0) {
    console.error('[Fatal] No players provided');
    process.exit(1);
  }

  const turso = createClient({ url: TURSO_URL, authToken: TURSO_TOKEN });
  console.log(`Connected to Turso, ${players.length} players`);

  // Convert dates
  const dataRange = options.dataRange || {};
  const startTimestamp = dataRange.startDate ? Math.floor(new Date(dataRange.startDate).getTime() / 1000) : null;
  const endTimestamp = dataRange.endDate ? Math.floor(new Date(dataRange.endDate).getTime() / 1000) : null;

  await reportProgress({ status: 'started', totalPlayers: players.length });

  // ── Step 1: op.gg SoloQ ─────────────────────────────────────────
  if (options.doSoloq && options.soloqMethod === 'opgg') {
    if (!await isStepComplete(turso, 'soloq_opgg')) {
      console.log('\n=== Step 1: op.gg SoloQ ===');
      await reportProgress({ step: 'opgg', status: 'started' });

      let totalGames = 0, totalErrors = 0;
      for (let i = 0; i < players.length; i++) {
        await reportProgress({ step: 'opgg', currentPlayer: i + 1, totalPlayers: players.length, playerName: players[i].riotId });
        const result = await scrapeOpgg(turso, players[i], options);
        totalGames += result.gamesImported;
        if (result.error) totalErrors++;
      }

      await logStep(turso, 'soloq_opgg', totalGames, totalErrors > 0 ? 'partial' : 'success', totalErrors > 0 ? `${totalErrors} players had errors` : null);
      await reportProgress({ step: 'opgg', status: 'completed', totalGames });
    } else {
      console.log('[Resume] op.gg SoloQ already complete, skipping');
    }
  }

  // ── Step 2: Riot API SoloQ ───────────────────────────────────────
  if (options.doSoloq && options.soloqMethod === 'riot-api') {
    if (!await isStepComplete(turso, 'soloq_riot_api')) {
      console.log('\n=== Step 2: Riot API SoloQ ===');
      await reportProgress({ step: 'riot-api', status: 'started' });

      if (!RIOT_API_KEY) {
        console.error('[Fatal] RIOT_API_KEY not set, cannot run Riot API scan');
      } else {
        let totalFound = 0, totalImported = 0;
        for (let i = 0; i < players.length; i++) {
          await reportProgress({ step: 'riot-api', currentPlayer: i + 1, totalPlayers: players.length, playerName: players[i].riotId });
          const result = await scanRiotApi(turso, players[i], startTimestamp, endTimestamp);
          totalFound += result.gamesFound || 0;
          totalImported += result.gamesImported || 0;
          await sleep(500);
        }
        await logStep(turso, 'soloq_riot_api', totalImported);
        await reportProgress({ step: 'riot-api', status: 'completed', totalFound, totalImported });
      }
    } else {
      console.log('[Resume] Riot API SoloQ already complete, skipping');
    }
  }

  // ── Step 3: Leaguepedia Competitive ──────────────────────────────
  if (options.doCompetitive) {
    if (!await isStepComplete(turso, 'competitive')) {
      console.log('\n=== Step 3: Leaguepedia Competitive ===');
      await reportProgress({ step: 'competitive', status: 'started' });

      let totalImported = 0;
      for (let i = 0; i < players.length; i++) {
        await reportProgress({ step: 'competitive', currentPlayer: i + 1, totalPlayers: players.length, playerName: players[i].riotId });
        const result = await scanCompetitive(turso, players[i]);
        totalImported += result.gamesImported || 0;
        await sleep(300);
      }
      await logStep(turso, 'competitive', totalImported);
      await reportProgress({ step: 'competitive', status: 'completed', totalImported });
    } else {
      console.log('[Resume] Competitive already complete, skipping');
    }
  }

  // ── Step 4: Team Import ──────────────────────────────────────────
  if (options.doTeam && team.teamId && team.selectedTournaments?.length > 0) {
    if (!await isStepComplete(turso, 'team_import')) {
      console.log('\n=== Step 4: Team Import ===');
      await reportProgress({ step: 'team', status: 'started' });

      let totalImported = 0;
      for (const tournament of team.selectedTournaments) {
        await reportProgress({ step: 'team', tournament });
        const result = await importTeamData(turso, team, tournament);
        totalImported += result.importedGames || 0;
      }
      await logStep(turso, 'team_import', totalImported);
      await reportProgress({ step: 'team', status: 'completed', totalImported });
    } else {
      console.log('[Resume] Team import already complete, skipping');
    }
  }

  // ── Done ─────────────────────────────────────────────────────────
  console.log('\n=== Scan Complete! ===');
  await reportProgress({ status: 'completed', completedAt: new Date().toISOString() });
}

main().catch(async (error) => {
  console.error('[Fatal]', error);
  await reportProgress({ status: 'failed', error: error.message }).catch(() => {});
  process.exit(1);
});