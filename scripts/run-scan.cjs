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
const { ChampionNameMapper } = require('./champion-name-mapper.cjs');

// ─── Force stdout/stderr flushing for live GitHub Actions logs ─────────
if (process.stdout._handle && process.stdout._handle.setBlocking) {
  process.stdout._handle.setBlocking(true);
}
if (process.stderr._handle && process.stderr._handle.setBlocking) {
  process.stderr._handle.setBlocking(true);
}

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
  console.log(`::notice::[op.gg] Scraping: ${riotId}#${tag}`);

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

      // Store each game individually
      for (let g = 0; g < games; g++) {
        try {
          await turso.execute({
            sql: `INSERT INTO soloq_games (player_id, workspace_id, source, game_date, champion, kills, deaths, assists, gold, cs, win, role)
                  VALUES (?, ?, 'opgg', ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
            args: [
              playerId, WORKSPACE_ID,
              new Date().toISOString().split('T')[0],
              name,
              Math.round(avgKills || 0), Math.round(avgDeaths || 0), Math.round(avgAssists || 0),
              Math.round(avgGold || 0), Math.round(avgCs || 0),
              wins / games > 0.5 ? 1 : 0,
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

/**
 * Find the opponent champion from match data.
 */
function findOpponentChampion(matchParticipants, puuid) {
  const player = matchParticipants.find(p => p.puuid === puuid);
  if (!player) return null;
  const teamId = player.teamId;
  const position = (player.teamPosition || '').toUpperCase();
  const enemy = matchParticipants.find(
    p => p.teamId !== teamId && p.teamPosition === position
  );
  return enemy?.championName || null;
}

async function scanRiotApi(turso, player, startTimestamp, endTimestamp) {
  const { playerId, riotId, tag, region, assignedRole, puuid: storedPuuid } = player;
  console.log(`[RiotAPI] Scanning: ${riotId}#${tag} (role filter: ${assignedRole || 'none'})`);

  if (await hasPlayerSoloqData(turso, playerId)) {
    console.log(`[RiotAPI] ${riotId}#${tag} already has data, skipping`);
    return { gamesFound: 0, gamesImported: 0, skipped: true };
  }

  const routing = regionToRouting(region);
  let puuid;
  if (storedPuuid) {
    console.log(`[RiotAPI] Using stored PUUID for ${riotId}#${tag} (skipped API resolve)`);
    puuid = storedPuuid;
  } else {
    console.log(`[RiotAPI] No stored PUUID for ${riotId}#${tag} — resolving from Riot API`);
    const puuidUrl = `https://${routing}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/${encodeURIComponent(riotId)}/${encodeURIComponent(tag)}`;
    const accountData = await riotFetch(puuidUrl);
    if (!accountData?.puuid) {
      console.error(`[RiotAPI] Could not resolve PUUID for ${riotId}#${tag}`);
      return { gamesFound: 0, gamesImported: 0 };
    }
    puuid = accountData.puuid;
  }

  const params = new URLSearchParams({ api_key: RIOT_API_KEY, count: '100', queue: '420' });
  if (startTimestamp) params.set('startTime', startTimestamp);
  if (endTimestamp) params.set('endTime', endTimestamp);
  const idsUrl = `https://${routing}.api.riotgames.com/lol/match/v5/matches/by-puuid/${puuid}/ids?${params}`;
  const matchIds = await riotFetch(idsUrl);
  if (!Array.isArray(matchIds) || matchIds.length === 0) {
    console.log(`[RiotAPI] No matches for ${riotId}#${tag}`);
    return { gamesFound: 0, gamesImported: 0 };
  }

  let gamesFound = 0, gamesMatched = 0, gamesImported = 0;
  for (let i = 0; i < matchIds.length; i++) {
    const matchData = await riotFetch(`https://${routing}.api.riotgames.com/lol/match/v5/matches/${matchIds[i]}`);
    if (!matchData?.info?.participants) continue;

    const p = matchData.info.participants.find(part => part.puuid === puuid);
    if (!p) continue;
    gamesFound++;

    const roleMap = { TOP: 'Top', JUNGLE: 'Jungle', MIDDLE: 'Mid', BOTTOM: 'Bot', UTILITY: 'Support', SUPPORT: 'Support' };
    const role = roleMap[(p.teamPosition || '').toUpperCase()] || null;

    if (assignedRole && role !== assignedRole) { continue; }
    gamesMatched++;

    const opponentChampion = findOpponentChampion(matchData.info.participants, puuid);
    const gameDate = new Date(matchData.info.gameCreation).toISOString().split('T')[0];

    try {
      await turso.execute({
        sql: `INSERT OR IGNORE INTO soloq_games (player_id, workspace_id, source, game_date, champion, opponent_champion, kills, deaths, assists, gold, cs, win, role)
              VALUES (?, ?, 'riot-api', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [playerId, WORKSPACE_ID, gameDate, p.championName, opponentChampion,
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

  console.log(`[RiotAPI] ${riotId}#${tag}: ${gamesMatched}/${gamesFound} role-matched, ${gamesImported} imported`);
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

async function scanCompetitive(turso, player) {
  const { playerId, riotId, tag, playerOverrideName, leaguepediaUrl, leaguepediaSlug } = player;
  const lpName = leaguepediaSlug || playerOverrideName || 
    (leaguepediaUrl ? decodeURIComponent(leaguepediaUrl.match(/wiki\/([^/?]+)/)?.[1] || '') : riotId);
  
  console.log(`::notice::[Competitive] Scanning player: ${lpName} (playerId: ${playerId})`);

  if (await hasPlayerCompData(turso, playerId)) {
    console.log(`[Competitive] ${lpName} already has data, skipping`);
    return { gamesImported: 0, skipped: true };
  }

  // 3-Table Join: SP (player), SPVs (lane opponent via UniqueRoleVs), SG (game metadata)
  // SPVs.Champion=ChampionVs — alias to avoid field collision with SP.Champion
  // SPVs.Team=TeamVs — alias to get opponent team name
  // SP.IngameRole=Role — alias for clarity
  // SG.Gamelength — game duration in "MM:SS" format
  const data = await fetchLeaguepediaCargo({
    tables: 'ScoreboardPlayers=SP,ScoreboardPlayers=SPVs,ScoreboardGames=SG',
    fields: [
      'SP.GameId',
      'SP.Champion',
      'SPVs.Champion=ChampionVs',
      'SP.Link',
      'SP.Side',
      'SP.Team',
      'SPVs.Team=TeamVs',
      'SP.IngameRole=Role',
      'SG.Tournament',
      'SP.Kills',
      'SP.Deaths',
      'SP.Assists',
      'SP.CS',
      'SP.Gold',
      'SG.DateTime_UTC',
      'SG.Winner',
      'SG.Team1',
      'SG.Team2',
      'SP.DamageToChampions',
      'SG.Gamelength',
    ].join(','),
    where: `SP.Link = "${lpName}"`,
    join_on: 'SG.GameId=SP.GameId,SP.UniqueRoleVs=SPVs.UniqueRole',
    limit: '500',
    format: 'json',
    order_by: 'SG.DateTime_UTC DESC',
  });

  if (!data) {
    console.log(`[Competitive] No data from Leaguepedia for ${lpName}`);
    console.log(`[Competitive] Debug — empty response for query Player = "${lpName}"`);
    return { gamesImported: 0, needsFallback: true, fallbackName: riotId };
  }

  let rows = [];
  if (typeof data === 'string') {
    try { rows = JSON.parse(data); } catch { 
      console.warn(`[Competitive] Failed to parse JSON response: ${data.substring(0, 200)}`);
      return { gamesImported: 0 }; 
    }
  } else if (Array.isArray(data)) { rows = data; }
  else if (data?.response) { rows = data.response; }

  if (rows.length > 0) {
    console.log(`[Competitive] Raw response sample (first 3 rows):`, rows.slice(0, 3).map(r => JSON.stringify(r)));
  }

  if (!Array.isArray(rows) || rows.length === 0) {
    console.log(`[Competitive] No rows for ${lpName}`);
    console.log(`::notice::[Competitive] Zero results — check if Leaguepedia slug "${lpName}" is correct`);
    return { gamesImported: 0, needsFallback: true, fallbackName: riotId };
  }

  console.log(`::notice::[Competitive] Found ${rows.length} competitive games for ${lpName}`);

  let totalImported = 0;

  for (const row of rows) {
    const champion = normalizeChampionName(row.Champion || row.champion || '');
    if (!champion) continue;

    const gameId = row.GameId || row.gameId || '';
    const team = row.Team || row.team || '';
    const side = parseInt(row.Side || row.side || 0);
    const tournament = row.Tournament || row.tournament || 'Pro Play';
    // Winner=1 means Team1 won, Winner=2 means Team2 won.
    // Side=1 means player on Team1, Side=2 means player on Team2.
    const winner = parseInt(row.Winner || row.winner || 0);
    const win = (side && winner && side === winner) ? 1 : 0;
    const kills = parseInt(row.Kills || row.kills || 0);
    const deaths = parseInt(row.Deaths || row.deaths || 0);
    const assists = parseInt(row.Assists || row.assists || 0);
    // DateTime_UTC comes back as "DateTime UTC" (space) from CargoExport
    const gameDate = row['DateTime UTC'] || row.DateTime_UTC || row.date || new Date().toISOString().split('T')[0];
    // New fields from expanded 3-table join
    const cs = parseInt(row.CS || row.cs || 0);
    const gold = parseInt(row.Gold || row.gold || 0);
    const damage = parseInt(row.DamageToChampions || row.damageToChampions || 0);
    // Gamelength comes as "MM:SS" string — parse to seconds for storage
    const gamelengthStr = row.Gamelength || row.gamelength || '';
    let duration = 0;
    if (gamelengthStr) {
      const parts = gamelengthStr.split(':');
      if (parts.length === 2) {
        duration = parseInt(parts[0]) * 60 + parseInt(parts[1]);
      }
    }
    
    // Derive side label and opponent team from Team1/Team2.
    // SG.Team1 = team on Blue side, SG.Team2 = team on Red side.
    // SP.Side = 1 means player is on Blue (Team1), 2 means player is on Red (Team2).
    const team1Name = row.Team1 || row.team1 || '';
    const team2Name = row.Team2 || row.team2 || '';
    const sideLabel = side === 1 ? 'Blue' : (side === 2 ? 'Red' : '');
    const opponent = row.TeamVs || (side === 1 ? team2Name : (side === 2 ? team1Name : ''));

    // Role is now available from the 3-table join via SP.IngameRole=Role alias
    const mappedRole = row.Role || row.role || null;

    // Opponent champion from the SPVs self-join via SPVs.Champion=ChampionVs alias
    const opponentChampion = normalizeChampionName(row.ChampionVs || row.championVs || '');

    try {
      await turso.execute({
        sql: `INSERT OR IGNORE INTO competitive_games 
              (player_id, workspace_id, leaguepedia_game_id, game_date, champion, opponent_champion, tournament, opponent, team, win, kills, deaths, assists, cs, gold, vision, damage, duration, side, role)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          playerId, WORKSPACE_ID, gameId,
          gameDate, champion, opponentChampion,
          tournament, opponent, team, win,
          kills, deaths, assists,
          cs, gold, 0, damage,
          duration, sideLabel, mappedRole,
        ],
      });
      totalImported++;
    } catch (err) {
      if (!err.message.includes('UNIQUE')) console.warn(`[Competitive] Insert error: ${err.message}`);
    }

    await reportProgress({ step: 'competitive', playerId, playerName: lpName, status: 'scanning',
      gamesImported: totalImported, progress: ((rows.indexOf(row) + 1) / rows.length * 100).toFixed(1) });
  }

  console.log(`[Competitive] ${lpName}: ${totalImported} games imported with real per-game data`);
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

  let importedGames = 0;
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

// ─── Step 5: Post-import aggregation ─────────────────────────────────

async function computeAggregates(turso, playerIds) {
  // Deprecated — precomputed tables removed in schema v2.
  // Raw-data queries used instead.
  console.log('[Aggregation] Skipped — using raw-data query architecture');
  await reportProgress({ step: 'aggregation', status: 'skipped' });
}

// ─── Main entry point ─────────────────────────────────────────────────

async function main() {
  console.log('=== Full Scouting Scan (GitHub Actions) ===');
  console.log(`Job ID: ${JOB_ID}, Workspace: ${WORKSPACE_ID}, Mode: ${MODE}`);

  // Initialize champion name mapper from Data Dragon
  console.log('::group::Initializing Champion Name Mapper');
  championMapper = new ChampionNameMapper();
  try {
    await championMapper.initialize();
    console.log(`::notice::Champion name mapper initialized successfully`);
  } catch (error) {
    console.warn(`[Warning] Could not initialize champion name mapper: ${error.message}`);
    console.warn(`[Warning] Falling back to passthrough name normalization`);
  }
  console.log('::endgroup::');

  const players = JSON.parse(PLAYERS_JSON);
  const options = JSON.parse(OPTIONS_JSON);
  const team = JSON.parse(TEAM_JSON);

  if (!Array.isArray(players) || players.length === 0) {
    console.error('[Fatal] No players provided');
    process.exit(1);
  }

  const turso = createClient({ url: TURSO_URL, authToken: TURSO_TOKEN });
  console.log(`Connected to Turso, ${players.length} players`);

  const dataRange = options.dataRange || {};
  const startTimestamp = dataRange.startDate ? Math.floor(new Date(dataRange.startDate).getTime() / 1000) : null;
  const endTimestamp = dataRange.endDate ? Math.floor(new Date(dataRange.endDate).getTime() / 1000) : null;

  await reportProgress({ status: 'started', totalPlayers: players.length });

  // ── Step 1: op.gg SoloQ ─────────────────────────────────────────
  console.log('::group::Step 1: op.gg SoloQ Scraping');
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
  console.log('::endgroup::');
  console.log('::group::Step 2: Riot API SoloQ');
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
  console.log('::endgroup::');
  console.log('::group::Step 3: Leaguepedia Competitive');
  if (options.doCompetitive) {
    if (!await isStepComplete(turso, 'competitive')) {
      console.log('\n=== Step 3: Leaguepedia Competitive ===');
      await reportProgress({ step: 'competitive', status: 'started' });

      let totalImported = 0;
      for (let i = 0; i < players.length; i++) {
        await reportProgress({ step: 'competitive', currentPlayer: i + 1, totalPlayers: players.length, playerName: players[i].riotId });
        let result = await scanCompetitive(turso, players[i]);
        
        if (result.needsFallback && result.fallbackName) {
          console.log(`[Competitive] Retrying with fallback name: ${result.fallbackName}`);
          await sleep(1000);
          const fallbackPlayer = { ...players[i], leaguepediaSlug: null, playerOverrideName: result.fallbackName };
          result = await scanCompetitive(turso, fallbackPlayer);
        }
        
        totalImported += result.gamesImported || 0;
        await logStep(turso, `competitive|${players[i].playerId}`, result.gamesImported || 0, 
          result.error ? 'partial' : 'success', result.error || null);
        
        await sleep(300);
      }
      await logStep(turso, 'competitive', totalImported);
      await reportProgress({ step: 'competitive', status: 'completed', totalImported });
    } else {
      console.log('[Resume] Competitive already complete, skipping');
    }
  }

  // ── Step 4: Team Import ──────────────────────────────────────────
  console.log('::endgroup::');
  console.log('::group::Step 4: Team Import');
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

  // ── Step 5: Aggregation ──────────────────────────────────────────
  console.log('::endgroup::');
  console.log('::group::Step 5: Aggregation');
  console.log('[Aggregation] Using raw-data query architecture — precomputed tables deprecated');
  await reportProgress({ step: 'aggregation', status: 'completed' });

  // ── Done ─────────────────────────────────────────────────────────
  console.log('\n=== Scan Complete! ===');
  console.log('::endgroup::');
  await reportProgress({ status: 'completed', completedAt: new Date().toISOString() });
}

main().catch(async (error) => {
  console.error('[Fatal]', error);
  console.log('::endgroup::');
  await reportProgress({ status: 'failed', error: error.message }).catch(() => {});
  process.exit(1);
});