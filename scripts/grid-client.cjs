/**
 * grid-client.cjs
 *
 * Shared Grid.gg API client for the fearless-scout-worker.
 * Extracted from netlify/functions/gridDataSync/index.js so the worker
 * can fetch series, draft data, and Riot end-state files from Grid.
 *
 * Environment variable required:
 *   GRID_API_KEY — Grid.gg API key
 *
 * Usage:
 *   const grid = require('./grid-client.cjs');
 *   await grid.initialize(mapper); // pass a ChampionNameMapper instance
 *   const series = await grid.fetchSeriesByTeamId('grid_team_id_here');
 */

const GRID_API_KEY = process.env.GRID_API_KEY || '';
const GRID_BASE_URL = 'https://api.grid.gg';
const LOL_TITLE_ID = 3;
const MAX_RETRIES = 5;
const FILE_DOWNLOAD_DELAY_MS = 1500;

/** @type {import('./champion-name-mapper.cjs').ChampionNameMapper|null} */
let championNameMapper = null;

/**
 * Initialize the Grid client with a champion name mapper instance.
 * Must be called before fetch/parse functions.
 */
function initialize(mapper) {
  championNameMapper = mapper;
}

function normalizeChampionName(name) {
  if (!name) return name;
  if (championNameMapper) {
    return championNameMapper.toChampionId(name);
  }
  return name.trim();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Generic GraphQL query to any Grid endpoint.
 */
async function gridGraphQL(query, endpoint = 'central-data/graphql') {
  if (!GRID_API_KEY) {
    throw new Error('[grid-client] GRID_API_KEY not set');
  }

  const url = `${GRID_BASE_URL}/${endpoint}`;
  const headers = {
    'x-api-key': GRID_API_KEY,
    'Content-Type': 'application/json',
  };

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify({ query }),
      });

      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After') || '3', 10);
        console.log(`[grid-client] Rate limited, sleeping ${retryAfter}s...`);
        await sleep(retryAfter * 1000);
        continue;
      }

      if (!response.ok) {
        throw new Error(`Grid API error ${response.status}: ${await response.text()}`);
      }

      const json = await response.json();
      if (json.errors) {
        throw new Error(`GraphQL errors: ${JSON.stringify(json.errors)}`);
      }
      return json.data;
    } catch (error) {
      if (attempt === MAX_RETRIES - 1) throw error;
      console.log(`[grid-client] Retry ${attempt + 1}/${MAX_RETRIES}: ${error.message}`);
      await sleep(2000);
    }
  }
}

/**
 * Fetch series where the given team IDs participated.
 * @param {string|string[]} teamIds — one or more Grid team IDs
 * @param {object} options
 * @param {string} [options.startTimeGte] — ISO date string
 * @param {string} [options.startTimeLte] — ISO date string
 * @param {number} [options.limit=50] — max series to return
 * @returns {Promise<Array>} array of series node objects
 */
async function fetchSeriesByTeamId(teamIds, options = {}) {
  const ids = Array.isArray(teamIds) ? teamIds : [teamIds];
  const idList = ids.map(id => `"${id}"`).join(', ');
  const limit = options.limit || 50;

  let timeFilter = '';
  if (options.startTimeGte || options.startTimeLte) {
    const parts = [];
    if (options.startTimeGte) parts.push(`gte: "${options.startTimeGte}"`);
    if (options.startTimeLte) parts.push(`lte: "${options.startTimeLte}"`);
    timeFilter = `startTimeScheduled: { ${parts.join(', ')} }`;
  }

  const query = `{
    allSeries(
      first: ${limit}
      filter: {
        titleId: ${LOL_TITLE_ID}
        types: ESPORTS
        teamIds: { in: [${idList}] }
        ${timeFilter ? `, ${timeFilter}` : ''}
      }
      orderBy: StartTimeScheduled
      orderDirection: DESC
    ) {
      totalCount
      pageInfo { hasNextPage endCursor }
      edges {
        node {
          id
          tournament { id name nameShortened }
          startTimeScheduled
          format { name nameShortened }
          teams { baseInfo { id name nameShortened } }
        }
      }
    }
  }`;

  const data = await gridGraphQL(query);
  return data?.allSeries?.edges?.map(e => e.node) || [];
}

/**
 * Fetch draft data for a series — returns games with draftActions, teams.
 */
async function fetchSeriesDraftData(seriesId) {
  const query = `{
    seriesState(id: "${seriesId}") {
      id
      games {
        id
        sequenceNumber
        started
        finished
        draftActions {
          type
          sequenceNumber
          drafter { id type }
          draftable { id name type }
        }
        teams { id name side won }
      }
    }
  }`;

  const data = await gridGraphQL(query, 'live-data-feed/series-state/graphql');
  return data?.seriesState || null;
}

/**
 * Download a Riot end-state file (summary or details) from Grid.
 */
async function downloadRiotFile(seriesId, gameNumber, fileType) {
  let endpoint;
  if (fileType === 'summary') {
    endpoint = `file-download/end-state/riot/series/${seriesId}/games/${gameNumber}/summary`;
  } else if (fileType === 'details') {
    endpoint = `file-download/end-state/riot/series/${seriesId}/games/${gameNumber}/details`;
  } else {
    throw new Error(`Unknown file type: ${fileType}`);
  }

  const url = `${GRID_BASE_URL}/${endpoint}`;
  const headers = { 'x-api-key': GRID_API_KEY };

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(url, { headers });

      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After') || '3', 10);
        console.log(`[grid-client] File download rate limited, sleeping ${retryAfter}s...`);
        await sleep(retryAfter * 1000);
        continue;
      }

      if (response.status === 404) {
        console.log(`[grid-client] File not available: ${fileType} for series ${seriesId} game ${gameNumber}`);
        return null;
      }

      if (response.status === 403) {
        console.log(`[grid-client] Access forbidden for ${fileType} series ${seriesId} game ${gameNumber}`);
        return null;
      }

      if (!response.ok) {
        throw new Error(`File download error ${response.status}: ${await response.text()}`);
      }

      const text = await response.text();
      try {
        return JSON.parse(text);
      } catch (e) {
        console.warn(`[grid-client] Could not parse JSON from ${fileType} for series ${seriesId} game ${gameNumber}: ${e.message}`);
        return null;
      }
    } catch (error) {
      if (attempt === MAX_RETRIES - 1) {
        console.error(`[grid-client] Failed to download ${fileType} for series ${seriesId} game ${gameNumber}: ${error.message}`);
        return null;
      }
      console.log(`[grid-client] File download retry ${attempt + 1}/${MAX_RETRIES}: ${error.message}`);
      await sleep(2000);
    }
  }
  return null;
}

/**
 * Parse a Riot summary file to extract participant data.
 * Returns array of participant objects with champion, role, stats.
 */
function parseRiotSummaryFile(summaryData) {
  if (!summaryData || !summaryData.participants) return [];

  const participants = [];
  for (const p of summaryData.participants) {
    // positionAssignedByMatchmaking is the authoritative role for esports matches
    const role = normalizeRole(p.positionAssignedByMatchmaking);
    if (!role) continue;

    participants.push({
      participantId: p.participantId,
      side: p.teamId === 100 ? 'blue' : 'red',
      championName: normalizeChampionName(p.championName),
      playerName: p.riotIdGameName || p.summonerName || null,
      role,
      kills: p.kills || 0,
      deaths: p.deaths || 0,
      assists: p.assists || 0,
      goldEarned: p.goldEarned || 0,
      cs: (p.totalMinionsKilled || 0) + (p.neutralMinionsKilled || 0),
      win: p.win ? 1 : 0,
    });
  }

  return participants;
}

/**
 * Parse a Riot details (timeline) file to extract gold and CS at ~15 minutes.
 * Returns a Map of participantId -> { goldAt15, csAt15 }
 */
function parseRiotTimelineAt15(timelineData) {
  const result = new Map();
  if (!timelineData || !timelineData.frames) return result;

  const targetTime = 900000; // 15 minutes in ms
  let bestFrame = null;
  let bestDiff = Infinity;

  for (const frame of timelineData.frames) {
    const diff = Math.abs(frame.timestamp - targetTime);
    if (diff < bestDiff) {
      bestDiff = diff;
      bestFrame = frame;
    }
    if (frame.timestamp > targetTime) break;
  }

  if (!bestFrame || !bestFrame.participantFrames) return result;

  for (const [pid, frameData] of Object.entries(bestFrame.participantFrames)) {
    const participantId = parseInt(pid);
    result.set(participantId, {
      goldAt15: frameData.totalGold || 0,
      csAt15: (frameData.minionsKilled || 0) + (frameData.jungleMinionsKilled || 0),
    });
  }

  return result;
}

/**
 * Normalize Riot role names to short form.
 */
function normalizeRole(role) {
  if (!role) return null;
  const upper = role.toUpperCase();
  switch (upper) {
    case 'TOP': return 'top';
    case 'JUNGLE': return 'jungle';
    case 'MIDDLE': return 'mid';
    case 'BOTTOM': return 'bot';
    case 'UTILITY': return 'support';
    case 'SUPPORT': return 'support';
    default: return role.toLowerCase();
  }
}

/**
 * Parse draft actions into structured pick/ban events.
 */
function parseDraftActions(games) {
  const results = [];

  for (const game of games) {
    if (!game.draftActions || game.draftActions.length === 0) continue;

    const sortedActions = [...game.draftActions].sort(
      (a, b) => parseInt(a.sequenceNumber) - parseInt(b.sequenceNumber)
    );

    const pickCounters = { blue: 0, red: 0 };
    const banCounters = { blue: 0, red: 0 };

    const teamSides = {};
    for (const team of game.teams || []) {
      if (team.side) {
        teamSides[team.id] = team.side.toLowerCase();
      }
    }

    for (const action of sortedActions) {
      const actionType = action.type.toLowerCase();
      const championName = normalizeChampionName(action.draftable?.name);
      if (!championName) continue;

      let side = teamSides[action.drafter?.id];
      if (!side) {
        console.warn(`[grid-client] Could not determine side for action in game ${game.id}, drafter: ${action.drafter?.id}`);
        continue;
      }

      if (actionType === 'pick') {
        pickCounters[side]++;
        results.push({
          game_sequence: game.sequenceNumber,
          side,
          action_type: 'pick',
          champion_name: championName,
          pick_position: pickCounters[side],
          ban_position: null,
          sequence_number: action.sequenceNumber,
        });
      } else if (actionType === 'ban') {
        banCounters[side]++;
        results.push({
          game_sequence: game.sequenceNumber,
          side,
          action_type: 'ban',
          champion_name: championName,
          pick_position: null,
          ban_position: banCounters[side],
          sequence_number: action.sequenceNumber,
        });
      }
    }
  }

  return results;
}

module.exports = {
  initialize,
  gridGraphQL,
  fetchSeriesByTeamId,
  fetchSeriesDraftData,
  downloadRiotFile,
  parseRiotSummaryFile,
  parseRiotTimelineAt15,
  parseDraftActions,
  normalizeRole,
};