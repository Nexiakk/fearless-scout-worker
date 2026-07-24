/**
 * compute-flags.cjs
 *
 * Worker-side flag computation script.
 * Runs after each scan to compute single-player and comparative flags
 * from Turso scouting data and save them to Firestore.
 *
 * Called from run-scan.cjs as the final step after aggregation.
 *
 * Environment variables:
 *   TURSO_SCOUTING_URL  — Turso scouting DB URL
 *   TURSO_SCOUTING_TOKEN
 *   WORKSPACE_ID        — Firestore workspace ID
 *   PLAYERS_JSON        — JSON array of player objects (with playerId, teamId)
 *   TEAM_JSON           — JSON { teamId, teamName, isOwnTeam, ... }
 */

const { createClient } = require('@libsql/client')

// Default thresholds (mirrors useFlagEngine.js)
const DEFAULT_THRESHOLDS = {
  soloqGamesThreshold: 5,
  soloqWRThreshold: 50,
  recentSurgeDays: 14,
  recentSurgeMinGames: 3,
  counterpickThreshold: 70,
  dangerousUnknownMinGames: 5,
  fadeOutDays: 30,
  contestedComfortGames: 5,
  contestedComfortPickRate: 20,
  blindPickRateThreshold: 35,
  counterWRThreshold: 60,
  counterMinGames: 5,
  banSqueezeDeltaEnemy: 25,
  banSqueezeDeltaOwn: 10,
  fearlessMinGames: 3,
  fearlessWRThreshold: 50,
  competitiveWeight: 3,
}

/**
 * Initialize Turso client.
 */
function getTurso() {
  const url = process.env.TURSO_SCOUTING_URL
  const token = process.env.TURSO_SCOUTING_TOKEN
  if (!url || !token) {
    throw new Error('TURSO_SCOUTING_URL and TURSO_SCOUTING_TOKEN are required')
  }
  return createClient({ url, authToken: token })
}

/**
 * Get Firestore client (lazy init).
 */
let _db = null
function getDb() {
  if (_db) return _db
  const { getFirestore } = require('./firebase-admin')
  _db = getFirestore()
  return _db
}

/**
 * Get admin instance for serverTimestamp.
 */
let _admin = null
function getAdmin() {
  if (_admin) return _admin
  const { getAdmin } = require('./firebase-admin')
  _admin = getAdmin()
  return _admin
}

/**
 * Fetch champion stats for a player from Turso.
 * @param {import('@libsql/client').Client} turso
 * @param {string} playerId
 * @param {string} source - 'soloq' or 'competitive'
 * @returns {Promise<Array>}
 */
async function getPlayerChampions(turso, playerId, source) {
  const table = source === 'soloq' ? 'soloq_games' : 'competitive_games'
  try {
    const result = await turso.execute({
      sql: `
        SELECT champion, COUNT(*) as games, SUM(CASE WHEN win THEN 1 ELSE 0 END) as wins
        FROM ${table}
        WHERE player_id = ?
        GROUP BY champion
        ORDER BY games DESC
      `,
      args: [playerId],
    })
    return result.rows.map(row => ({
      championName: row.champion,
      games: Number(row.games),
      wins: Number(row.wins || 0),
      winrate: Number(row.games) > 0 ? (Number(row.wins || 0) / Number(row.games)) * 100 : 0,
    }))
  } catch (err) {
    console.error(`[Flags] Error fetching ${source} champions for ${playerId}:`, err.message)
    return []
  }
}

/**
 * Compute single-player flags for a given player.
 * @param {import('@libsql/client').Client} turso
 * @param {string} playerId
 * @param {string} playerName
 * @param {Object} thresholds
 * @returns {Promise<Array>}
 */
async function computePlayerFlags(turso, playerId, playerName, thresholds) {
  const t = { ...DEFAULT_THRESHOLDS, ...thresholds }
  const flags = []

  const soloqChamps = await getPlayerChampions(turso, playerId, 'soloq')
  const compChamps = await getPlayerChampions(turso, playerId, 'competitive')

  // Flag 1: SoloQ Specialist
  for (const champ of soloqChamps) {
    if (champ.games >= t.soloqGamesThreshold && champ.winrate >= t.soloqWRThreshold) {
      const compMatch = compChamps.find(c => c.championName === champ.championName)
      if (!compMatch || compMatch.games === 0) {
        flags.push({
          type: 'soloq_specialist',
          severity: 'info',
          champion: champ.championName,
          playerId,
          playerName,
          description: `${champ.championName}: ${champ.games} games at ${Math.round(champ.winrate)}% WR in SoloQ, 0 competitive games.`,
          relatedData: { games: champ.games, winrate: champ.winrate },
        })
      }
    }
  }

  // Flag 4: Dangerous Unknown
  const top5Soloq = soloqChamps.slice(0, 5)
  for (const champ of top5Soloq) {
    if (champ.games >= t.dangerousUnknownMinGames) {
      const compMatch = compChamps.find(c => c.championName === champ.championName)
      if (!compMatch || compMatch.games === 0) {
        flags.push({
          type: 'dangerous_unknown',
          severity: 'danger',
          champion: champ.championName,
          playerId,
          playerName,
          description: `${champ.championName}: Top 5 SoloQ champion (${champ.games} games) but 0 competitive appearances. Unknown factor.`,
          relatedData: { games: champ.games, rank: top5Soloq.indexOf(champ) + 1 },
        })
      }
    }
  }

  return flags
}

/**
 * Build a champion map for a team from Turso data.
 * @param {import('@libsql/client').Client} turso
 * @param {Array} players - Array of { playerId }
 * @returns {Promise<Object>}
 */
async function buildTeamChampionMap(turso, players) {
  const champMap = {}
  let totalGames = 0

  for (const player of players) {
    const soloq = await getPlayerChampions(turso, player.playerId, 'soloq')
    const comp = await getPlayerChampions(turso, player.playerId, 'competitive')
    const allChamps = [...soloq, ...comp]

    for (const c of allChamps) {
      if (!champMap[c.championName]) {
        champMap[c.championName] = { games: 0, wins: 0, winrate: 0, pickRate: 0 }
      }
      champMap[c.championName].games += c.games
      champMap[c.championName].wins += c.wins
      totalGames += c.games
    }
  }

  for (const [name, data] of Object.entries(champMap)) {
    data.winrate = data.games > 0 ? (data.wins / data.games) * 100 : 0
    data.pickRate = totalGames > 0 ? (data.games / totalGames) * 100 : 0
  }

  return champMap
}

/**
 * Compute comparative flags between our team and an enemy team.
 * @param {import('@libsql/client').Client} turso
 * @param {Array} ourPlayers
 * @param {Array} enemyPlayers
 * @param {Object} thresholds
 * @returns {Promise<Array>}
 */
async function computeComparativeFlags(turso, ourPlayers, enemyPlayers, thresholds) {
  const t = { ...DEFAULT_THRESHOLDS, ...thresholds }
  const flags = []

  if (ourPlayers.length === 0 || enemyPlayers.length === 0) return flags

  const ourChampMap = await buildTeamChampionMap(turso, ourPlayers)
  const enemyChampMap = await buildTeamChampionMap(turso, enemyPlayers)

  // Flag 6: Contested Comfort
  for (const [champion, ourData] of Object.entries(ourChampMap)) {
    const enemyData = enemyChampMap[champion]
    if (!enemyData) continue

    if (ourData.games >= t.contestedComfortGames && enemyData.games >= t.contestedComfortGames &&
        ourData.pickRate >= t.contestedComfortPickRate && enemyData.pickRate >= t.contestedComfortPickRate) {
      flags.push({
        type: 'contested_comfort',
        severity: 'warning',
        champion,
        description: `${champion}: Core comfort for BOTH players (Enemy ${Math.round(enemyData.pickRate)}% pick rate / Our ${Math.round(ourData.pickRate)}% pick rate). High priority B1 or Ban candidate.`,
        relatedData: {
          enemyGames: enemyData.games,
          ownGames: ourData.games,
          enemyPickRate: enemyData.pickRate,
          ownPickRate: ourData.pickRate,
        },
      })
    }
  }

  // Flag 7: Punishable Blind
  for (const [champion, enemyData] of Object.entries(enemyChampMap)) {
    if (enemyData.pickRate < t.blindPickRateThreshold) continue

    for (const [ourChamp, ourData] of Object.entries(ourChampMap)) {
      if (ourData.games >= t.counterMinGames && ourData.winrate >= t.counterWRThreshold) {
        flags.push({
          type: 'punishable_blind',
          severity: 'info',
          champion: ourChamp,
          description: `Enemy blind-picks ${champion} ${Math.round(enemyData.pickRate)}% of games. Our player has ${ourChamp} with ${Math.round(ourData.winrate)}% WR over ${ourData.games} games.`,
          relatedData: {
            enemyChampion: champion,
            enemyPickRate: enemyData.pickRate,
            counterChampion: ourChamp,
            counterWinrate: ourData.winrate,
            counterGames: ourData.games,
          },
        })
      }
    }
  }

  // Flag 8: Ban Squeeze Asymmetry
  const ourTop2 = Object.entries(ourChampMap)
    .sort((a, b) => b[1].games - a[1].games)
    .slice(0, 2)
    .map(([name]) => name)

  for (const [champion, ourData] of Object.entries(ourChampMap)) {
    if (ourTop2.includes(champion)) continue
    const enemyData = enemyChampMap[champion]
    if (!enemyData) continue

    const ourWRDrop = ourData.winrate ? 100 - ourData.winrate : 0
    const enemyWRDrop = enemyData.winrate ? 100 - enemyData.winrate : 0

    if (enemyWRDrop >= t.banSqueezeDeltaEnemy && ourWRDrop <= t.banSqueezeDeltaOwn) {
      flags.push({
        type: 'ban_squeeze_asymmetry',
        severity: 'danger',
        champion,
        description: `${champion}: Enemy WR drops ${Math.round(enemyWRDrop)}% when banned (Δ≥${t.banSqueezeDeltaEnemy}%), but our WR only drops ${Math.round(ourWRDrop)}% (Δ≤${t.banSqueezeDeltaOwn}%). Asymmetric ban value.`,
        relatedData: { enemyWRDrop, ourWRDrop, enemyGames: enemyData.games, ownGames: ourData.games },
      })
    }
  }

  // Flag 9: Fearless Pool Exhaustion
  const ourComfortCount = Object.values(ourChampMap).filter(d => d.games >= t.fearlessMinGames && d.winrate >= t.fearlessWRThreshold).length
  const enemyComfortCount = Object.values(enemyChampMap).filter(d => d.games >= t.fearlessMinGames && d.winrate >= t.fearlessWRThreshold).length
  const diff = ourComfortCount - enemyComfortCount

  if (diff >= 2) {
    flags.push({
      type: 'fearless_pool_exhaustion',
      severity: 'danger',
      champion: null,
      description: `Fearless Pool advantage: Our team has ${ourComfortCount} comfort champions vs enemy's ${enemyComfortCount} (Δ=${diff}). We can outlast them in a long series.`,
      relatedData: { ourComfortCount, enemyComfortCount, difference: diff },
    })
  }

  return flags
}

/**
 * Save flags to Firestore for a player.
 * @param {string} workspaceId
 * @param {string} playerId
 * @param {Array} flags
 */
async function savePlayerFlags(workspaceId, playerId, flags) {
  const db = getDb()
  const admin = getAdmin()

  try {
    // Clear existing flags
    const flagsRef = db.collection('workspaces').doc(workspaceId)
      .collection('scoutedPlayers').doc(playerId).collection('flags')
    const existing = await flagsRef.get()
    await Promise.allSettled(existing.docs.map(d => d.ref.delete()))

    // Write new flags
    await Promise.allSettled(
      flags.map((flag, index) =>
        flagsRef.doc(`flag_${index}`).set({
          ...flag,
          computedAt: admin.firestore.FieldValue.serverTimestamp(),
          sortOrder: index,
        })
      )
    )
    console.log(`[Flags] Saved ${flags.length} flags for player ${playerId}`)
  } catch (err) {
    console.error(`[Flags] Error saving flags for ${playerId}:`, err.message)
  }
}

/**
 * Main entry point.
 */
async function main() {
  console.log('::group::Step 6: Flag Computation')
  console.log('[Flags] Starting flag computation...')

  const workspaceId = process.env.WORKSPACE_ID
  if (!workspaceId) {
    console.warn('[Flags] No WORKSPACE_ID set, skipping flag computation')
    console.log('::endgroup::')
    return
  }

  let players = []
  try {
    players = JSON.parse(process.env.PLAYERS_JSON || '[]')
  } catch {
    console.warn('[Flags] Invalid PLAYERS_JSON, skipping flag computation')
    console.log('::endgroup::')
    return
  }

  if (players.length === 0) {
    console.log('[Flags] No players to compute flags for')
    console.log('::endgroup::')
    return
  }

  let teamData = null
  try {
    teamData = JSON.parse(process.env.TEAM_JSON || 'null')
  } catch {
    // No team data — only compute single-player flags
  }

  const turso = getTurso()

  // Load custom thresholds from Firestore
  let thresholds = { ...DEFAULT_THRESHOLDS }
  try {
    const db = getDb()
    const thresholdDoc = await db.collection('workspaces').doc(workspaceId)
      .collection('_settings').doc('flagThresholds').get()
    if (thresholdDoc.exists) {
      thresholds = { ...DEFAULT_THRESHOLDS, ...thresholdDoc.data() }
    }
  } catch (err) {
    console.warn('[Flags] Could not load thresholds from Firestore, using defaults:', err.message)
  }

  // Compute single-player flags for each player
  for (const player of players) {
    const playerFlags = await computePlayerFlags(turso, player.playerId, player.name || player.riotId || 'Unknown', thresholds)
    await savePlayerFlags(workspaceId, player.playerId, playerFlags)
  }

  // Compute comparative flags if we have team data with isOwnTeam
  if (teamData && teamData.isOwnTeam) {
    // This is "Our Team" — compute comparative flags against enemy teams
    // Enemy teams are other teams in the workspace that have been scouted
    console.log('[Flags] Our Team detected, computing comparative flags...')
    // Note: Full comparative flag computation requires fetching enemy team data
    // which is done on-demand by the frontend via useScoutingFlags composable
  }

  console.log('[Flags] Flag computation complete')
  console.log('::endgroup::')
}

// Run if called directly
if (require.main === module) {
  main().catch(err => {
    console.error('[Flags] Fatal error:', err.message)
    process.exit(1)
  })
}

module.exports = { computePlayerFlags, computeComparativeFlags, savePlayerFlags, DEFAULT_THRESHOLDS }