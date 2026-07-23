/**
 * firebase-admin.js
 *
 * Firebase Admin SDK initialization for the fearless-scout-worker.
 * Reads FIREBASE_SERVICE_ACCOUNT_JSON from environment and returns a
 * Firestore client for writing progress updates to scoutJobs/{jobId}.
 *
 * Usage:
 *   const { getFirestore } = require('./firebase-admin');
 *   const db = getFirestore();
 *   await db.collection('scoutJobs').doc(jobId).set({ ... });
 */

let firestoreInstance = null;

/**
 * Initialize and return a Firestore client.
 * The FIREBASE_SERVICE_ACCOUNT_JSON env var must contain the full
 * service account key JSON (as a JSON string).
 *
 * @returns {FirebaseFirestore.Firestore}
 */
function getFirestore() {
  if (firestoreInstance) return firestoreInstance;

  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (!serviceAccountJson) {
    throw new Error(
      'FIREBASE_SERVICE_ACCOUNT_JSON environment variable is not set. ' +
      'This is required for Firebase Admin SDK initialization.'
    );
  }

  let serviceAccount;
  try {
    serviceAccount = JSON.parse(serviceAccountJson);
  } catch (err) {
    throw new Error(
      `Failed to parse FIREBASE_SERVICE_ACCOUNT_JSON: ${err.message}`
    );
  }

  if (!serviceAccount.project_id) {
    throw new Error(
      'FIREBASE_SERVICE_ACCOUNT_JSON is missing project_id. ' +
      'Please ensure the service account key is valid.'
    );
  }

  const admin = require('firebase-admin');

  if (!admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  }

  firestoreInstance = admin.firestore();
  return firestoreInstance;
}

/**
 * Get a reference to the admin Firebase instance (useful for serverTimestamp).
 */
function getAdmin() {
  return require('firebase-admin');
}

module.exports = { getFirestore, getAdmin };