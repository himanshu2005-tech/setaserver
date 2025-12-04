const express = require('express');
const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore, FieldValue } = require('firebase-admin/firestore');
const admin = require('firebase-admin');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const cors = require('cors'); 
require("dotenv").config();

// Initialize Express
const app = express();
const PORT = process.env.PORT || 5000;

// Enable CORS
app.use(cors());

// Initialize Firebase
const serviceAccount = {
  type: process.env.TYPE,
  project_id: process.env.PROJECT_ID,
  private_key_id: process.env.PRIVATE_KEY_ID,
  // Handle newline characters for Railway/Heroku env vars
  private_key: process.env.PRIVATE_KEY ? process.env.PRIVATE_KEY.replace(/\\n/g, '\n') : undefined,
  client_email: process.env.CLIENT_EMAIL,
  client_id: process.env.CLIENT_ID,
  auth_uri: process.env.AUTH_URI,
  token_uri: process.env.TOKEN_URI,
  auth_provider_x509_cert_url: process.env.AUTH_PROVIDER_CERT_URL,
  client_x509_cert_url: process.env.CLIENT_CERT_URL,
  universe_domain: process.env.UNIVERSE_DOMAIN
};

initializeApp({
  credential: cert(serviceAccount)
});

const db = getFirestore();

// --- Middleware ---

const validatePath = (req, res, next) => {
  if (req.query.savePath) {
    try {
      if (!fs.existsSync(req.query.savePath)) {
        fs.mkdirSync(req.query.savePath, { recursive: true });
      }
      next();
    } catch (error) {
      res.status(400).json({ error: 'Invalid save path' });
    }
  } else {
    next();
  }
};

// --- Helpers ---

/**
 * Authorization Logic:
 * 1. If dataset does not exist -> Deny.
 * 2. If visibility is 'Public' -> Allow.
 * 3. If visibility is 'Private' -> Check if userId is in 'access_users'.
 */
function checkDatasetAccess(datasetDoc, userId) {
  if (!datasetDoc.exists) return false;
  
  const data = datasetDoc.data();

  // 1. Allow if explicitly Public
  if (data.visibility === "Public" || data.isPublic === true) {
    return true;
  }

  // 2. Check Private Access via access_users array
  // We check this regardless of whether visibility is explicitly set to 'Private' 
  // to catch cases where visibility might be undefined but access_users exists.
  if (data.access_users && Array.isArray(data.access_users)) {
    if (data.access_users.includes(userId)) {
      return true;
    }
  }

  // 3. Fallback / Default Deny
  return false;
}

async function updateRequestCount(userId, datasetId, version) {
  try {
    const currentTime = new Date().toISOString();
    const versionField = `v${version.replace(/\./g, '_')}`;
    
    const userRequestRef = db.collection("Users")
      .doc(userId)
      .collection("requests")
      .doc(datasetId);

    await db.runTransaction(async (transaction) => {
      const doc = await transaction.get(userRequestRef);
      
      if (!doc.exists) {
        transaction.set(userRequestRef, {
          versions: { [versionField]: [currentTime] },
          lastUpdated: currentTime
        });
      } else {
        const updates = {
          lastUpdated: currentTime,
          [`versions.${versionField}`]: FieldValue.arrayUnion(currentTime)
        };
        transaction.update(userRequestRef, updates);
      }
    });

    const datasetVersionRef = db.collection("datasets")
      .doc(datasetId)
      .collection("versions")
      .doc(version);
    const rootDatasetRef = db.collection("datasets").doc(datasetId);
    
    const versionDoc = await datasetVersionRef.get();
    if(versionDoc.exists) {
        await datasetVersionRef.update({ requestCount: FieldValue.increment(1) });
    } else {
        await datasetVersionRef.set({ requestCount: 1 }, { merge: true });
    }

    await rootDatasetRef.update({ requestCount: FieldValue.increment(1) });

    const userRequestMetaRef = datasetVersionRef
      .collection("requestedUsers")
      .doc(userId);

    await userRequestMetaRef.set({
      requestedCount: FieldValue.increment(1),
      requestedTime: FieldValue.arrayUnion(currentTime)
    }, { merge: true });

    console.log(`Stats updated: User ${userId}, Dataset ${datasetId}, Version ${version}`);
  } catch (error) {
    console.error("Error updating request count:", error.message);
  }
}

// --- API Endpoints ---

/**
 * 1. Get Recent Seta (Latest Version)
 */
app.get('/getRecentSeta', async (req, res) => {
  const { id, userId } = req.query;

  if (!id || !userId) return res.status(400).json({ error: 'Missing id or userId parameter' });

  try {
    // 1. Check Root Access
    const datasetRef = db.collection("datasets").doc(id);
    const datasetDoc = await datasetRef.get();
    
    // STRICT AUTH CHECK
    if (!checkDatasetAccess(datasetDoc, userId)) {
      return res.status(403).json({ error: 'Not authorized' });
    }

    // 2. Find Latest Enabled Version
    const versionsRef = datasetRef.collection("versions");
    
    const querySnapshot = await versionsRef
      .orderBy("publishedOn", "desc")
      .limit(5)
      .get();

    if (querySnapshot.empty) return res.status(404).json({ error: 'No active versions found for this dataset' });

    const latestVersionDoc = querySnapshot.docs.find(doc => doc.data().isDisabled !== true);

    if (!latestVersionDoc) return res.status(404).json({ error: 'No active/enabled versions available' });

    const latestVersionData = latestVersionDoc.data();
    const versionId = latestVersionDoc.id;

    // 3. Update Stats
    updateRequestCount(userId, id, versionId).catch(err => console.error("Stat update failed", err));

    // 4. Return Data
    res.status(200).json({ 
      version: versionId,
      publishedOn: latestVersionData.publishedOn,
      files: latestVersionData.files || [], 
      metadata: latestVersionData
    });

  } catch (error) {
    console.error("getRecentSeta Error:", error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * 2. Get Specific Version
 */
app.get('/getSetaByVersion', async (req, res) => {
  const { id, version, userId } = req.query;

  if (!id || !version || !userId) return res.status(400).json({ error: 'Missing id, version, or userId parameter' });

  try {
    const datasetRef = db.collection("datasets").doc(id);
    const datasetDoc = await datasetRef.get();
    
    // STRICT AUTH CHECK
    if (!checkDatasetAccess(datasetDoc, userId)) {
      return res.status(403).json({ error: 'Not authorized' });
    }

    const docRef = db.collection('datasets').doc(id).collection('versions').doc(version);
    const doc = await docRef.get();

    if (!doc.exists) return res.status(404).json({ error: 'Version not found' });
    if (doc.data().isDisabled) return res.status(403).json({ error: 'This version has been disabled' });

    updateRequestCount(userId, id, version).catch(err => console.error("Stat update failed", err));

    res.status(200).json({
        version: version,
        files: doc.data().files || [],
        metadata: doc.data()
    });

  } catch (error) {
    console.error("getSetaByVersion Error:", error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * 3. Get Instance
 */
app.get('/getSetaInstance', async (req, res) => {
  const { id, version, instanceId, userId } = req.query;

  if (!id || !version || !instanceId || !userId) return res.status(400).json({ error: 'Missing id, version, instanceId, or userId' });

  try {
    const datasetRef = db.collection("datasets").doc(id);
    const datasetDoc = await datasetRef.get();
    
    // STRICT AUTH CHECK
    if (!checkDatasetAccess(datasetDoc, userId)) {
      return res.status(403).json({ error: 'Not authorized' });
    }

    const instanceRef = db.collection('datasets')
        .doc(id)
        .collection('versions')
        .doc(version)
        .collection('instances')
        .doc(instanceId);

    const instanceDoc = await instanceRef.get();

    if (!instanceDoc.exists) return res.status(404).json({ error: 'Instance not found' });

    updateRequestCount(userId, id, version).catch(err => console.error("Stat update failed", err));

    const instanceData = instanceDoc.data();
    
    res.status(200).json({
        instanceId: instanceId,
        files: instanceData.files || [],
        savedAt: instanceData.savedAt,
        metadata: instanceData
    });

  } catch (error) {
    console.error("getSetaInstance Error:", error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * 4. PROXY Endpoint
 * Using axios here because fetch in Node requires polyfills or specific versions,
 * and axios streams are robust for file proxying.
 */
app.get('/proxy', async (req, res) => {
  const url = req.query.url;
  
  if (!url) return res.status(400).send('URL parameter is required');

  try {
    console.log(`Proxying request to: ${url}`);
    
    const response = await axios({
      method: 'get',
      url: url,
      responseType: 'stream'
    });
    
    if (response.headers['content-type']) res.setHeader('Content-Type', response.headers['content-type']);
    if (response.headers['content-length']) res.setHeader('Content-Length', response.headers['content-length']);
    if (response.headers['content-disposition']) res.setHeader('Content-Disposition', response.headers['content-disposition']);
    
    res.setHeader('Access-Control-Allow-Origin', '*');
    
    response.data.pipe(res);

  } catch (err) {
    console.error('Proxy fetch error:', err.message);
    if (!res.headersSent) {
      res.status(500).send('Proxy fetch error: ' + err.message);
    }
  }
});

/**
 * 5. Download Seta By Version (Local Save)
 */
app.get('/downloadSetaByVersion', validatePath, async (req, res) => {
  const { id, version, userId, savePath } = req.query;

  if (!id || !version || !userId || !savePath) return res.status(400).json({ error: 'Missing required parameters' });

  try {
    const datasetRef = db.collection("datasets").doc(id);
    const datasetDoc = await datasetRef.get();

    // STRICT AUTH CHECK
    if (!checkDatasetAccess(datasetDoc, userId)) {
      return res.status(403).json({ error: 'Not authorized' });
    }

    const doc = await db.collection('datasets').doc(id).collection('versions').doc(version).get();
    if (!doc.exists || doc.data().isDisabled) return res.status(404).json({ error: 'Not found or disabled' });

    const files = doc.data().files;
    let targetUrl = null;

    if (files && Array.isArray(files) && files.length > 0) {
        targetUrl = files[0].fileUrl; // Defaults to first file
    } else if (doc.data().fileUrls && doc.data().fileUrls.length > 0) {
        // Fallback for old schema
        targetUrl = doc.data().fileUrls[0];
    }

    if (!targetUrl) return res.status(404).json({ error: 'No valid file URL found in this version' });

    const fileName = `${id}_${version}_download.zip`; 
    const filePath = path.join(savePath, fileName);
    const writer = fs.createWriteStream(filePath);

    const response = await axios({
      url: targetUrl,
      method: 'GET',
      responseType: 'stream',
      timeout: 30000
    });

    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on('finish', async () => {
        await updateRequestCount(userId, id, version);
        res.status(200).json({ message: 'Downloaded successfully', path: filePath });
        resolve();
      });
      writer.on('error', (err) => {
        res.status(500).json({ error: 'Write error' });
        reject(err);
      });
    });

  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Server error during download' });
  }
});

app.get('/health', (req, res) => {
  res.status(200).send('Server is running');
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});