const express = require('express');
require('dotenv').config();
const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore, FieldValue } = require('firebase-admin/firestore');
const admin = require('firebase-admin');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const serviceAccount = JSON.parse(
  Buffer.from(process.env.FIREBASE_ADMIN_BASE64, 'base64').toString('utf-8')
);

initializeApp({
  credential: cert(serviceAccount)
});

const db = getFirestore();
const app = express();
const PORT = 5000;

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

async function updateRequestCount(userId, datasetId, version) {
  try {
    const currentTime = new Date().toISOString();
    const userRequestRef = db.collection("Users")
      .doc(userId)
      .collection("requests")
      .doc(datasetId)

    const userDoc = await userRequestRef.get();
    const versionField = `v${version.replace(/\./g, '_')}`;
    await db.runTransaction(async (transaction) => {
      const doc = await transaction.get(userRequestRef);
      
      if (!doc.exists) {
        transaction.set(userRequestRef, {
          versions: {
            [versionField]: [currentTime]
          },
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
    const datasetVersionDoc = await datasetVersionRef.get();

    if (!datasetVersionDoc.exists) {
      await datasetVersionRef.set({
        requestCount: 1
      });
      await rootDatasetRef.update({
        requestCount: 1
      }, { merge: true });
    } else {
      await datasetVersionRef.update({
        requestCount: FieldValue.increment(1)
      });
      await rootDatasetRef.update({
        requestCount: FieldValue.increment(1)
      });
    }

    const userRequestMetaRef = datasetVersionRef
      .collection("requestedUsers")
      .doc(userId);

    const userRequestMetaDoc = await userRequestMetaRef.get();

    if (!userRequestMetaDoc.exists) {
      await userRequestMetaRef.set({
        requestedCount: 1,
        requestedTime: FieldValue.arrayUnion(currentTime)
      });
    } else {
      await userRequestMetaRef.update({
        requestedCount: FieldValue.increment(1),
        requestedTime: FieldValue.arrayUnion(currentTime)
      });
    }

    console.log(`Request count updated for user ${userId}, dataset ${datasetId}, version ${version}`);
  } catch (error) {
    console.error("Error updating request count:", error.message);
  }
}

function checkDatasetAccess(datasetDoc, userId) {
  if (!datasetDoc.exists) return false;
  const data = datasetDoc.data();
  if (data.isPublic) return true;
  if (!data.access_users || !Array.isArray(data.access_users)) return false;
  return data.access_users.includes(userId);
}

app.get('/getSetaByVersion', async (req, res) => {
  const { id, version, userId } = req.query;

  if (!id || !version || !userId) {
    return res.status(400).json({ error: 'Missing id, version, or userId parameter' });
  }

  try {
    const datasetRef = db.collection("datasets").doc(id);
    const datasetDoc = await datasetRef.get();
    
    if (!checkDatasetAccess(datasetDoc, userId)) {
      return res.status(403).json({ error: 'No access to this dataset' });
    }

    const docRef = db.collection('datasets').doc(id).collection('versions').doc(version);
    const doc = await docRef.get();

    if (!doc.exists) {
      return res.status(404).json({ error: 'Version not found' });
    }
    if (doc.data().isDisabled) {
      return res.status(403).json({ error: 'Document disabled' });
    }

    await updateRequestCount(userId, id, version);
    res.status(200).json({ url: doc.data().fileUrl });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/getRecentSeta', async (req, res) => {
  const { id, userId } = req.query;

  if (!id || !userId) {
    return res.status(400).json({ error: 'Missing id or userId parameter' });
  }

  try {
    const datasetRef = db.collection("datasets").doc(id);
    const datasetDoc = await datasetRef.get();
    
    if (!checkDatasetAccess(datasetDoc, userId)) {
      return res.status(403).json({ error: 'No access to this dataset' });
    }

    const docRef = db.collection('datasets').doc(id);
    const doc = await docRef.get();

    if (!doc.exists) {
      return res.status(404).json({ error: 'Dataset ID not found' });
    }

    const latestVersionRef = db.collection('datasets').doc(id).collection('versions').doc(doc.data().latestVersion);
    const latestVersion = await latestVersionRef.get();

    if (!latestVersion.exists) {
      return res.status(404).json({ error: 'Version not found' });
    }
    if (latestVersion.data().isDisabled) {
      return res.status(403).json({ error: 'Document disabled' });
    }

    await updateRequestCount(userId, id, doc.data().latestVersion);
    res.status(200).json({ url: latestVersion.data().fileUrl });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/downloadSetaByVersion', validatePath, async (req, res) => {
  const { id, version, userId, savePath } = req.query;

  if (!id || !version || !userId || !savePath) {
    return res.status(400).json({ error: 'Missing required parameters' });
  }

  try {
    const datasetRef = db.collection("datasets").doc(id);
    const datasetDoc = await datasetRef.get();
    
    if (!checkDatasetAccess(datasetDoc, userId)) {
      return res.status(403).json({ error: 'No access to this dataset' });
    }

    const docRef = db.collection('datasets').doc(id).collection('versions').doc(version);
    const doc = await docRef.get();

    if (!doc.exists) {
      return res.status(404).json({ error: 'Document not found' });
    }
    if (doc.data().isDisabled) {
      return res.status(403).json({ error: 'Document disabled' });
    }

    const fileUrl = doc.data().fileUrl;
    if (!fileUrl) {
      return res.status(404).json({ error: 'File URL not found' });
    }

    const fileName = `${id}_${version}.zip`;
    const filePath = path.join(savePath, fileName);

    const writer = fs.createWriteStream(filePath);
    const response = await axios({
      url: fileUrl,
      method: 'GET',
      responseType: 'stream',
      timeout: 30000
    });

    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on('finish', async () => {
        try {
          await updateRequestCount(userId, id, version);
          res.status(200).json({ message: 'File downloaded successfully', path: filePath });
          resolve();
        } catch (error) {
          console.error(error);
          res.status(500).json({ error: 'Error updating request count' });
          reject(error);
        }
      });

      writer.on('error', (error) => {
        console.error('File write error:', error);
        res.status(500).json({ error: 'Error writing the file' });
        reject(error);
      });
    });
  } catch (error) {
    console.error('Download error:', error);
    if (error.response) {
      return res.status(502).json({ error: 'Failed to download file from storage' });
    }
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/downloadRecentSeta', validatePath, async (req, res) => {
  const { id, userId, savePath } = req.query;

  if (!id || !userId || !savePath) {
    return res.status(400).json({ error: 'Missing required parameters' });
  }

  try {
    const datasetRef = db.collection("datasets").doc(id);
    const datasetDoc = await datasetRef.get();
    
    if (!checkDatasetAccess(datasetDoc, userId)) {
      return res.status(403).json({ error: 'No access to this dataset' });
    }

    const docRef = db.collection('datasets').doc(id);
    const doc = await docRef.get();

    if (!doc.exists) {
      return res.status(404).json({ error: 'Dataset ID not found' });
    }

    const latestVersionId = doc.data().latestVersion;
    const latestVersionRef = db.collection('datasets').doc(id).collection('versions').doc(latestVersionId);
    const latestVersion = await latestVersionRef.get();

    if (!latestVersion.exists) {
      return res.status(404).json({ error: 'Version not found' });
    }
    if (latestVersion.data().isDisabled) {
      return res.status(403).json({ error: 'Document disabled' });
    }

    const fileUrl = latestVersion.data().fileUrl;
    if (!fileUrl) {
      return res.status(404).json({ error: 'File URL not found' });
    }

    const fileName = `${id}_${latestVersionId}.zip`;
    const filePath = path.join(savePath, fileName);

    const writer = fs.createWriteStream(filePath);
    const response = await axios({
      url: fileUrl,
      method: 'GET',
      responseType: 'stream',
      timeout: 30000
    });

    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on('finish', async () => {
        try {
          await updateRequestCount(userId, id, latestVersionId);
          res.status(200).json({ message: 'File downloaded successfully', path: filePath });
          resolve();
        } catch (error) {
          console.error(error);
          res.status(500).json({ error: 'Error updating request count' });
          reject(error);
        }
      });

      writer.on('error', (error) => {
        console.error('File write error:', error);
        res.status(500).json({ error: 'Error writing the file' });
        reject(error);
      });
    });
  } catch (error) {
    console.error('Download error:', error);
    if (error.response) {
      return res.status(502).json({ error: 'Failed to download file from storage' });
    }
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});