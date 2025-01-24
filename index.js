const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const { pool } = require('./db');
const crypto = require('crypto');

async function createTable() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS public.ransomware_data (
        id UUID NOT NULL PRIMARY KEY,
        name TEXT[] NOT NULL,
        extensions TEXT NOT NULL,
        "extensionPattern" TEXT,
        "ransomNoteFilenames" TEXT,
        comment TEXT,
        "encryptionAlgorithm" TEXT,
        decryptor TEXT,
        resources TEXT[],
        screenshots TEXT,
        "microsoftDetectionName" TEXT,
        "microsoftInfo" TEXT,
        sandbox TEXT,
        iocs TEXT,
        snort TEXT,
        hash TEXT NOT NULL UNIQUE
    );
    ALTER TABLE public.ransomware_data OWNER TO postgres;
  `;

  try {
    await pool.query(createTableQuery);
    console.log('Table created or already exists.');
  } catch (err) {
    console.error('Error creating table:', err);
    throw err; // Let the error bubble up
  }
}

async function insertData() {
  return new Promise((resolve, reject) => {
    fs.readFile('./ransomware_overview.json', async (err, data) => {
      if (err) {
        return reject(err);
      }

      const parsedData = JSON.parse(data);
      let insertCount = 0;

      try {
        const promises = parsedData.map(async (d) => {
          const hash = crypto
            .createHash('sha256')
            .update(JSON.stringify(d.name) + d.extensions)
            .digest('hex');

          const res = await pool.query(
            `INSERT INTO ransomware_data (
              id, name, extensions, "extensionPattern", "ransomNoteFilenames", comment, "encryptionAlgorithm", decryptor, resources, screenshots, "microsoftDetectionName", "microsoftInfo", sandbox, iocs, snort, hash
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (hash) DO NOTHING`,
            [
              uuidv4(),
              d['name'] || null,
              d['extensions'] || '',
              d['extensionPattern'] || null,
              d['ransomNoteFilenames'] || null,
              d['comment'] || null,
              d['encryptionAlgorithm'] || null,
              d['decryptor'] || null,
              d['resources'] || null,
              d['screenshots'] || null,
              d['microsoftDetectionName'] || null,
              d['microsoftInfo'] || null,
              d['sandbox'] || null,
              d['iocs'] || null,
              d['snort'] || null,
              hash
            ]
          );

          if (res.rowCount > 0) {
            insertCount++;
          }
        });

        await Promise.all(promises);
        console.log('Successfully inserted:', insertCount);
        resolve(); // Resolve after all inserts are done
      } catch (error) {
        console.error('Error inserting data:', error);
        reject(error); // Reject the promise if any error occurs
      }
    });
  });
}

async function main() {
  try {
    await createTable();
    await insertData();
  } catch (err) {
    console.error('Error in main function:', err);
  } finally {
    await pool.end(); // Close the connection pool only after everything is done
    console.log('Database connection closed.');
  }
}

main();
