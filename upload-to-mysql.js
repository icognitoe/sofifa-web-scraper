const mysql = require('mysql2/promise');
const fs = require('fs').promises;

async function uploadToDatabase() {
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME
  });

  try {
    console.log('Connecting to database...');
    
    // Read the scraped data
    const playersData = JSON.parse(
      await fs.readFile('./data/players.json', 'utf8')
    );

    console.log(`Processing ${playersData.length} players...`);

    // Analyze data and create missing columns
    await createMissingColumns(connection, playersData);

    // Process each player
    for (let i = 0; i < playersData.length; i++) {
      const player = playersData[i];
      
      if (i % 100 === 0) {
        console.log(`Processed ${i} players...`);
      }

      await insertPlayerData(connection, player);
    }

    console.log('Successfully uploaded all player data to database!');
    
  } catch (error) {
    console.error('Error uploading to database:', error);
    process.exit(1);
  } finally {
    await connection.end();
  }
}

async function createMissingColumns(connection, playersData) {
  console.log('Analyzing data structure and creating missing columns...');
  
  // Get existing columns for each table
  const existingColumns = {
    players: await getTableColumns(connection, 'players'),
    player_profiles: await getTableColumns(connection, 'player_profiles'),
    player_statistics: await getTableColumns(connection, 'player_statistics')
  };

  // Analyze what columns we need based on the data
  const requiredColumns = analyzeDataStructure(playersData);
  
  // Create missing columns
  await addMissingColumns(connection, 'players', existingColumns.players, requiredColumns.players);
  await addMissingColumns(connection, 'player_profiles', existingColumns.player_profiles, requiredColumns.player_profiles);
  await addMissingColumns(connection, 'player_statistics', existingColumns.player_statistics, requiredColumns.player_statistics);
}

async function getTableColumns(connection, tableName) {
  try {
    const [rows] = await connection.execute(`SHOW COLUMNS FROM ${tableName}`);
    return rows.map(row => row.Field);
  } catch (error) {
    console.log(`Table ${tableName} doesn't exist, will create columns as needed`);
    return [];
  }
}

function analyzeDataStructure(playersData) {
  const columns = {
    players: new Set(['player_id', 'player_name', 'club_name', 'club_id', 'created_at', 'last_updated']),
    player_profiles: new Set(['player_id', 'player_name', 'full_name', 'nationality', 'birth_date', 'height', 'preferred_foot', 'positions', 'last_updated']),
    player_statistics: new Set(['player_id', 'player_name', 'season', 'club', 'competition', 'last_updated'])
  };

  // Sample some players to find all possible fields
  const sampleSize = Math.min(100, playersData.length);
  for (let i = 0; i < sampleSize; i++) {
    const player = playersData[i];
    
    // Add any additional fields found in player data
    Object.keys(player).forEach(key => {
      const cleanKey = sanitizeColumnName(key);
      
      // Determine which table this field belongs to
      if (isProfileField(key)) {
        columns.player_profiles.add(cleanKey);
      } else if (isStatField(key)) {
        columns.player_statistics.add(cleanKey);
      } else {
        columns.players.add(cleanKey);
      }
    });

    // Add stats fields if they exist
    if (player.stats) {
      Object.keys(player.stats).forEach(statKey => {
        const cleanKey = sanitizeColumnName(statKey);
        columns.player_statistics.add(cleanKey);
      });
    }

    // Add any nested object fields
    if (player.attributes) {
      Object.keys(player.attributes).forEach(attrKey => {
        const cleanKey = sanitizeColumnName(attrKey);
        columns.player_profiles.add(cleanKey);
      });
    }
  }

  // Convert Sets back to Arrays
  return {
    players: Array.from(columns.players),
    player_profiles: Array.from(columns.player_profiles),
    player_statistics: Array.from(columns.player_statistics)
  };
}

function isProfileField(fieldName) {
  const profileFields = [
    'fullName', 'nationality', 'birthDate', 'height', 'weight', 
    'preferredFoot', 'positions', 'skillMoves', 'weakFoot',
    'workRate', 'bodyType', 'realFace', 'releaseClause',
    'attributes', 'traits', 'specialities'
  ];
  return profileFields.some(field => fieldName.toLowerCase().includes(field.toLowerCase()));
}

function isStatField(fieldName) {
  const statFields = [
    'goals', 'assists', 'appearances', 'minutes', 'cards', 'overall',
    'pace', 'shooting', 'passing', 'dribbling', 'defending', 'physical',
    'crossing', 'finishing', 'heading', 'short', 'volleys', 'curve',
    'freekick', 'longpassing', 'ballcontrol'
  ];
  return statFields.some(field => fieldName.toLowerCase().includes(field.toLowerCase()));
}

function sanitizeColumnName(name) {
  return name
    .replace(/[^a-zA-Z0-9]/g, '_')
    .toLowerCase()
    .replace(/__+/g, '_')
    .replace(/^_|_$/g, '')
    .substring(0, 64); // MySQL column name limit
}

function determineColumnType(sampleValue, columnName) {
  if (sampleValue === null || sampleValue === undefined) {
    return 'TEXT';
  }
  
  if (typeof sampleValue === 'number') {
    if (Number.isInteger(sampleValue)) {
      return sampleValue >= 0 && sampleValue <= 255 ? 'TINYINT UNSIGNED' : 'INT';
    } else {
      return 'DECIMAL(10,2)';
    }
  }
  
  if (typeof sampleValue === 'boolean') {
    return 'BOOLEAN';
  }
  
  if (typeof sampleValue === 'string') {
    if (sampleValue.length <= 50) {
      return 'VARCHAR(255)';
    } else {
      return 'TEXT';
    }
  }
  
  if (Array.isArray(sampleValue)) {
    return 'JSON';
  }
  
  if (typeof sampleValue === 'object') {
    return 'JSON';
  }
  
  return 'TEXT'; // Default fallback
}

async function addMissingColumns(connection, tableName, existingColumns, requiredColumns) {
  const missingColumns = requiredColumns.filter(col => !existingColumns.includes(col));
  
  if (missingColumns.length === 0) {
    console.log(`No missing columns for table ${tableName}`);
    return;
  }

  console.log(`Adding ${missingColumns.length} missing columns to ${tableName}:`, missingColumns);

  for (const columnName of missingColumns) {
    try {
      // Determine appropriate column type (you might want to make this more sophisticated)
      const columnType = getDefaultColumnType(columnName);
      
      const alterQuery = `ALTER TABLE ${tableName} ADD COLUMN \`${columnName}\` ${columnType}`;
      await connection.execute(alterQuery);
      console.log(`Added column: ${columnName} (${columnType}) to ${tableName}`);
      
    } catch (error) {
      if (error.code === 'ER_DUP_FIELDNAME') {
        console.log(`Column ${columnName} already exists in ${tableName}`);
      } else {
        console.error(`Error adding column ${columnName} to ${tableName}:`, error.message);
      }
    }
  }
}

function getDefaultColumnType(columnName) {
  const name = columnName.toLowerCase();
  
  // Specific mappings based on common field names
  if (name.includes('id')) return 'VARCHAR(50)';
  if (name.includes('name')) return 'VARCHAR(255)';
  if (name.includes('date')) return 'DATE';
  if (name.includes('timestamp') || name.includes('updated') || name.includes('created')) return 'TIMESTAMP';
  if (name.includes('url')) return 'VARCHAR(500)';
  if (name.includes('email')) return 'VARCHAR(255)';
  if (name.includes('phone')) return 'VARCHAR(20)';
  if (name.includes('age')) return 'TINYINT UNSIGNED';
  if (name.includes('height') || name.includes('weight')) return 'INT';
  if (name.includes('price') || name.includes('value') || name.includes('wage')) return 'DECIMAL(15,2)';
  if (name.includes('rating') || name.includes('overall')) return 'TINYINT UNSIGNED';
  if (name.includes('position') || name.includes('foot')) return 'VARCHAR(100)';
  if (name.includes('nationality') || name.includes('country')) return 'VARCHAR(100)';
  if (name.includes('club') || name.includes('team')) return 'VARCHAR(255)';
  
  // Default types
  return 'TEXT';
}

async function insertPlayerData(connection, player) {
  try {
    // Insert into players table
    const playerFields = extractPlayerFields(player);
    await insertWithDynamicFields(connection, 'players', playerFields);

    // Insert into player_profiles
    const profileFields = extractProfileFields(player);
    await insertWithDynamicFields(connection, 'player_profiles', profileFields);

    // Insert into player_statistics
    const statsFields = extractStatsFields(player);
    await insertWithDynamicFields(connection, 'player_statistics', statsFields);
    
  } catch (error) {
    console.error(`Error inserting player ${player.name}:`, error.message);
  }
}

function extractPlayerFields(player) {
  return {
    player_id: player.id,
    player_name: player.name,
    club_name: player.club?.name || null,
    club_id: player.club?.id || null,
    // Add any additional fields that might be in the data
    ...extractAdditionalFields(player, ['id', 'name', 'club', 'stats', 'attributes'])
  };
}

function extractProfileFields(player) {
  const profile = {
    player_id: player.id,
    player_name: player.name,
    full_name: player.fullName || player.name,
    nationality: player.nationality,
    birth_date: player.birthDate,
    height: player.height,
    preferred_foot: player.preferredFoot,
    positions: player.positions ? JSON.stringify(player.positions) : null
  };

  // Add attributes if they exist
  if (player.attributes) {
    Object.keys(player.attributes).forEach(key => {
      const cleanKey = sanitizeColumnName(key);
      profile[cleanKey] = player.attributes[key];
    });
  }

  return profile;
}

function extractStatsFields(player) {
  const stats = {
    player_id: player.id,
    player_name: player.name,
    season: '2024-25',
    club: player.club?.name || null,
    competition: player.league?.name || null
  };

  // Add all stats if they exist
  if (player.stats) {
    Object.keys(player.stats).forEach(key => {
      const cleanKey = sanitizeColumnName(key);
      stats[cleanKey] = player.stats[key];
    });
  }

  return stats;
}

function extractAdditionalFields(obj, excludeKeys = []) {
  const additional = {};
  Object.keys(obj).forEach(key => {
    if (!excludeKeys.includes(key) && typeof obj[key] !== 'object') {
      const cleanKey = sanitizeColumnName(key);
      additional[cleanKey] = obj[key];
    }
  });
  return additional;
}

async function insertWithDynamicFields(connection, tableName, data) {
  const columns = Object.keys(data).filter(key => data[key] !== undefined);
  const values = columns.map(key => data[key]);
  
  if (columns.length === 0) return;

  // Add last_updated
  columns.push('last_updated');
  values.push(new Date());

  const placeholders = columns.map(col => col === 'last_updated' ? 'NOW()' : '?');
  const updateClauses = columns
    .filter(col => !['id', 'player_id'].includes(col))
    .map(col => col === 'last_updated' ? '`last_updated`=NOW()' : `\`${col}\`=VALUES(\`${col}\`)`);

  const query = `
    INSERT INTO ${tableName} (${columns.map(col => `\`${col}\``).join(', ')})
    VALUES (${placeholders.join(', ')})
    ON DUPLICATE KEY UPDATE ${updateClauses.join(', ')}
  `;

  await connection.execute(query, values.filter((_, index) => columns[index] !== 'last_updated'));
}

uploadToDatabase();
