const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });

(async () => {
  const dbName = process.env.MYSQL_DATABASE || 'web3_mvp';
  const socketPath = process.env.MYSQL_SOCKET || '';
  const host = process.env.MYSQL_HOST || 'localhost';
  const port = Number(process.env.MYSQL_PORT || 3306);
  const user = process.env.MYSQL_USER || 'root';
  const password = process.env.MYSQL_PASSWORD || '';

  let connection;
  try {
    const baseConfig = { user, password, multipleStatements: true };
    const cfg = socketPath ? { ...baseConfig, socketPath } : { ...baseConfig, host, port };
    connection = await mysql.createConnection(cfg);
    await connection.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``);
    await connection.query(`USE \`${dbName}\``);

    const schemaPath = path.resolve(__dirname, '..', 'db', 'schema.sql');
    const schemaSql = fs.readFileSync(schemaPath, 'utf8');

    await connection.query(schemaSql);
    console.log(`Database ${dbName} is ready. Schema applied.`);
  } catch (err) {
    console.error('DB setup error:', err.message);
    process.exitCode = 1;
  } finally {
    if (connection) await connection.end();
  }
})(); 