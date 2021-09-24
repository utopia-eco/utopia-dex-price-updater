require('dotenv').config()

const express = require('express')
const app = express()
const bodyParser = require('body-parser')
const connection = require('./databaseClient');
const pool = require('./databaseClient');
const port = process.env.PORT



app.get('/', (req, res) => {
  res.send('This service updates the price history of tokens')
})

// Returns associated limit orders for orderer address
app.route('/testGet')
  .get(function(req, res) {
    const query = "SELECT * FROM 0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82_14400"
    pool.query(query, [ req.params.ordererAddress ], (error, results) => {
      console.error(error);
      if (error) throw error;
      if (!results[0]) {
        res.json({ status: "Not Found"});
      } else {
        res.json(results[0]);
      }
    })
  });

// Returns associated limit orders for orderer address
app.route('/testGet2')
  .get(function(req, res) {
    const query = "SELECT * FROM limitOrders"
    pool.query(query, [ req.params.ordererAddress ], (error, results) => {
      console.error(error);
      if (error) throw error;
      if (!results[0]) {
        res.json({ status: "Not Found"});
      } else {
        res.json(results[0]);
      }
    })
  });

app.get('/health', (req, res) => res.send("Healthy"));

const { Bar } = require('./bar.js')
const { PriceUpdater } = require("./priceQueryJob")
const Tokens = require("./tokens.js")
const TokensList = Tokens.TokensList

app.listen(port, async () => {
  console.log(`Listening at http://localhost:${port}`)
  // Poll PKS
  var priceUpdater = new PriceUpdater(); 
  
  var fiveMinBarMap = new Map();
  var fourHrBarMap = new Map();
  var dailyBarMap = new Map();
  var tokens = Tokens.TokenList;

  while (false) {
    // Loop through tokens that we are interestedin
    for  (const token of tokens) {
      const priceUpdateTime = Date.now();
      await priceUpdater.init(token); 
      var currentPrice = await priceUpdater.getLatestPrice(token);
      console.log(token, currentPrice)
      fiveMinBarMap.set(token, updateCacheAndDatabase(token, currentPrice, fiveMinBarMap, 300, priceUpdateTime));
      fourHrBarMap.set(token, updateCacheAndDatabase(token, currentPrice, fourHrBarMap, 14400, priceUpdateTime));
      dailyBarMap.set(token, updateCacheAndDatabase(token, currentPrice, dailyBarMap, 86400, priceUpdateTime));
      

      await new Promise(r => setTimeout(r, 200000000));

    }
    
  }
})

function updateCacheAndDatabase(token, currentPrice, barMap, timePeriod, currentTime) {
  var bar = barMap.get(token);
      if (bar != null && (bar.startTime + timePeriod) < currentTime) {
        bar.updatePrice(currentPrice, token);
        updateDatabaseEntry(bar);
      } else {
        bar = new Bar(currentTime, timePeriod, currentPrice, token);
        createDatabaseEntry(bar);
      }
  return bar;
}

// Updates database entry for token using Bar object
function updateDatabaseEntry(bar) {
  const data = {
    open: bar.open,
    close: bar.close,
    low: bar.low,
    high: bar.high
  }
  const query = "UPDATE " + bar.token + "_" + bar.timePeriod + 
    " SET OPEN = ?, CLOSE = ?, LOW = ?, HIGH = ? " +
    "WHERE startTime = ?";
  
  pool.query(query, Object.values(data), (error) => {
    if (error) {
      console.error('dbuser' + process.env.DB_USER)
      console.error('dbpass' + process.env.DB_PASS)
      console.error('dbdb' + process.env.DB_DATABASE)
      console.error(process.env.INSTANCE_CONNECTION_NAME)
      console.error("Price update failed", data, error)
    }
  })
}

// Creates database entry for token using Bar object
function createDatabaseEntry(bar) {
  console.log(bar)
  const data = {
    startTime: bar.startTime,
    open: bar.open,
    close: bar.close,
    low: bar.low,
    high: bar.high
  }
  const query = "INSERT INTO " + bar.token + "_" + bar.timePeriod + " VALUES (?, ?, ?, ?, ?, ?)";
  console.log(query);
  pool.query(query, Object.values(data), (error) => {
    if (error) {
      console.error('dbuser' + process.env.DB_USER)
      console.error('dbpass' + process.env.DB_PASS)
      console.error('dbdb' + process.env.DB_DATABASE)
      console.error(process.env.INSTANCE_CONNECTION_NAME)
      console.error("Price insertion failed", data, error)
    }
  })
}