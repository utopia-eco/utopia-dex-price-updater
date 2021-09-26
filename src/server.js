require('dotenv').config()

const express = require('express')
const app = express()
const bodyParser = require('body-parser')
const connection = require('./databaseClient');
const { pool } = require('./databaseClient');
const cors = require ('cors')
const port = process.env.PORT

app.use(cors());
app.options('*', cors())

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
        res.json(results);
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
  

  while (true) {
    // Loop through tokens that we are interestedin
    for  (const token of tokens) {
      const priceUpdateTime = Math.round(new Date() / 1000)
      await priceUpdater.init(token); 
      var currentPrice = await priceUpdater.getLatestPrice(token);
      fiveMinBarMap.set(token, updateCacheAndDatabase(token, currentPrice, fiveMinBarMap, 300, priceUpdateTime));
      fourHrBarMap.set(token, updateCacheAndDatabase(token, currentPrice, fourHrBarMap, 14400, priceUpdateTime));
      dailyBarMap.set(token, updateCacheAndDatabase(token, currentPrice, dailyBarMap, 86400, priceUpdateTime));
    }  
  }
})

function updateCacheAndDatabase(token, currentPrice, barMap, timePeriod, currentTime) {
  var bar = barMap.get(token);
  // First attempt to retrieve bar from db
  if (bar == null) {
    bar = getPrevBarFromDb(token, timePeriod, currentTime);
  } 
  
  // Only updates the price if there is a previous recent bar located in the db or locally, and the bar is recent
  if (bar != null && currentTime < (bar.startTime + timePeriod)) {
    bar.updatePrice(currentPrice, token);
    updateDatabaseEntry(bar);
  } else {
    bar = Bar.createFreshBar(currentTime, timePeriod, currentPrice, token);
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
    high: bar.high,
    startTime: bar.startTime
  }
  const query = "UPDATE " + bar.token + "_" + bar.timePeriod + 
    " SET OPEN = ?, CLOSE = ?, LOW = ?, HIGH = ? " +
    "WHERE startTime = ?";
  
  pool.query(query, Object.values(data), (error) => {
    if (error) {
      console.error("Price update failed", data, error)
    }
  })
}

// Creates database entry for token using Bar object
function createDatabaseEntry(bar) {
  const data = {
    startTime: bar.startTime,
    open: bar.open,
    close: bar.close,
    low: bar.low,
    high: bar.high
  }
  console.error("error bar", bar.startTime)
  const query = "INSERT INTO " + bar.token + "_" + bar.timePeriod + " VALUES (?, ?, ?, ?, ?)";
  pool.query(query, Object.values(data), (error) => {
    if (error) {
      console.error("Price insertion failed", data, error)
    }
  })
  
}

function getPrevBarFromDb(token, timePeriod, time) {
  var startTime = time - (time % timePeriod)
  const query = "SELECT * FROM " + token + "_? WHERE startTime = ?"; // We substitute token directly here else it will have quotes
  pool.query(query, [ timePeriod, startTime], (error, results) => {
    if (error) {
      console.error("Retrieval of prev latest input has failed", token, startTime, timePeriod, error)
      throw error;
    }
    if (results == undefined || results == `{"status":"Not Found"}` || !results[0]) {
      return null;
    } else {
      var jsonBar =  JSON.parse(JSON.stringify(results));
      var bar = new Bar(token, jsonBar.startTime, timePeriod, jsonBar.low, jsonBar.high, jsonBar.open, jsonBar.close)
      return bar;
    }
  })
}