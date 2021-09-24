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
      const priceUpdateTime = Date.now();
      await priceUpdater.init(token); 
      var currentPrice = await priceUpdater.getLatestPrice(token);
      console.log(token, currentPrice)
      updateCacheAndDatabase(token, fiveMinBarMap, 300, priceUpdateTime)
      updateCacheAndDatabase(token, fourHrBarMap, 14400, priceUpdateTime)
      updateCacheAndDatabase(token, dailyBarMap, 86400, priceUpdateTime)

      await new Promise(r => setTimeout(r, 2000));

    }
    
  }
})

function updateCacheAndDatabase(token, barMap, timePeriod, currentTime) {
  var bar = barMap.get(token);
      if (bar != null && (bar.startTime + timePeriod) < currentTime) {
        bar.updatePrice(currentPrice, token);
        updateDatabaseEntry(bar);
      } else {
        fiveMinBar = new Bar(currentTime, timePeriod, currentPrice, token);
        createDatabaseEntry(bar);
      }
      fiveMinBarMap.set(token, fiveMinBar);
}

// Updates database entry for token using Bar object
function updateDatabaseEntry(bar) {
  const data = {
    open: bar.open,
    close: bar.close,
    low: bar.low,
    high: bar.high
  }
  const query = "UPDATE " + token + "_" + bar.timePeriod + 
    " SET OPEN = ?, CLOSE = ?, LOW = ?, HIGH = ? " +
    "WHERE startTime = ?";
  pool.query(query, Object.values(data), (error) => {
    if (error) {
      res.json({ status: "failure", reason: error.code });
    } else {
      res.json({ status: "success", data: data});
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
  const query = "INSERT INTO " + token + "_" + bar.timePeriod + " VALUES (?, ?, ?, ?, ?, ?)";
  pool.query(query, Object.values(data), (error) => {
    if (error) {
      res.json({ status: "failure", reason: error.code });
    } else {
      res.json({ status: "success", data: data});
    }
  })
}