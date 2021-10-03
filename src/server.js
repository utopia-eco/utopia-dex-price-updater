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
const { PriceUpdater, web3Providers } = require("./priceQueryJob")
const Tokens = require("./tokens.js")
var web3ProviderChoice = 0;

app.listen(port, async () => {
  console.log(`Listening at http://localhost:${port}`)
  // Poll PKS
  var priceUpdater = new PriceUpdater(); 
  
  var fiveMinBarMap = new Map();
  var fourHrBarMap = new Map();
  var dailyBarMap = new Map();
  var tokens = Tokens.TokenList;
  var currentPrice;



  while (true) {
    // Loop through tokens that we are interestedin
    for  (const token of tokens) {
      const priceUpdateTime = Math.round(new Date() / 1000)
      console.log("Updating token", token, new Date()/1000)
      timeLimit = 15000; // 10 second time limit to retrieve price
      
      try {
        await fulfillWithTimeLimit(timeLimit, priceUpdater.init(token, web3ProviderChoice));
      } catch(error) {
        console.error("error initializing price updater for token", error)
        continue;
      }

      try {
        currentPrice = await fulfillWithTimeLimit(timeLimit, await priceUpdater.getLatestPrice(token));
        console.log("price of token", token, currentPrice)
      } catch(error) {
        console.error(token, error)
      }
      
      fiveMinBarMap.set(token, await updateCacheAndDatabase(token, currentPrice, fiveMinBarMap, 300, priceUpdateTime));
      fourHrBarMap.set(token, await updateCacheAndDatabase(token, currentPrice, fourHrBarMap, 14400, priceUpdateTime));
      dailyBarMap.set(token, await updateCacheAndDatabase(token, currentPrice, dailyBarMap, 86400, priceUpdateTime));
      await new Promise(resolve => setTimeout(resolve, 5000));
    }  
  }
})

async function updateCacheAndDatabase(token, currentPrice, barMap, timePeriod, currentTime) {
  var bar = barMap.get(token);
  // First attempt to retrieve bar from db
  if (bar == null || bar == undefined) {
    bar = await getPrevBarFromDb(token, timePeriod, currentTime);
  } 
  // Only updates the price if there is a previous recent bar located in the db or locally, and the bar is recent
  if (bar != null && bar != undefined && currentTime < (bar.startTime + timePeriod)) {
    bar.updatePrice(currentPrice, token);
    await updateDatabaseEntry(bar);
  } else {
    bar = Bar.createFreshBar(currentTime, timePeriod, currentPrice, token);
    bar = await createDatabaseEntry(bar);
  }
  return bar;
}

// Updates database entry for token using Bar object
async function updateDatabaseEntry(bar) {
  const data = {
    open: bar.open,
    close: bar.close,
    low: bar.low,
    high: bar.high,
    startTime: bar.startTime
  }
  const query = "UPDATE " + bar.token.toLowerCase() + "_" + bar.timePeriod + 
    " SET OPEN = ?, CLOSE = ?, LOW = ?, HIGH = ? " +
    "WHERE startTime = ?";
  
  try {
    await pool.query(query, Object.values(data)).catch((error) => {
        console.error("Execution of query to update price failed", data, error)
    })
  } catch (err) {
    console.error("Creation of connection to update price failed")
  }
}

// Creates database entry for token using Bar object
async function createDatabaseEntry(bar) {
  const data = {
    startTime: bar.startTime,
    open: bar.open,
    close: bar.close,
    low: bar.low,
    high: bar.high
  }
  
  const query = "INSERT INTO " + bar.token.toLowerCase() + "_" + bar.timePeriod + " VALUES (?, ?, ?, ?, ?)";
  try {
    await pool.query(query, Object.values(data)).catch((error) => {
      console.error("Execution of query to insert price failed", data, error)
    })
    return bar;
  } catch (err) {
    console.error("Price insertion query failed")
    console.error("Attempting to retrieve bar in case there is a duplicate entry")
    bar = await getPrevBarFromDb(token, timePeriod, currentTime);
    return bar;
  }
}

async function getPrevBarFromDb(token, timePeriod, time) {
  var startTime = time - (time % timePeriod)
  const query = "SELECT * FROM " + token.toLowerCase() + "_? WHERE startTime = ?"; // We substitute token directly here else it will have quotes
  try {
    const [result, fields] = await pool.query(query, [ timePeriod, startTime]);
    if (!result[0] || result == `{"status":"Not Found"}` ) {
      return null;
    } else {
      var jsonBar =  JSON.parse(JSON.stringify(result))[0];
      var bar = new Bar(token, jsonBar.startTime, timePeriod, jsonBar.low, jsonBar.high, jsonBar.open, jsonBar.close)
      return bar;
    }
  } catch (err) {
    console.error("Attempt to get previous bar from db failed")
  }
}

async function fulfillWithTimeLimit(timeLimit, task){
  let timeout;
  const timeoutPromise = new Promise((resolve, reject) => {
      timeout = setTimeout(() => {
          console.error("web3Provider is not responding after 15 seconds", web3Providers[web3ProviderChoice]);
          web3ProviderChoice = (web3ProviderChoice + 1) % web3Providers.length;
          console.error("Using this web3 provider now", web3Providers[web3ProviderChoice])
      }, timeLimit);
  });
  const response = await Promise.race([task, timeoutPromise]);
  if(timeout){ //the code works without this but let's be safe and clean up the timeout
      clearTimeout(timeout);
  }
  return response;
}