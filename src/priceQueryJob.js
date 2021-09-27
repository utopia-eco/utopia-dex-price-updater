const Constants = require("./constants.js")
const Web3 = require("web3")
const web3 = new Web3("https://bsc-dataseed1.binance.org:443");
const BigNumber = require("./bigNumber.js");

class PriceUpdater {
  pancakeswapFactoryAbi = require("../abis/PancakeFactoryV2.json");
  pancakeswapPairAbi = require("../abis/PancakePair.json");
  pancakeswapFactoryV1 = new web3.eth.Contract(
    this.pancakeswapFactoryAbi,
    Constants.PANCAKESWAP_FACTORY_ADDR_V1
  );
  pancakeswapFactoryV2 = new web3.eth.Contract(
    this.pancakeswapFactoryAbi,
    Constants.PANCAKESWAP_FACTORY_ADDR_V2
  );

  constructor() {
  }

  init = async function (token) {

    const tokenAbi = require("../abis/" + token + ".json");
    const tokenContract = new web3.eth.Contract(
        tokenAbi,
        token
    );

    this.tokenDecimals = await tokenContract.methods
      .decimals()
      .call();
      this.contractPairs = [];
      
      if (Constants.ENABLE_PANCAKESWAP_V2) {
        this.contractPairs.push(await this.getContractPair(this.pancakeswapFactoryV2, token, Constants.ADDRESS_BNB));
      }
  };

  getContractPair = async function (factory, address0, address1) {
    const pairAddress = await factory.methods
      .getPair(address0, address1)
      .call();

    const contract = new web3.eth.Contract(this.pancakeswapPairAbi, pairAddress);
    const token0 = await contract.methods.token0().call();
    contract.addressOrderReversed = token0.toLowerCase() !== address0.toLowerCase();
    return contract;
  };

  // Price is reserve1/reserve0. However, sometimes we want to take the average of all of the pairs in the
  // event there are multiple liquidity pools. This helps in those cases.
  getAveragedPriceFromReserves = function (callContractAndResultList) {
    const reserve0 = callContractAndResultList
      .reduce(
        (a, b) => a.plus(new BigNumber(b.result[b.contract.addressOrderReversed ? "1" : "0"])),
        new BigNumber(0)
    );
    const reserve1 = callContractAndResultList
      .reduce(
        (a, b) => a.plus(new BigNumber(b.result[b.contract.addressOrderReversed ? "0" : "1"])),
        new BigNumber(0)
    );
    return reserve1.dividedBy(reserve0);
  };

  // web3.eth.BatchRequest allows us to batch requests, but each of the requests
  // have their own callback and return individually. It makes it a little hard to manage like this.
  // This is just a Promise that returns the entire result once they've all completed.
  batchCalls = function (callAndContractList) {
    return new Promise((resolve, reject) => {
      let operations = callAndContractList.map((c) => ({
        call: c.call,
        contract: c.contract,
        completed: false,
        result: null,
      }));

      const callback = function (callAndContract, error, response) {
        if (error) {
          reject(error);
        }

        const currentOperation = operations.find((c) => c.call === callAndContract.call);
        currentOperation.completed = true;
        currentOperation.result = response;

        if (operations.every((o) => o.completed)) {
          resolve(operations);
        }
      };

      let batch = new web3.eth.BatchRequest();
      callAndContractList.forEach((cc) => {
        batch.add(cc.call.call.request((e, r) => callback(cc, e, r)));
      });

      batch.execute();
    });
  };

  getLatestPrice = async function () {
    const reservesResults = await this.batchCalls(
      this.contractPairs.map((cp) => ({call: cp.methods.getReserves(), contract: cp }))
    );

    // Calculate average price for Riskmoon/BNB pair from reserves for PCS
    var price = this.getAveragedPriceFromReserves(reservesResults);

    // number is still a whole number, apply the proper decimal places from the contract (9)
    if (this.tokenDecimals != 18) {
        price = price.dividedBy(Math.pow(10, this.tokenDecimals));
    }
    return price.toFixed();
  };
}

module.exports = { PriceUpdater }