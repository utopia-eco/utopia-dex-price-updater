class Bar { 
    constructor(time, timePeriod, price, token) {
        this.token = token;
        //initialTime operation
        this.startTime = this.getNearestTime(time, timePeriod);
        this.timePeriod = timePeriod
        this.low = price;
        this.high = price;
        this.open = price;
        this.close = price;
    }

    updatePrice(price) {
        this.close = price
        if (price > this.high) {
            this.high = price;
        }
        if (price < this.low) {
            this.low = price
        }
    }

    getNearestTime(time, timePeriod) {
        return time - (time % timePeriod)
    }
}

module.exports = { Bar }