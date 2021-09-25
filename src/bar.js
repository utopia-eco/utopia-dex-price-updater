class Bar { 
    constructor(token, startTime, timePeriod, low, high, open, close) {
        this.token = token;
        this.startTime = startTime;
        this.timePeriod = timePeriod
        this.low = price;
        this.high = price;
        this.open = price;
        this.close = price;
    }

    static createFreshBar(time, timePeriod, price, token) {
        return new Bar(token, 
            this.getNearestTime(time, timePeriod),
            timePeriod,
            price,
            price,
            price,
            price)
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