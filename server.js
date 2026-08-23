const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Official public App ID 1089 with secure WebSocket URI
const DERIV_WS_URL = 'wss://ws.derivws.com/websockets/v3?app_id=1089';
let derivWs = null;

const MARKETS = {
    'R_100': 'Volatility 100 Index',
    'R_75': 'Volatility 75 Index',
    'R_50': 'Volatility 50 Index',
    'R_25': 'Volatility 25 Index',
    'R_10': 'Volatility 10 Index',
    '1HZ100V': 'Volatility 100 (1s) Index',
    '1HZ75V': 'Volatility 75 (1s) Index'
};

let activeSymbol = 'R_100';
let activeWindowSize = 100;
const tickBuffers = {};

Object.keys(MARKETS).forEach(sym => { tickBuffers[sym] = []; });

function connectDeriv() {
    console.log('[Deriv API] Connecting to WebSocket...');
    derivWs = new WebSocket(DERIV_WS_URL);

    derivWs.on('open', () => {
        console.log('[Deriv API] Connected successfully.');
        subscribeSymbol(activeSymbol);
    });

    derivWs.on('message', (data) => {
        try {
            const response = JSON.parse(data);

            if (response.msg_type === 'history') {
                const prices = response.history.prices || [];
                const times = response.history.times || [];
                const symbol = response.echo_req.ticks_history;

                tickBuffers[symbol] = prices.map((p, idx) => {
                    const priceStr = p.toFixed(4);
                    return {
                        quote: p,
                        epoch: times[idx],
                        digit: parseInt(priceStr.slice(-1))
                    };
                });

                broadcastUpdate(symbol);
            }

            if (response.msg_type === 'tick') {
                const tick = response.tick;
                const symbol = tick.symbol;
                const priceStr = tick.quote.toFixed(4);
                const lastDigit = parseInt(priceStr.slice(-1));

                if (!tickBuffers[symbol]) tickBuffers[symbol] = [];

                tickBuffers[symbol].push({
                    quote: tick.quote,
                    epoch: tick.epoch,
                    digit: lastDigit
                });

                if (tickBuffers[symbol].length > 1000) tickBuffers[symbol].shift();

                broadcastUpdate(symbol, tick.quote, lastDigit);
            }
        } catch (err) {
            console.error('[Parse Error]:', err.message);
        }
    });

    derivWs.on('close', () => {
        console.log('[Deriv API] Disconnected. Reconnecting in 3s...');
        setTimeout(connectDeriv, 3000);
    });

    derivWs.on('error', (err) => {
        console.error('[Socket Error]:', err.message);
    });
}

function subscribeSymbol(symbol) {
    if (derivWs && derivWs.readyState === WebSocket.OPEN) {
        // Forget previous tick subscriptions
        derivWs.send(JSON.stringify({ forget_all: "ticks" }));

        // Request initial history + subscribe
        derivWs.send(JSON.stringify({
            ticks_history: symbol,
            adjust_start_time: 1,
            count: activeWindowSize,
            end: "latest",
            style: "ticks",
            subscribe: 1
        }));
    }
}

function analyzeTicks(symbol, windowSize) {
    const rawBuffer = tickBuffers[symbol] || [];
    if (rawBuffer.length === 0) return null;

    const history = rawBuffer.slice(-windowSize);
    const total = history.length;

    const digitCounts = Array(10).fill(0);
    history.forEach(t => {
        if (!isNaN(t.digit) && t.digit >= 0 && t.digit <= 9) {
            digitCounts[t.digit]++;
        }
    });

    const digitPercentages = digitCounts.map(c => ((c / total) * 100).toFixed(1));

    let hotDigit = 0, coldDigit = 0;
    digitCounts.forEach((cnt, idx) => {
        if (cnt > digitCounts[hotDigit]) hotDigit = idx;
        if (cnt < digitCounts[coldDigit]) coldDigit = idx;
    });

    const over5Count = history.filter(t => t.digit > 5).length;
    const under5Count = history.filter(t => t.digit < 5).length;
    const over5Prob = ((over5Count / total) * 100).toFixed(1);
    const under5Prob = ((under5Count / total) * 100).toFixed(1);

    const evenCount = history.filter(t => t.digit % 2 === 0).length;
    const oddCount = total - evenCount;
    const evenProb = ((evenCount / total) * 100).toFixed(1);
    const oddProb = ((oddCount / total) * 100).toFixed(1);

    let riseCount = 0, fallCount = 0;
    const recent = history.slice(-10);
    for (let i = 1; i < recent.length; i++) {
        if (recent[i].quote > recent[i - 1].quote) riseCount++;
        if (recent[i].quote < recent[i - 1].quote) fallCount++;
    }
    const riseProb = (((riseCount / (recent.length - 1 || 1))) * 100).toFixed(1);
    const fallProb = (((fallCount / (recent.length - 1 || 1))) * 100).toFixed(1);

    const hotFreq = parseFloat(digitPercentages[hotDigit]);
    let confidence = Math.min(Math.round((hotFreq / 10) * 55 + (total / windowSize) * 30), 96);
    
    let signalType = 'MATCHES';
    let prediction = `MATCHES ${hotDigit}`;
    let strength = confidence >= 80 ? 'HIGH' : 'MODERATE';

    return {
        sampleSize: total,
        windowSize,
        digitPercentages,
        hotDigit,
        coldDigit,
        over5Prob,
        under5Prob,
        evenProb,
        oddProb,
        riseProb,
        fallProb,
        signalType,
        prediction,
        confidence,
        strength,
        recentPrices: history.slice(-20).map(t => t.quote)
    };
}

function broadcastUpdate(symbol, price = null, lastDigit = null) {
    const stats = analyzeTicks(symbol, activeWindowSize);
    if (!stats) return;

    const currentPrice = price || (tickBuffers[symbol].length ? tickBuffers[symbol][tickBuffers[symbol].length - 1].quote : 0);
    const currentDigit = lastDigit !== null ? lastDigit : (tickBuffers[symbol].length ? tickBuffers[symbol][tickBuffers[symbol].length - 1].digit : 0);

    const payload = JSON.stringify({
        type: 'MARKET_UPDATE',
        symbol,
        marketName: MARKETS[symbol] || symbol,
        price: Number(currentPrice).toFixed(2),
        lastDigit: currentDigit,
        stats,
        timestamp: new Date().toISOString()
    });

    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    });
}

wss.on('connection', (ws) => {
    ws.send(JSON.stringify({ type: 'STATUS', status: 'CONNECTED' }));
    if (tickBuffers[activeSymbol] && tickBuffers[activeSymbol].length > 0) {
        broadcastUpdate(activeSymbol);
    }

    ws.on('message', (message) => {
        try {
            const cmd = JSON.parse(message);
            if (cmd.action === 'CHANGE_MARKET') {
                activeSymbol = cmd.symbol;
                subscribeSymbol(activeSymbol);
            }
            if (cmd.action === 'CHANGE_WINDOW') {
                activeWindowSize = parseInt(cmd.window);
                subscribeSymbol(activeSymbol);
            }
        } catch (e) {
            console.error('[Client Command Error]:', e.message);
        }
    });
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

connectDeriv();

const PORT = process.env.PORT || 10000;
server.listen(PORT, '0.0.0.0', () => console.log(`[Server] Running on port ${PORT}`));
