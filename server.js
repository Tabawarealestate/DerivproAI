const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const DERIV_WS_URL = 'wss://ws.derivws.com/websockets/v3?app_id=1089';
let derivWs = null;

// Active Markets Registry
const MARKETS = {
    'R_100': 'Volatility 100 Index',
    'R_75': 'Volatility 75 Index',
    'R_50': 'Volatility 50 Index',
    'R_25': 'Volatility 25 Index',
    'R_10': 'Volatility 10 Index',
    '1HZ100V': 'Volatility 100 (1s) Index',
    '1HZ75V': 'Volatility 75 (1s) Index',
    '1HZ50V': 'Volatility 50 (1s) Index'
};

let activeSymbol = 'R_100';
let activeWindowSize = 100; // Default rolling window
const tickBuffers = {}; 

// Initialize tick buffers
Object.keys(MARKETS).forEach(sym => { tickBuffers[sym] = []; });

function connectDeriv() {
    derivWs = new WebSocket(DERIV_WS_URL);

    derivWs.on('open', () => {
        console.log('[Deriv API] Connected successfully.');
        requestHistoricalTicks(activeSymbol, activeWindowSize);
    });

    derivWs.on('message', (data) => {
        try {
            const response = JSON.parse(data);

            // 1. Historical Ticks Payload
            if (response.msg_type === 'history') {
                const prices = response.history.prices;
                const times = response.history.times;
                const symbol = response.echo_req.ticks_history;
                
                tickBuffers[symbol] = prices.map((p, idx) => {
                    const priceStr = p.toString();
                    return {
                        quote: p,
                        epoch: times[idx],
                        digit: parseInt(priceStr.charAt(priceStr.length - 1))
                    };
                });

                subscribeLiveTick(symbol);
                broadcastUpdate(symbol);
            }

            // 2. Live Tick Stream
            if (response.msg_type === 'tick') {
                const tick = response.tick;
                const symbol = tick.symbol;
                const priceStr = tick.quote.toString();
                const lastDigit = parseInt(priceStr.charAt(priceStr.length - 1));

                if (!tickBuffers[symbol]) tickBuffers[symbol] = [];

                tickBuffers[symbol].push({
                    quote: tick.quote,
                    epoch: tick.epoch,
                    digit: lastDigit
                });

                // Maintain max rolling buffer of 1000 ticks
                if (tickBuffers[symbol].length > 1000) tickBuffers[symbol].shift();

                broadcastUpdate(symbol, tick.quote, lastDigit);
            }
        } catch (err) {
            console.error('[Deriv Data Parse Error]:', err.message);
        }
    });

    derivWs.on('close', () => {
        console.log('[Deriv API] Disconnected. Attempting reconnect in 3s...');
        setTimeout(connectDeriv, 3000);
    });

    derivWs.on('error', (err) => {
        console.error('[Deriv Socket Error]:', err.message);
    });
}

// Keep-Alive Ping Every 30s
setInterval(() => {
    if (derivWs && derivWs.readyState === WebSocket.OPEN) {
        derivWs.send(JSON.stringify({ ping: 1 }));
    }
}, 30000);

function requestHistoricalTicks(symbol, count = 100) {
    if (derivWs && derivWs.readyState === WebSocket.OPEN) {
        derivWs.send(JSON.stringify({ forget_all: "ticks" }));
        derivWs.send(JSON.stringify({
            ticks_history: symbol,
            adjust_start_time: 1,
            count: count,
            end: "latest",
            style: "ticks"
        }));
    }
}

function subscribeLiveTick(symbol) {
    if (derivWs && derivWs.readyState === WebSocket.OPEN) {
        derivWs.send(JSON.stringify({
            ticks: symbol,
            subscribe: 1
        }));
    }
}

// Statistical Engine Calculations
function analyzeTicks(symbol, windowSize) {
    const rawBuffer = tickBuffers[symbol] || [];
    if (rawBuffer.length === 0) return null;

    const history = rawBuffer.slice(-windowSize);
    const total = history.length;

    // Digit Distribution
    const digitCounts = Array(10).fill(0);
    history.forEach(t => {
        if (!isNaN(t.digit) && t.digit >= 0 && t.digit <= 9) {
            digitCounts[t.digit]++;
        }
    });

    const digitPercentages = digitCounts.map(c => ((c / total) * 100).toFixed(1));

    // Hot & Cold Digits
    let hotDigit = 0, coldDigit = 0;
    digitCounts.forEach((cnt, idx) => {
        if (cnt > digitCounts[hotDigit]) hotDigit = idx;
        if (cnt < digitCounts[coldDigit]) coldDigit = idx;
    });

    // Over/Under 5
    const over5Count = history.filter(t => t.digit > 5).length;
    const under5Count = history.filter(t => t.digit < 5).length;
    const over5Prob = ((over5Count / total) * 100).toFixed(1);
    const under5Prob = ((under5Count / total) * 100).toFixed(1);

    // Even/Odd
    const evenCount = history.filter(t => t.digit % 2 === 0).length;
    const oddCount = total - evenCount;
    const evenProb = ((evenCount / total) * 100).toFixed(1);
    const oddProb = ((oddCount / total) * 100).toFixed(1);

    // Rise/Fall (Momentum over last 10 ticks)
    let riseCount = 0, fallCount = 0;
    const recent = history.slice(-10);
    for (let i = 1; i < recent.length; i++) {
        if (recent[i].quote > recent[i - 1].quote) riseCount++;
        if (recent[i].quote < recent[i - 1].quote) fallCount++;
    }
    const riseProb = (((riseCount / (recent.length - 1 || 1))) * 100).toFixed(1);
    const fallProb = (((fallCount / (recent.length - 1 || 1))) * 100).toFixed(1);

    // AI Confidence & Signal Generator (Multi-Factor Scoring)
    const hotFreq = parseFloat(digitPercentages[hotDigit]);
    let confidence = Math.min(Math.round((hotFreq / 10) * 55 + (total / windowSize) * 30), 96);
    
    let signalType = 'NO SIGNAL';
    let prediction = 'WAITING FOR CONFLUENCE';
    let strength = 'LOW';

    if (confidence >= 75) {
        if (hotFreq >= 15.0) {
            signalType = 'MATCHES';
            prediction = `MATCHES ${hotDigit}`;
            strength = confidence >= 85 ? 'VERY HIGH' : 'HIGH';
        } else if (parseFloat(over5Prob) >= 60.0) {
            signalType = 'OVER / UNDER';
            prediction = 'OVER 5';
            strength = 'HIGH';
        } else if (parseFloat(under5Prob) >= 60.0) {
            signalType = 'OVER / UNDER';
            prediction = 'UNDER 5';
            strength = 'HIGH';
        } else if (parseFloat(evenProb) >= 60.0) {
            signalType = 'EVEN / ODD';
            prediction = 'EVEN';
            strength = 'MODERATE';
        } else if (parseFloat(oddProb) >= 60.0) {
            signalType = 'EVEN / ODD';
            prediction = 'ODD';
            strength = 'MODERATE';
        } else if (parseFloat(riseProb) >= 70.0) {
            signalType = 'RISE / FALL';
            prediction = 'RISE';
            strength = 'HIGH';
        } else if (parseFloat(fallProb) >= 70.0) {
            signalType = 'RISE / FALL';
            prediction = 'FALL';
            strength = 'HIGH';
        }
    }

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
        last15Digits: history.slice(-15).map(t => t.digit),
        recentPrices: history.slice(-20).map(t => t.quote)
    };
}

function broadcastUpdate(symbol, price = null, lastDigit = null) {
    const stats = analyzeTicks(symbol, activeWindowSize);
    if (!stats) return;

    const payload = JSON.stringify({
        type: 'MARKET_UPDATE',
        symbol,
        marketName: MARKETS[symbol] || symbol,
        price: price || (tickBuffers[symbol].length ? tickBuffers[symbol][tickBuffers[symbol].length - 1].quote : '0.00'),
        lastDigit: lastDigit !== null ? lastDigit : (tickBuffers[symbol].length ? tickBuffers[symbol][tickBuffers[symbol].length - 1].digit : '-'),
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
    
    // Broadcast initial payload immediately
    if (tickBuffers[activeSymbol] && tickBuffers[activeSymbol].length > 0) {
        broadcastUpdate(activeSymbol);
    }

    ws.on('message', (message) => {
        try {
            const cmd = JSON.parse(message);
            if (cmd.action === 'CHANGE_MARKET') {
                activeSymbol = cmd.symbol;
                requestHistoricalTicks(activeSymbol, activeWindowSize);
            }
            if (cmd.action === 'CHANGE_WINDOW') {
                activeWindowSize = parseInt(cmd.window);
                requestHistoricalTicks(activeSymbol, activeWindowSize);
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
server.listen(PORT, '0.0.0.0', () => console.log(`[Deriv AI Platform] Running on port ${PORT}`));
