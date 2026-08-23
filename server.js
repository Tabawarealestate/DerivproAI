const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static(path.join(__dirname, 'public')));

const PORT = process.env.PORT || 10000;
const DERIV_APP_ID = 1089;

let derivWs = null;
let activeSymbol = 'R_100';
let activeWindow = 100;
let tickBuffer = [];

function connectDeriv() {
    derivWs = new WebSocket(`wss://ws.derivws.com/websockets/v3?app_id=${DERIV_APP_ID}`);

    derivWs.on('open', () => {
        console.log('[Deriv API] Connected successfully.');
        subscribeTicks(activeSymbol);
    });

    derivWs.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());

            // Ignore ping/pong and non-tick payload responses safely
            if (msg.msg_type !== 'tick' || !msg.tick) return;

            const tick = msg.tick;
            
            // Extract prices and digits safely
            const price = tick.quote;
            const priceStr = price.toFixed(tick.pip_size || 2);
            const lastDigit = parseInt(priceStr.slice(-1));

            if (!isNaN(lastDigit)) {
                tickBuffer.push({ price, priceStr, digit: lastDigit, time: tick.epoch });
                if (tickBuffer.length > 1000) tickBuffer.shift();
            }

            broadcastMarketUpdate();

        } catch (err) {
            console.error('[Deriv Data Parse Error]:', err.message);
        }
    });

    derivWs.on('close', () => {
        console.log('[Deriv API] Connection closed. Reconnecting...');
        setTimeout(connectDeriv, 3000);
    });

    derivWs.on('error', (err) => {
        console.error('[Deriv API Error]:', err.message);
    });
}

function subscribeTicks(symbol) {
    if (derivWs && derivWs.readyState === WebSocket.OPEN) {
        derivWs.send(JSON.stringify({ forget_all: 'ticks' }));
        derivWs.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
        tickBuffer = [];
    }
}

function calculateStats() {
    const currentWindow = tickBuffer.slice(-activeWindow);
    if (currentWindow.length === 0) return null;

    const counts = new Array(10).fill(0);
    currentWindow.forEach(t => counts[t.digit]++);

    const total = currentWindow.length;
    const digitPercentages = counts.map(c => Math.round((c / total) * 100));

    let hotDigit = 0, coldDigit = 0;
    let maxC = -1, minC = Infinity;

    counts.forEach((c, d) => {
        if (c > maxC) { maxC = c; hotDigit = d; }
        if (c < minC) { minC = c; coldDigit = d; }
    });

    const recentPrices = currentWindow.slice(-15).map(t => t.price);
    const confidence = digitPercentages[hotDigit] || 0;

    return {
        sampleSize: total,
        hotDigit,
        coldDigit,
        confidence,
        digitPercentages,
        recentPrices
    };
}

function broadcastMarketUpdate() {
    if (tickBuffer.length === 0) return;
    const lastTick = tickBuffer[tickBuffer.length - 1];
    const stats = calculateStats();

    const payload = JSON.stringify({
        type: 'MARKET_UPDATE',
        symbol: activeSymbol,
        marketName: activeSymbol.replace('_', ' '),
        price: lastTick.priceStr,
        lastDigit: lastTick.digit,
        stats
    });

    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    });
}

wss.on('connection', (ws) => {
    if (tickBuffer.length > 0) broadcastMarketUpdate();

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            if (data.action === 'CHANGE_MARKET' && data.symbol) {
                activeSymbol = data.symbol;
                subscribeTicks(activeSymbol);
            } else if (data.action === 'CHANGE_WINDOW' && data.window) {
                activeWindow = parseInt(data.window) || 100;
                broadcastMarketUpdate();
            }
        } catch (err) {
            console.error('[Client Message Error]:', err.message);
        }
    });
});

server.listen(PORT, () => {
    console.log(`[Deriv AI Platform] Running on port ${PORT}`);
    connectDeriv();
});
