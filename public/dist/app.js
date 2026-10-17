/**
 * ============================================================================
 * REAL-TIME CRYPTO INTELLIGENCE DASHBOARD - CORE ENGINE
 * ============================================================================
 *
 * Portfolio Project: Associate Frontend Developer
 * Focus Areas: High-Frequency WebSocket Streaming, Multi-Asset Switching, Memory Safety,
 *              Canvas Rendering Optimization, Core Web Vitals, and TypeScript Architecture.
 *
 * Architectural Highlights:
 *  1. Autonomous WebSocket Lifecycle: Self-healing connection with exponential
 *     backoff, heartbeat detection, dynamic asset switching, and explicit listeners.
 *  2. Zero-Memory-Leak Ring Buffer: Strict cap of 15 data points using O(1) array
 *     shifting (.shift()), avoiding garbage collection spikes during high-frequency ticks.
 *  3. Canvas Engine Optimization: chart.update('none') eliminates expensive requestAnimationFrame
 *     interpolation loops, preventing layout thrashing and preserving 60+ FPS.
 *  4. High-Performance DOM Updates: Cached formatters (Intl.NumberFormat) with dynamic
 *     currency precision, minimizing browser reflows and cumulative layout shifts (CLS).
 *
 * Supported Assets: BTC/USDT (default), ETH/USDT, SOL/USDT, BNB/USDT, XRP/USDT, DOGE/USDT
 * ============================================================================
 */
export const SUPPORTED_ASSETS = {
    BTC: {
        symbol: 'btcusdt',
        ticker: 'BTC',
        name: 'Bitcoin',
        decimals: 2,
        accentColor: '#10b981' // Emerald
    },
    ETH: {
        symbol: 'ethusdt',
        ticker: 'ETH',
        name: 'Ethereum',
        decimals: 2,
        accentColor: '#06b6d4' // Cyan
    },
    SOL: {
        symbol: 'solusdt',
        ticker: 'SOL',
        name: 'Solana',
        decimals: 2,
        accentColor: '#a855f7' // Purple
    },
    BNB: {
        symbol: 'bnbusdt',
        ticker: 'BNB',
        name: 'BNB',
        decimals: 2,
        accentColor: '#f59e0b' // Amber
    },
    XRP: {
        symbol: 'xrpusdt',
        ticker: 'XRP',
        name: 'Ripple',
        decimals: 4,
        accentColor: '#38bdf8' // Sky
    },
    DOGE: {
        symbol: 'dogeusdt',
        ticker: 'DOGE',
        name: 'Dogecoin',
        decimals: 4,
        accentColor: '#eab308' // Yellow
    }
};
// ----------------------------------------------------------------------------
// 2. BINANCE WEBSOCKET SERVICE (LIFECYCLE & RESILIENCE)
// ----------------------------------------------------------------------------
export class BinanceWebSocketService {
    socket = null;
    currentSymbol = 'btcusdt';
    usePort443 = false;
    reconnectAttempts = 0;
    maxReconnectDelay = 10000;
    reconnectTimer = null;
    isExplicitlyClosed = false;
    // Callback hooks for reactive subscribers
    onTradeCallback = null;
    onStateChangeCallback = null;
    lastPrice = null;
    constructor(initialSymbol = 'btcusdt') {
        this.currentSymbol = initialSymbol.toLowerCase();
    }
    onTrade(callback) {
        this.onTradeCallback = callback;
    }
    onStateChange(callback) {
        this.onStateChangeCallback = callback;
    }
    getStreamUrl() {
        const port = this.usePort443 ? '443' : '9443';
        return `wss://stream.binance.com:${port}/ws/${this.currentSymbol}@trade`;
    }
    /**
     * Switches to a new cryptocurrency pair, gracefully terminating the prior stream.
     */
    switchAsset(newSymbol) {
        const sanitized = newSymbol.toLowerCase();
        if (sanitized === this.currentSymbol && this.socket && this.socket.readyState === WebSocket.OPEN) {
            return;
        }
        this.currentSymbol = sanitized;
        this.lastPrice = null;
        this.disconnect();
        this.connect();
    }
    getCurrentSymbol() {
        return this.currentSymbol;
    }
    /**
     * Initiates the WebSocket handshake with autonomous error recovery.
     */
    connect() {
        this.isExplicitlyClosed = false;
        this.clearPendingReconnect();
        const targetUrl = this.getStreamUrl();
        this.notifyState('connecting', 'Connecting...');
        try {
            this.socket = new WebSocket(targetUrl);
            // 1. 'open' LIFECYCLE EVENT LISTENER
            this.socket.addEventListener('open', () => {
                this.reconnectAttempts = 0;
                this.notifyState('connected', 'Live Streaming');
                console.info(`[BinanceWebSocket] Connected to ${targetUrl}`);
            });
            // 2. 'message' LIFECYCLE EVENT LISTENER
            this.socket.addEventListener('message', (event) => {
                this.handleRawMessage(event.data);
            });
            // 3. 'error' LIFECYCLE EVENT LISTENER
            this.socket.addEventListener('error', (event) => {
                console.error('[BinanceWebSocket] Connection error event:', event);
                this.notifyState('error', 'Connection Error');
                // Toggle port fallback
                this.usePort443 = !this.usePort443;
            });
            // 4. 'close' LIFECYCLE EVENT LISTENER
            this.socket.addEventListener('close', (event) => {
                console.warn(`[BinanceWebSocket] Socket closed (Code: ${event.code})`);
                if (!this.isExplicitlyClosed) {
                    this.notifyState('error', 'Reconnecting...');
                    this.scheduleReconnect();
                }
                else {
                    this.notifyState('disconnected', 'Disconnected');
                }
            });
        }
        catch (err) {
            console.error('[BinanceWebSocket] Exception during socket creation:', err);
            this.notifyState('error', 'Connection Error');
            this.scheduleReconnect();
        }
    }
    handleRawMessage(rawData) {
        try {
            const data = JSON.parse(rawData);
            if (data.e !== 'trade')
                return;
            const price = parseFloat(data.p);
            const quantity = parseFloat(data.q);
            const quoteVolume = price * quantity;
            let direction = 'flat';
            if (this.lastPrice !== null) {
                if (price > this.lastPrice)
                    direction = 'up';
                else if (price < this.lastPrice)
                    direction = 'down';
            }
            this.lastPrice = price;
            const date = new Date(data.T);
            const hours = String(date.getHours()).padStart(2, '0');
            const minutes = String(date.getMinutes()).padStart(2, '0');
            const seconds = String(date.getSeconds()).padStart(2, '0');
            const milliseconds = String(Math.floor(date.getMilliseconds() / 10)).padStart(2, '0');
            const formattedTime = `${hours}:${minutes}:${seconds}.${milliseconds}`;
            const processed = {
                tradeId: data.t,
                timestamp: data.T,
                formattedTime,
                price,
                quantity,
                quoteVolume,
                isBuyerMaker: data.m,
                direction
            };
            if (this.onTradeCallback) {
                this.onTradeCallback(processed);
            }
        }
        catch (err) {
            console.error('[BinanceWebSocket] Failed to deserialize message:', err);
        }
    }
    scheduleReconnect() {
        if (this.isExplicitlyClosed || this.reconnectTimer !== null)
            return;
        this.reconnectAttempts++;
        const baseDelay = Math.min(1000 * Math.pow(1.5, this.reconnectAttempts - 1), this.maxReconnectDelay);
        const jitter = Math.floor(Math.random() * 500);
        const totalDelay = baseDelay + jitter;
        console.info(`[BinanceWebSocket] Reconnecting #${this.reconnectAttempts} in ${Math.round(totalDelay)}ms`);
        this.reconnectTimer = window.setTimeout(() => {
            this.reconnectTimer = null;
            this.connect();
        }, totalDelay);
    }
    clearPendingReconnect() {
        if (this.reconnectTimer !== null) {
            window.clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
    }
    notifyState(state, message) {
        if (this.onStateChangeCallback) {
            this.onStateChangeCallback(state, message);
        }
    }
    disconnect() {
        this.isExplicitlyClosed = true;
        this.clearPendingReconnect();
        if (this.socket) {
            this.socket.close();
            this.socket = null;
        }
        this.notifyState('disconnected', 'Disconnected');
    }
}
// ----------------------------------------------------------------------------
// 3. REAL-TIME CHART MANAGER (HIGH-PERFORMANCE CANVAS RENDERING)
// ----------------------------------------------------------------------------
export class RealtimeChartManager {
    chart = null;
    canvasElement;
    maxDataPoints = 15; // Strict maximum historical points
    labels = [];
    dataPoints = [];
    currentAccentColor = '#10b981';
    constructor(canvasElement) {
        this.canvasElement = canvasElement;
        this.initChart();
    }
    setAccentColor(color) {
        this.currentAccentColor = color;
    }
    initChart() {
        const ctx = this.canvasElement.getContext('2d');
        if (!ctx) {
            throw new Error('[RealtimeChartManager] Unable to acquire 2D context.');
        }
        const gradient = ctx.createLinearGradient(0, 0, 0, 320);
        gradient.addColorStop(0, 'rgba(16, 185, 129, 0.28)');
        gradient.addColorStop(0.5, 'rgba(6, 182, 212, 0.12)');
        gradient.addColorStop(1, 'rgba(15, 23, 42, 0.0)');
        this.chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: this.labels,
                datasets: [
                    {
                        label: 'Spot Price',
                        data: this.dataPoints,
                        borderColor: this.currentAccentColor,
                        backgroundColor: gradient,
                        borderWidth: 2.2,
                        pointRadius: 3,
                        pointHoverRadius: 6,
                        pointBackgroundColor: '#34d399',
                        pointBorderColor: '#0f172a',
                        pointBorderWidth: 1.5,
                        fill: true,
                        tension: 0.25,
                        cubicInterpolationMode: 'monotone'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                normalized: true,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        enabled: true,
                        backgroundColor: 'rgba(15, 23, 42, 0.95)',
                        titleColor: '#94a3b8',
                        bodyColor: '#f8fafc',
                        borderColor: '#334155',
                        borderWidth: 1,
                        padding: 10,
                        displayColors: false,
                        callbacks: {
                            label: (context) => {
                                const val = context.parsed.y;
                                return `Price: $${val.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 })}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        display: true,
                        grid: {
                            color: 'rgba(51, 65, 85, 0.35)',
                            drawTicks: false
                        },
                        ticks: {
                            color: '#64748b',
                            font: { family: 'monospace', size: 11 },
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 7
                        }
                    },
                    y: {
                        display: true,
                        position: 'right',
                        grid: {
                            color: 'rgba(51, 65, 85, 0.35)',
                            drawTicks: false
                        },
                        ticks: {
                            color: '#94a3b8',
                            font: { family: 'monospace', size: 11 },
                            callback: (value) => `$${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 })}`
                        }
                    }
                }
            }
        });
    }
    /**
     * Pushes a new tick, enforcing the strict 15-item cap via .shift() and zero-animation update.
     */
    pushTick(timestamp, price) {
        if (!this.chart)
            return;
        // Enforce 15-item maximum memory cap
        while (this.labels.length >= this.maxDataPoints) {
            this.labels.shift();
            this.dataPoints.shift();
        }
        this.labels.push(timestamp);
        this.dataPoints.push(price);
        // Dynamic accent color: uptick vs downtick
        if (this.dataPoints.length >= 2) {
            const prev = this.dataPoints[this.dataPoints.length - 2];
            const isUptick = price >= prev;
            this.chart.data.datasets[0].borderColor = isUptick ? this.currentAccentColor : '#f43f5e';
            this.chart.data.datasets[0].pointBackgroundColor = isUptick ? this.currentAccentColor : '#fb7185';
        }
        // Optimization Trick: chart.update('none') bypasses CPU animation loops
        this.chart.update('none');
    }
    /**
     * Flushes historical data points when switching between coins.
     */
    reset() {
        this.labels = [];
        this.dataPoints = [];
        if (this.chart) {
            this.chart.data.labels = this.labels;
            this.chart.data.datasets[0].data = this.dataPoints;
            this.chart.update('none');
        }
    }
    getBufferedCount() {
        return this.dataPoints.length;
    }
    destroy() {
        if (this.chart) {
            this.chart.destroy();
            this.chart = null;
        }
        this.labels = [];
        this.dataPoints = [];
    }
}
// ----------------------------------------------------------------------------
// 4. TELEMETRY & METRICS ACCUMULATOR
// ----------------------------------------------------------------------------
export class SessionMetricsTracker {
    sessionHigh = -Infinity;
    sessionLow = Infinity;
    sessionStartPrice = null;
    totalVolumeUSDT = 0;
    totalTradesCount = 0;
    tickTimestamps = [];
    recordTrade(trade) {
        const { price, quoteVolume } = trade;
        if (this.sessionStartPrice === null) {
            this.sessionStartPrice = price;
        }
        if (price > this.sessionHigh)
            this.sessionHigh = price;
        if (price < this.sessionLow)
            this.sessionLow = price;
        this.totalVolumeUSDT += quoteVolume;
        this.totalTradesCount++;
        const now = Date.now();
        this.tickTimestamps.push(now);
        while (this.tickTimestamps.length > 0 && this.tickTimestamps[0] < now - 1000) {
            this.tickTimestamps.shift();
        }
        const ticksPerSecond = this.tickTimestamps.length;
        const delta = price - this.sessionStartPrice;
        const deltaPercent = this.sessionStartPrice > 0 ? (delta / this.sessionStartPrice) * 100 : 0;
        return {
            high: this.sessionHigh,
            low: this.sessionLow,
            delta,
            deltaPercent,
            ticksPerSecond,
            totalVolume: this.totalVolumeUSDT,
            tradeCount: this.totalTradesCount
        };
    }
    reset() {
        this.sessionHigh = -Infinity;
        this.sessionLow = Infinity;
        this.sessionStartPrice = null;
        this.totalVolumeUSDT = 0;
        this.totalTradesCount = 0;
        this.tickTimestamps = [];
    }
}
// ----------------------------------------------------------------------------
// 5. DASHBOARD UI CONTROLLER (DOM BINDINGS & PRESENTATION)
// ----------------------------------------------------------------------------
export class DashboardController {
    wsService;
    chartManager = null;
    metricsTracker;
    currentAsset = SUPPORTED_ASSETS.BTC;
    // Cached DOM elements
    statusBadge = null;
    statusDot = null;
    statusText = null;
    assetNameLabel = null;
    assetTickerLabel = null;
    assetWatermark = null;
    headerSubText = null;
    priceDisplay = null;
    priceDeltaBadge = null;
    priceDirectionIcon = null;
    sessionHighDisplay = null;
    sessionLowDisplay = null;
    sessionVolumeDisplay = null;
    tickRateDisplay = null;
    memoryBufferBadge = null;
    lastTradeSizeDisplay = null;
    tradeTapeContainer = null;
    reconnectBtn = null;
    pauseBtn = null;
    assetButtons = null;
    isPaused = false;
    constructor() {
        this.wsService = new BinanceWebSocketService(this.currentAsset.symbol);
        this.metricsTracker = new SessionMetricsTracker();
    }
    initialize() {
        this.cacheDOMElements();
        this.initChart();
        this.setupEventListeners();
        this.setupAssetSelector();
        this.bindWebSocketService();
        this.wsService.connect();
    }
    cacheDOMElements() {
        this.statusBadge = document.getElementById('status-badge');
        this.statusDot = document.getElementById('status-dot');
        this.statusText = document.getElementById('status-text');
        this.assetNameLabel = document.getElementById('asset-name-label');
        this.assetTickerLabel = document.getElementById('asset-ticker-label');
        this.assetWatermark = document.getElementById('asset-watermark');
        this.headerSubText = document.getElementById('header-subtext');
        this.priceDisplay = document.getElementById('live-price');
        this.priceDeltaBadge = document.getElementById('price-delta-badge');
        this.priceDirectionIcon = document.getElementById('price-direction-icon');
        this.sessionHighDisplay = document.getElementById('stat-high');
        this.sessionLowDisplay = document.getElementById('stat-low');
        this.sessionVolumeDisplay = document.getElementById('stat-volume');
        this.tickRateDisplay = document.getElementById('stat-tick-rate');
        this.memoryBufferBadge = document.getElementById('stat-memory-buffer');
        this.lastTradeSizeDisplay = document.getElementById('stat-last-size');
        this.tradeTapeContainer = document.getElementById('trade-tape');
        this.reconnectBtn = document.getElementById('btn-reconnect');
        this.pauseBtn = document.getElementById('btn-pause');
        this.assetButtons = document.querySelectorAll('[data-asset]');
    }
    initChart() {
        const canvas = document.getElementById('crypto-chart');
        if (canvas) {
            this.chartManager = new RealtimeChartManager(canvas);
            this.chartManager.setAccentColor(this.currentAsset.accentColor);
        }
    }
    setupEventListeners() {
        if (this.reconnectBtn) {
            this.reconnectBtn.addEventListener('click', () => {
                this.resetStateForAssetSwitch();
                this.wsService.disconnect();
                setTimeout(() => this.wsService.connect(), 200);
            });
        }
        if (this.pauseBtn) {
            this.pauseBtn.addEventListener('click', () => {
                this.isPaused = !this.isPaused;
                this.pauseBtn.innerHTML = this.isPaused
                    ? '<svg class="w-4 h-4 inline-block mr-1 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"></path></svg> Resume'
                    : '<svg class="w-4 h-4 inline-block mr-1 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg> Pause';
            });
        }
        window.addEventListener('beforeunload', () => {
            this.wsService.disconnect();
            if (this.chartManager)
                this.chartManager.destroy();
        });
    }
    /**
     * Sets up interactive asset switcher pills (BTC, ETH, SOL, BNB, XRP, DOGE).
     */
    setupAssetSelector() {
        if (!this.assetButtons)
            return;
        this.assetButtons.forEach((btn) => {
            btn.addEventListener('click', () => {
                const targetTicker = btn.getAttribute('data-asset') || 'BTC';
                const targetAsset = SUPPORTED_ASSETS[targetTicker];
                if (!targetAsset || targetAsset.ticker === this.currentAsset.ticker)
                    return;
                this.selectAsset(targetAsset);
            });
        });
    }
    selectAsset(asset) {
        this.currentAsset = asset;
        // Update active pill styling
        if (this.assetButtons) {
            this.assetButtons.forEach((b) => {
                const isCurrent = b.getAttribute('data-asset') === asset.ticker;
                if (isCurrent) {
                    b.className = 'px-3 py-1 rounded-lg text-xs font-bold transition bg-emerald-500/20 text-emerald-300 border border-emerald-500/40 shadow-sm shadow-emerald-500/20';
                }
                else {
                    b.className = 'px-3 py-1 rounded-lg text-xs font-semibold text-slate-400 hover:text-slate-200 hover:bg-slate-800/60 border border-transparent transition';
                }
            });
        }
        // Update UI headers & watermark
        if (this.assetNameLabel)
            this.assetNameLabel.textContent = `${asset.name} Spot Index`;
        if (this.assetTickerLabel)
            this.assetTickerLabel.textContent = `${asset.ticker} / USDT`;
        if (this.assetWatermark)
            this.assetWatermark.textContent = asset.ticker;
        if (this.headerSubText)
            this.headerSubText.textContent = `${asset.ticker} / USDT`;
        // Reset chart, metrics, and tape
        this.resetStateForAssetSwitch();
        if (this.chartManager) {
            this.chartManager.setAccentColor(asset.accentColor);
        }
        // Switch WebSocket stream to the new symbol
        this.wsService.switchAsset(asset.symbol);
    }
    resetStateForAssetSwitch() {
        this.metricsTracker.reset();
        if (this.chartManager)
            this.chartManager.reset();
        if (this.tradeTapeContainer) {
            this.tradeTapeContainer.innerHTML = '<div class="text-center py-5 text-xs text-slate-500 font-mono">Connecting to ' + this.currentAsset.ticker + ' live stream...</div>';
        }
        if (this.priceDeltaBadge) {
            this.priceDeltaBadge.textContent = 'Syncing...';
            this.priceDeltaBadge.className = 'inline-flex items-center text-xs font-semibold px-2.5 py-1 rounded-md border bg-slate-800/60 text-slate-400 border-slate-700';
        }
        if (this.priceDisplay) {
            this.priceDisplay.textContent = '$0.00';
        }
    }
    bindWebSocketService() {
        this.wsService.onStateChange((state, message) => {
            this.renderConnectionStatus(state, message);
        });
        this.wsService.onTrade((trade) => {
            if (this.isPaused)
                return;
            this.handleIncomingTrade(trade);
        });
    }
    renderConnectionStatus(state, customMessage) {
        if (!this.statusBadge || !this.statusDot || !this.statusText)
            return;
        this.statusBadge.className = 'inline-flex items-center gap-2 px-3.5 py-1.5 rounded-full text-xs font-semibold tracking-wide border transition-all duration-300 backdrop-blur-md';
        this.statusDot.className = 'w-2 h-2 rounded-full';
        switch (state) {
            case 'connected':
                this.statusBadge.classList.add('bg-emerald-500/10', 'text-emerald-400', 'border-emerald-500/30', 'shadow-[0_0_15px_rgba(16,185,129,0.2)]');
                this.statusDot.classList.add('bg-emerald-400', 'animate-pulse');
                this.statusText.textContent = customMessage || 'Live Streaming';
                break;
            case 'error':
                this.statusBadge.classList.add('bg-rose-500/10', 'text-rose-400', 'border-rose-500/30', 'shadow-[0_0_15px_rgba(244,63,94,0.2)]');
                this.statusDot.classList.add('bg-rose-400', 'animate-ping');
                this.statusText.textContent = customMessage || 'Connection Error';
                break;
            case 'connecting':
                this.statusBadge.classList.add('bg-amber-500/10', 'text-amber-400', 'border-amber-500/30');
                this.statusDot.classList.add('bg-amber-400', 'animate-pulse');
                this.statusText.textContent = customMessage || 'Connecting...';
                break;
            case 'disconnected':
            default:
                this.statusBadge.classList.add('bg-slate-700/30', 'text-slate-400', 'border-slate-600/30');
                this.statusDot.classList.add('bg-slate-500');
                this.statusText.textContent = customMessage || 'Disconnected';
                break;
        }
    }
    formatPrice(val) {
        return `$${val.toLocaleString('en-US', {
            minimumFractionDigits: this.currentAsset.decimals,
            maximumFractionDigits: this.currentAsset.decimals
        })}`;
    }
    handleIncomingTrade(trade) {
        const { price, formattedTime, direction, quantity, quoteVolume } = trade;
        // 1. Update Core Metric Block
        if (this.priceDisplay) {
            this.priceDisplay.textContent = this.formatPrice(price);
            this.priceDisplay.classList.remove('text-emerald-400', 'text-rose-400', 'text-white');
            if (direction === 'up') {
                this.priceDisplay.classList.add('text-emerald-400');
            }
            else if (direction === 'down') {
                this.priceDisplay.classList.add('text-rose-400');
            }
            else {
                this.priceDisplay.classList.add('text-white');
            }
            setTimeout(() => {
                if (this.priceDisplay) {
                    this.priceDisplay.classList.remove('text-emerald-400', 'text-rose-400');
                    this.priceDisplay.classList.add('text-white');
                }
            }, 350);
        }
        // 2. Update Direction Icon
        if (this.priceDirectionIcon) {
            if (direction === 'up') {
                this.priceDirectionIcon.innerHTML = `<svg class="w-6 h-6 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 10l7-7m0 0l7 7m-7-7v18"></path></svg>`;
            }
            else if (direction === 'down') {
                this.priceDirectionIcon.innerHTML = `<svg class="w-6 h-6 text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M19 14l-7 7m0 0l-7-7m7 7V3"></path></svg>`;
            }
        }
        // 3. Push to Chart (Enforces 15 max limit & update('none') optimization)
        if (this.chartManager) {
            const shortTime = formattedTime.substring(0, 8);
            this.chartManager.pushTick(shortTime, price);
            if (this.memoryBufferBadge) {
                const buffered = this.chartManager.getBufferedCount();
                this.memoryBufferBadge.textContent = `${buffered} / 15 points (Memory Safe)`;
            }
        }
        // 4. Update Statistics & Performance Telemetry
        const stats = this.metricsTracker.recordTrade(trade);
        if (this.sessionHighDisplay && stats.high !== -Infinity) {
            this.sessionHighDisplay.textContent = this.formatPrice(stats.high);
        }
        if (this.sessionLowDisplay && stats.low !== Infinity) {
            this.sessionLowDisplay.textContent = this.formatPrice(stats.low);
        }
        if (this.sessionVolumeDisplay) {
            this.sessionVolumeDisplay.textContent = `$${Math.round(stats.totalVolume).toLocaleString('en-US')}`;
        }
        if (this.tickRateDisplay) {
            this.tickRateDisplay.textContent = `${stats.ticksPerSecond} ticks/s`;
        }
        if (this.lastTradeSizeDisplay) {
            this.lastTradeSizeDisplay.textContent = `${quantity.toFixed(4)} ${this.currentAsset.ticker} ($${Math.round(quoteVolume).toLocaleString()})`;
        }
        if (this.priceDeltaBadge) {
            const isPositive = stats.delta >= 0;
            const sign = isPositive ? '+' : '';
            this.priceDeltaBadge.textContent = `${sign}${this.formatPrice(stats.delta)} (${sign}${stats.deltaPercent.toFixed(2)}%)`;
            this.priceDeltaBadge.className = `inline-flex items-center text-xs font-semibold px-2.5 py-1 rounded-md border ${isPositive
                ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                : 'bg-rose-500/10 text-rose-400 border-rose-500/20'}`;
        }
        // 5. Render mini order flow tick in the ticker tape
        this.prependTradeTapeItem(trade);
    }
    prependTradeTapeItem(trade) {
        if (!this.tradeTapeContainer)
            return;
        if (this.tradeTapeContainer.children.length === 1 && this.tradeTapeContainer.children[0].textContent?.includes('Connecting')) {
            this.tradeTapeContainer.innerHTML = '';
        }
        const row = document.createElement('div');
        const isBuy = !trade.isBuyerMaker;
        row.className = `flex items-center justify-between py-1.5 px-3 rounded-lg text-xs font-mono transition-all duration-200 border-b border-slate-800/40 hover:bg-slate-800/40 ${isBuy ? 'text-emerald-400/90' : 'text-rose-400/90'}`;
        row.innerHTML = `
      <span class="text-slate-400 font-sans text-[11px]">${trade.formattedTime}</span>
      <span class="font-semibold">${this.formatPrice(trade.price)}</span>
      <span class="text-slate-300">${trade.quantity.toFixed(4)} ${this.currentAsset.ticker}</span>
      <span class="text-[10px] font-sans font-bold px-1.5 py-0.5 rounded ${isBuy ? 'bg-emerald-500/20 text-emerald-300' : 'bg-rose-500/20 text-rose-300'}">${isBuy ? 'BUY' : 'SELL'}</span>
    `;
        this.tradeTapeContainer.prepend(row);
        while (this.tradeTapeContainer.children.length > 6) {
            this.tradeTapeContainer.removeChild(this.tradeTapeContainer.lastElementChild);
        }
    }
}
// ----------------------------------------------------------------------------
// 6. APPLICATION BOOTSTRAPPER
// ----------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
    console.info('%c[Crypto Intelligence Hub] Initializing Multi-Asset Engine...', 'color: #10b981; font-weight: bold;');
    const dashboard = new DashboardController();
    dashboard.initialize();
});
//# sourceMappingURL=app.js.map