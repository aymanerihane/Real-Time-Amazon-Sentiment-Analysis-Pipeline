import { DateTime } from 'luxon';

// WebSocket connection handler
let ws = null;
let messageHandlers = new Set();

export const connectWebSocket = () => {
    if (ws) return; // Already connected

    ws = new WebSocket('ws://localhost:8006/ws/reviews');

    ws.onopen = () => {
        console.log('WebSocket connected');
        // Subscribe to receive all reviews
        ws.send(JSON.stringify({
            type: 'subscribe'
        }));
    };

    ws.onmessage = (event) => {
        try {
            const message = JSON.parse(event.data);
            // Notify all registered handlers
            messageHandlers.forEach(handler => handler(message));
        } catch (error) {
            console.error('Error processing WebSocket message:', error);
        }
    };

    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
    };

    ws.onclose = () => {
        console.log('WebSocket disconnected');
        ws = null;
        // Attempt to reconnect after 5 seconds
        setTimeout(connectWebSocket, 5000);
    };
};

export const addMessageHandler = (handler) => {
    messageHandlers.add(handler);
};

export const removeMessageHandler = (handler) => {
    messageHandlers.delete(handler);
};

// Process incoming review data
export const processReviewData = (data) => {
    if (!data || !data.data) return null;

    const review = data.data;
    return {
        id: review.reviewerID,
        asin: review.asin,
        reviewText: review.reviewText,
        sentiment: review.sentiment,
        confidence: review.sentiment_score,
        timestamp: review.date,
        overall: review.overall,
        helpful_votes: review.helpful_votes,
        total_votes: review.total_votes
    };
};

// Generate time series data from actual review data
export const generateTimeSeriesData = (reviews, days = 7) => {
    const data = [];
    const now = DateTime.now();
    const reviewCounts = new Map();

    // Initialize counts for each day
    for (let i = days; i >= 0; i--) {
        const date = now.minus({ days: i }).startOf('day').toJSDate();
        reviewCounts.set(date.toISOString(), 0);
    }

    // Count reviews for each day
    reviews.forEach(review => {
        const reviewDate = DateTime.fromISO(review.timestamp).startOf('day').toJSDate();
        const dateKey = reviewDate.toISOString();
        if (reviewCounts.has(dateKey)) {
            reviewCounts.set(dateKey, reviewCounts.get(dateKey) + 1);
        }
    });

    // Convert to array format
    reviewCounts.forEach((count, date) => {
        data.push({
            x: new Date(date),
            y: count
        });
    });

    return data.sort((a, b) => a.x - b.x);
};
  