import { DateTime } from 'luxon';

// WebSocket connection handler
let ws = null;
let messageHandlers = new Set();

export const connectWebSocket = () => {
    if (ws) return; // Already connected

    const wsUrl = 'ws://localhost:8006/ws/reviews';
    
    ws = new WebSocket(wsUrl);

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
            console.log("message:",message)
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
    console.log("data:",data)
    if (!data) return null;
    
    return {
        asin: data.asin,
        title: data.title,
        reviewText: data.reviewText,
        overall: data.overall,
        reviewerName: data.reviewer_name,
        date: data.date,
        helpfulVotes: parseInt(data.helpful_votes) || 0,
        totalVotes: parseInt(data.total_votes) || 0,
        sentiment: data.sentiment,
        processedAt: data.processed_at
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
        const reviewDate = DateTime.fromISO(review.timestamp).startOf('day').toJSDate(); //! check if timestamp is a part of the data
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

export const groupReviewsByASIN = (reviews) => {
    return reviews.reduce((acc, review) => {
        if (!acc[review.asin]) {
            acc[review.asin] = {
                asin: review.asin,
                title: review.title,
                reviews: [],
                averageRating: 0,
                totalReviews: 0,
                sentimentDistribution: {
                    positive: 0,
                    neutral: 0,
                    negative: 0
                }
            };
        }
        
        acc[review.asin].reviews.push(review);
        acc[review.asin].totalReviews++;
        acc[review.asin].sentimentDistribution[review.sentiment]++;
        acc[review.asin].averageRating = (
            acc[review.asin].reviews.reduce((sum, r) => sum + r.overall, 0) / 
            acc[review.asin].totalReviews
        );
        
        return acc;
    }, {});
};

export const calculateSentimentStats = (reviews) => {
    const stats = {
        positive: 0,
        neutral: 0,
        negative: 0,
        total: reviews.length
    };
    
    reviews.forEach(review => {
        stats[review.sentiment]++;
    });
    
    return stats;
};

export const formatDate = (dateString) => {
    return new Date(dateString).toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric'
    });
};
  