import { DateTime } from 'luxon';

// WebSocket connection handler
let ws = null;
let messageHandlers = new Set();

// Global state for chart data persistence across tab switches
export const globalDataStore = {
    sentimentTimeSeriesData: {
        positive: [],
        neutral: [], 
        negative: []
    },
    asinReviewData: {},
    topProducts: []
};

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
            console.log("Received WebSocket message:", message);
            
            if (message.type === 'new_sentiment' || message.type === 'new_review') {
                console.log(`Processing ${message.type} data:`, message.data);
                
                // Update global data store with new sentiment data
                updateGlobalDataStore(message);
            }
            
            // Notify all registered handlers
            messageHandlers.forEach(handler => handler(message));
        } catch (error) {
            console.error('Error processing WebSocket message:', error, 'Raw data:', event.data);
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

// Function to update global data store with incoming data
function updateGlobalDataStore(message) {
    const data = message.data || message;
    if (!data) return;
    
    // Update sentiment time series data
    if (data.sentiment !== undefined || data.sentiment_label) {
        // Convert sentiment to standard format
        let sentimentCategory;
        if (data.sentiment === 2 || data.sentiment === 'positive' || data.sentiment_label === 'Positive') {
            sentimentCategory = 'positive';
        } else if (data.sentiment === 1 || data.sentiment === 'neutral' || data.sentiment_label === 'Neutral') {
            sentimentCategory = 'neutral';
        } else if (data.sentiment === 0 || data.sentiment === 'negative' || data.sentiment_label === 'Negative') {
            sentimentCategory = 'negative';
        } else {
            return; // Skip unknown sentiment
        }
        
        const timestamp = new Date(data.processed_at || data.prediction_time || new Date()).getTime();
        
        // Add to time series data
        globalDataStore.sentimentTimeSeriesData[sentimentCategory].push({
            x: timestamp,
            y: 1,
            asin: data.asin || 'unknown'
        });
        
        // Update ASIN review data
        if (data.asin) {
            const asin = data.asin;
            
            if (!globalDataStore.asinReviewData[asin]) {
                globalDataStore.asinReviewData[asin] = {
                    asin: asin,
                    title: data.title || data.summary || `Product ${asin}`,
                    sentimentCounts: {
                        positive: 0,
                        neutral: 0,
                        negative: 0
                    },
                    timestamp: timestamp
                };
            }
            
            // Increment sentiment counter
            globalDataStore.asinReviewData[asin].sentimentCounts[sentimentCategory]++;
            
            // Update top products list
            updateTopProducts();
        }
    }
}

// Calculate and update top products list
function updateTopProducts() {
    globalDataStore.topProducts = Object.entries(globalDataStore.asinReviewData)
        .sort(([, a], [, b]) => {
            const totalA = Object.values(a.sentimentCounts).reduce((sum, count) => sum + count, 0);
            const totalB = Object.values(b.sentimentCounts).reduce((sum, count) => sum + count, 0);
            return totalB - totalA;
        })
        .slice(0, 5)
        .map(([asin, data]) => ({ asin, ...data }));
}

export const addMessageHandler = (handler) => {
    messageHandlers.add(handler);
};

export const removeMessageHandler = (handler) => {
    messageHandlers.delete(handler);
};

// Process incoming review data
export const processReviewData = (data) => {
    console.log("data:", data);
    if (!data) return null;
    
    // Get the content based on message type
    const content = data.data || data;
    
    // Convert numeric sentiment to string if needed
    let sentimentLabel = content.sentiment_label || '';
    if (content.sentiment !== undefined && !sentimentLabel) {
        // Map numeric sentiment to string value if sentiment_label is not provided
        if (content.sentiment === 2) sentimentLabel = 'positive';
        else if (content.sentiment === 1) sentimentLabel = 'neutral';
        else if (content.sentiment === 0) sentimentLabel = 'negative';
    }
    console.log("content:", content);
    
    // Handle different field naming conventions
    return {
        asin: content.asin || content.ASIN || content.product_id || 'unknown',
        title: content.title || content.product_title || content.summary || '',
        reviewText: content.reviewText || content.review_text || content.text || '',
        overall: content.overall || content.rating || content.stars || 0,
        reviewerName: content.reviewerName || content.reviewer_name || 'Anonymous',
        date: content.date || content.reviewTime || content.review_time || content.prediction_time || new Date().toISOString(),
        helpfulVotes: parseInt(content.helpfulVotes || content.helpful_votes || 0),
        totalVotes: parseInt(content.totalVotes || content.total_votes || 0),
        sentiment: sentimentLabel.toLowerCase(),
        confidence: content.confidence || content.sentiment_confidence || 0,
        processedAt: content.processedAt || content.processed_at || content.prediction_time || new Date().toISOString()
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

  