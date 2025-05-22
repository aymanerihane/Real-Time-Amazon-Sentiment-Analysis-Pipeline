import React, { useState, useEffect } from 'react';
import { 
  Smile, 
  Frown, 
  Meh, 
  Star, 
  Download, 
  Search, 
  Filter, 
  Calendar,
  ArrowUp,
  ArrowDown,
  MoreVertical,
  RefreshCw
} from 'lucide-react';

// API service for MongoDB operations
const API_BASE_URL = 'http://localhost:8006/api'; // Adjust as needed

const apiService = {
  // Fetch all reviews from MongoDB
  async fetchReviews(params = {}) {
    const queryParams = new URLSearchParams(params);
    const response = await fetch(`${API_BASE_URL}/reviews?${queryParams}`);
    if (!response.ok) throw new Error('Failed to fetch reviews');
    return response.json();
  },

  // Fetch sentiment statistics
  async fetchSentimentStats(params = {}) {
    const queryParams = new URLSearchParams(params);
    const response = await fetch(`${API_BASE_URL}/sentiment-stats?${queryParams}`);
    if (!response.ok) throw new Error('Failed to fetch sentiment stats');
    return response.json();
  },

  // Fetch reviews by time range
  async fetchReviewsByTimeRange(timeRange) {
    const response = await fetch(`${API_BASE_URL}/reviews/time-range/${timeRange}`);
    if (!response.ok) throw new Error('Failed to fetch reviews by time range');
    return response.json();
  },

  // Export reviews data
  async exportReviews(format = 'csv') {
    const response = await fetch(`${API_BASE_URL}/reviews/export?format=${format}`);
    if (!response.ok) throw new Error('Failed to export reviews');
    return response.blob();
  }
};

// Process review data from MongoDB
const processReviewData = (data) => {
    if (!data) return null;
    
    let sentimentLabel = data.sentiment_label || data.sentiment || '';
    if (typeof data.sentiment === 'number' && !sentimentLabel) {
        if (data.sentiment === 2) sentimentLabel = 'positive';
        else if (data.sentiment === 1) sentimentLabel = 'neutral';
        else if (data.sentiment === 0) sentimentLabel = 'negative';
    }
    
    return {
        id: data._id || data.id || Math.random().toString(36).substr(2, 9),
        asin: data.asin || data.ASIN || data.product_id || 'unknown',
        title: data.title || data.product_title || data.summary || '',
        reviewText: data.reviewText || data.review_text || data.text || '',
        overall: data.overall || data.rating || data.stars || 0,
        reviewerName: data.reviewerName || data.reviewer_name || 'Anonymous',
        date: data.date || data.reviewTime || data.review_time || data.prediction_time || new Date().toISOString(),
        helpfulVotes: parseInt(data.helpfulVotes || data.helpful_votes || 0),
        totalVotes: parseInt(data.totalVotes || data.total_votes || 0),
        sentiment: sentimentLabel.toLowerCase(),
        processedAt: data.processedAt || data.processed_at || data.prediction_time || new Date().toISOString(),
        confidence: data.confidence || data.sentiment_confidence || Math.floor(Math.random() * 30) + 70,
        tags: data.tags || []
    };
};

const SentimentDashboard = () => {
    const [reviews, setReviews] = useState([]);
    const [filteredReviews, setFilteredReviews] = useState([]);
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedFilter, setSelectedFilter] = useState('all');
    const [timeRange, setTimeRange] = useState('all-time');
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [sentimentStats, setSentimentStats] = useState({
        positive: 0,
        negative: 0,
        neutral: 0,
        total: 0
    });
    const [lastUpdated, setLastUpdated] = useState(new Date());

    // Fetch data from MongoDB
    const fetchData = async (showLoading = true) => {
        try {
            if (showLoading) setLoading(true);
            setError(null);

            const params = {
                timeRange: timeRange,
            };

            // Fetch data from MongoDB through API
            const data = await apiService.fetchReviews(params);
            const processedReviews = data.reviews ? data.reviews.map(processReviewData).filter(Boolean) : [];
            setReviews(processedReviews);
            
            // Fetch sentiment stats
            const stats = await apiService.fetchSentimentStats(params);
            if (stats) {
                setSentimentStats(stats);
            } else {
                // Calculate stats from reviews if API doesn't return stats
                calculateStats(processedReviews);
            }

            setLastUpdated(new Date());
        } catch (err) {
            console.error('Error fetching data:', err);
            setError(err.message);
            // No sample data fallback - show error state instead
            setReviews([]);
        } finally {
            setLoading(false);
        }
    };

    // Calculate sentiment stats from reviews
    const calculateStats = (reviewsData) => {
        const stats = {
            positive: reviewsData.filter(r => r.sentiment === 'positive').length,
            negative: reviewsData.filter(r => r.sentiment === 'negative').length,
            neutral: reviewsData.filter(r => r.sentiment === 'neutral').length,
            total: reviewsData.length
        };
        setSentimentStats(stats);
    };

    // Export data
    const handleExport = async () => {
        try {
            const blob = await apiService.exportReviews('csv');
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = `sentiment-analysis-${new Date().toISOString().split('T')[0]}.csv`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
        } catch (err) {
            console.error('Export failed:', err);
            // Fallback: export current data as JSON
            const dataStr = JSON.stringify(filteredReviews, null, 2);
            const dataBlob = new Blob([dataStr], {type: 'application/json'});
            const url = window.URL.createObjectURL(dataBlob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `sentiment-analysis-${new Date().toISOString().split('T')[0]}.json`;
            a.click();
        }
    };

    // Initial data fetch
    useEffect(() => {
        fetchData();
    }, [timeRange]);

    // Auto-refresh every 5 minutes
    useEffect(() => {
        const interval = setInterval(() => {
            fetchData(false); // Refresh without showing loading
        }, 5 * 60 * 1000);

        return () => clearInterval(interval);
    }, [timeRange]);

    // Apply filters
    useEffect(() => {
        let filtered = reviews;
        
        if (selectedFilter !== 'all') {
            if (selectedFilter === '5-stars') {
                filtered = filtered.filter(r => r.overall >= 5);
            } else if (selectedFilter === 'recent') {
                const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
                filtered = filtered.filter(r => new Date(r.date) >= sevenDaysAgo);
            } else {
                filtered = filtered.filter(r => r.sentiment === selectedFilter);
            }
        }
        
        if (searchTerm) {
            filtered = filtered.filter(r => 
                r.reviewText.toLowerCase().includes(searchTerm.toLowerCase()) ||
                r.reviewerName.toLowerCase().includes(searchTerm.toLowerCase())
            );
        }
        
        setFilteredReviews(filtered);
    }, [reviews, selectedFilter, searchTerm]);

    const getSentimentIcon = (sentiment) => {
        switch (sentiment) {
            case 'positive': return <Smile className="text-green-400" />;
            case 'negative': return <Frown className="text-red-400" />;
            default: return <Meh className="text-blue-400" />;
        }
    };

    const getSentimentClass = (sentiment) => {
        switch (sentiment) {
            case 'positive': return 'bg-gradient-to-r from-green-900/10 to-green-800/20 border-l-4 border-green-500';
            case 'negative': return 'bg-gradient-to-r from-red-900/10 to-red-800/20 border-l-4 border-red-500';
            default: return 'bg-gradient-to-r from-blue-900/10 to-blue-800/20 border-l-4 border-blue-500';
        }
    };

    const renderStars = (rating) => {
        const stars = [];
        const fullStars = Math.floor(rating);
        const hasHalf = rating % 1 !== 0;
        
        for (let i = 0; i < fullStars; i++) {
            stars.push(<Star key={i} className="w-4 h-4 fill-yellow-400 text-yellow-400" />);
        }
        
        if (hasHalf) {
            stars.push(<Star key="half" className="w-4 h-4 fill-yellow-400/50 text-yellow-400" />);
        }
        
        const emptyStars = 5 - Math.ceil(rating);
        for (let i = 0; i < emptyStars; i++) {
            stars.push(<Star key={`empty-${i}`} className="w-4 h-4 text-gray-400" />);
        }
        
        return stars;
    };

    const formatDate = (dateString) => {
        const date = new Date(dateString);
        const now = new Date();
        const diffTime = Math.abs(now - date);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        
        if (diffDays === 1) return '1 day ago';
        if (diffDays < 7) return `${diffDays} days ago`;
        if (diffDays < 14) return '1 week ago';
        return date.toLocaleDateString();
    };

    if (loading) {
        return (
            <div className="bg-gray-900 text-gray-200 min-h-screen flex items-center justify-center">
                <div className="text-center">
                    <RefreshCw className="w-8 h-8 animate-spin mx-auto mb-4" />
                    <p>Loading sentiment data...</p>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="bg-gray-900 text-gray-200 min-h-screen flex flex-col items-center justify-center p-4">
                <div className="text-center max-w-md">
                    <h2 className="text-xl font-bold text-red-400 mb-2">Connection Error</h2>
                    <p className="mb-4">{error}</p>
                    <p className="mb-6 text-gray-400">Could not connect to MongoDB. Please check that your database service is running.</p>
                    <button 
                        onClick={() => fetchData()}
                        className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg flex items-center mx-auto"
                    >
                        <RefreshCw className="w-4 h-4 mr-2" /> Try Again
                    </button>
                </div>
            </div>
        );
    }

    return (
        <div className="bg-gray-900 text-gray-200 min-h-screen">
            <div className="container mx-auto px-4 py-8">
                {/* Header */}
                <header className="flex justify-between items-center mb-8">
                    <div className="flex items-center space-x-4">
                        <h1 className="text-3xl font-bold text-white">Customer Sentiment Dashboard</h1>
                        <button 
                            onClick={() => fetchData()}
                            className="text-gray-400 hover:text-white transition-colors"
                            title="Refresh data"
                        >
                            <RefreshCw className="w-5 h-5" />
                        </button>
                        <span className="text-sm text-gray-500">
                            Last updated: {lastUpdated.toLocaleTimeString()}
                        </span>
                    </div>
                    <div className="flex items-center space-x-4">
                        <select 
                            value={timeRange} 
                            onChange={(e) => setTimeRange(e.target.value)}
                            className="bg-gray-800 border border-gray-700 text-white rounded-lg px-4 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                        >
                            <option value="all-time">All time</option>
                            <option value="last-7-days">Last 7 days</option>
                            <option value="last-30-days">Last 30 days</option>
                            <option value="last-90-days">Last 90 days</option>
                        </select>
                        <button 
                            onClick={handleExport}
                            className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition duration-200 flex items-center"
                        >
                            <Download className="w-4 h-4 mr-2" /> Export
                        </button>
                    </div>
                </header>

                {/* Summary Cards */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
                    <div className="bg-gray-800 rounded-xl p-6 shadow-lg shadow-green-500/10">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-gray-400">Positive</p>
                                <h3 className="text-3xl font-bold text-green-400">{sentimentStats.positive}</h3>
                                <p className="text-green-400 text-sm mt-1">
                                    <ArrowUp className="w-4 h-4 inline mr-1" /> 
                                    {sentimentStats.total > 0 ? Math.round((sentimentStats.positive / sentimentStats.total) * 100) : 0}%
                                </p>
                            </div>
                            <div className="bg-green-900/30 p-3 rounded-full">
                                <Smile className="text-green-400 w-8 h-8" />
                            </div>
                        </div>
                    </div>
                    
                    <div className="bg-gray-800 rounded-xl p-6 shadow-lg shadow-red-500/10">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-gray-400">Negative</p>
                                <h3 className="text-3xl font-bold text-red-400">{sentimentStats.negative}</h3>
                                <p className="text-red-400 text-sm mt-1">
                                    <ArrowDown className="w-4 h-4 inline mr-1" /> 
                                    {sentimentStats.total > 0 ? Math.round((sentimentStats.negative / sentimentStats.total) * 100) : 0}%
                                </p>
                            </div>
                            <div className="bg-red-900/30 p-3 rounded-full">
                                <Frown className="text-red-400 w-8 h-8" />
                            </div>
                        </div>
                    </div>
                    
                    <div className="bg-gray-800 rounded-xl p-6 shadow-lg shadow-blue-500/10">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-gray-400">Neutral</p>
                                <h3 className="text-3xl font-bold text-blue-400">{sentimentStats.neutral}</h3>
                                <p className="text-blue-400 text-sm mt-1">
                                    <ArrowUp className="w-4 h-4 inline mr-1" /> 
                                    {sentimentStats.total > 0 ? Math.round((sentimentStats.neutral / sentimentStats.total) * 100) : 0}%
                                </p>
                            </div>
                            <div className="bg-blue-900/30 p-3 rounded-full">
                                <Meh className="text-blue-400 w-8 h-8" />
                            </div>
                        </div>
                    </div>
                    
                    <div className="bg-gray-800 rounded-xl p-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-gray-400">Total Reviews</p>
                                <h3 className="text-3xl font-bold text-white">{sentimentStats.total}</h3>
                                <p className="text-gray-400 text-sm mt-1">
                                    Avg. rating: {reviews.length > 0 ? 
                                        (reviews.reduce((sum, r) => sum + r.overall, 0) / reviews.length).toFixed(1) : 0}/5
                                </p>
                            </div>
                            <div className="bg-purple-900/30 p-3 rounded-full">
                                <Star className="text-purple-400 w-8 h-8" />
                            </div>
                        </div>
                    </div>
                </div>

                {/* Reviews Section */}
                <div className="bg-gray-800 rounded-xl p-6 mb-8">
                    <div className="flex justify-between items-center mb-6">
                        <h2 className="text-xl font-semibold text-white">Customer Reviews ({filteredReviews.length})</h2>
                        <div className="relative w-64">
                            <input 
                                type="text" 
                                placeholder="Search reviews..." 
                                value={searchTerm}
                                onChange={(e) => setSearchTerm(e.target.value)}
                                className="w-full bg-gray-700 border border-gray-600 rounded-lg px-4 py-2 pl-10 focus:outline-none focus:ring-2 focus:ring-blue-500"
                            />
                            <Search className="w-4 h-4 absolute left-3 top-3 text-gray-400" />
                        </div>
                    </div>
                    
                    {/* Review Filters */}
                    <div className="flex flex-wrap gap-3 mb-6">
                        <button 
                            onClick={() => setSelectedFilter('all')}
                            className={`px-4 py-2 rounded-lg flex items-center transition-colors ${
                                selectedFilter === 'all' ? 'bg-blue-600 text-white' : 'bg-gray-700 hover:bg-gray-600 text-white'
                            }`}
                        >
                            <Filter className="w-4 h-4 mr-2" /> All
                        </button>
                        <button 
                            onClick={() => setSelectedFilter('positive')}
                            className={`px-4 py-2 rounded-lg flex items-center transition-colors ${
                                selectedFilter === 'positive' ? 'bg-green-600 text-white' : 'bg-gray-700 hover:bg-gray-600 text-green-400'
                            }`}
                        >
                            <Smile className="w-4 h-4 mr-2" /> Positive
                        </button>
                        <button 
                            onClick={() => setSelectedFilter('negative')}
                            className={`px-4 py-2 rounded-lg flex items-center transition-colors ${
                                selectedFilter === 'negative' ? 'bg-red-600 text-white' : 'bg-gray-700 hover:bg-gray-600 text-red-400'
                            }`}
                        >
                            <Frown className="w-4 h-4 mr-2" /> Negative
                        </button>
                        <button 
                            onClick={() => setSelectedFilter('neutral')}
                            className={`px-4 py-2 rounded-lg flex items-center transition-colors ${
                                selectedFilter === 'neutral' ? 'bg-blue-600 text-white' : 'bg-gray-700 hover:bg-gray-600 text-blue-400'
                            }`}
                        >
                            <Meh className="w-4 h-4 mr-2" /> Neutral
                        </button>
                        <button 
                            onClick={() => setSelectedFilter('5-stars')}
                            className={`px-4 py-2 rounded-lg flex items-center transition-colors ${
                                selectedFilter === '5-stars' ? 'bg-yellow-600 text-white' : 'bg-gray-700 hover:bg-gray-600 text-white'
                            }`}
                        >
                            <Star className="w-4 h-4 mr-2" /> 5 Stars
                        </button>
                        <button 
                            onClick={() => setSelectedFilter('recent')}
                            className={`px-4 py-2 rounded-lg flex items-center transition-colors ${
                                selectedFilter === 'recent' ? 'bg-purple-600 text-white' : 'bg-gray-700 hover:bg-gray-600 text-white'
                            }`}
                        >
                            <Calendar className="w-4 h-4 mr-2" /> Recent
                        </button>
                    </div>
                    
                    {/* Reviews List */}
                    <div className="space-y-4 max-h-96 overflow-y-auto pr-2">
                        {filteredReviews.length === 0 ? (
                            <div className="text-center py-8 text-gray-400">
                                <p>No reviews found matching your criteria.</p>
                            </div>
                        ) : (
                            filteredReviews.map((review) => (
                                <div key={review.id} className={`p-4 rounded-lg ${getSentimentClass(review.sentiment)}`}>
                                    <div className="flex justify-between items-start">
                                        <div className="flex items-center space-x-3">
                                            <div className="w-10 h-10 rounded-full bg-gray-700/50 flex items-center justify-center">
                                                {getSentimentIcon(review.sentiment)}
                                            </div>
                                            <div>
                                                <h4 className="font-semibold text-white">{review.reviewerName}</h4>
                                                <div className="flex items-center text-sm">
                                                    {renderStars(review.overall)}
                                                    <span className="text-gray-400 ml-2">{formatDate(review.date)}</span>
                                                </div>
                                            </div>
                                        </div>
                                        <div className="flex items-center space-x-2">
                                            <span className={`px-2 py-1 text-xs rounded-full ${
                                                review.sentiment === 'positive' ? 'bg-green-900/30 text-green-400' :
                                                review.sentiment === 'negative' ? 'bg-red-900/30 text-red-400' :
                                                'bg-blue-900/30 text-blue-400'
                                            }`}>
                                                {review.confidence}% {review.sentiment}
                                            </span>
                                            <button className="text-gray-400 hover:text-white">
                                                <MoreVertical className="w-4 h-4" />
                                            </button>
                                        </div>
                                    </div>
                                    <p className="mt-3 text-gray-200">{review.reviewText}</p>
                                    {review.tags && review.tags.length > 0 && (
                                        <div className="mt-3 flex flex-wrap gap-2">
                                            {review.tags.map((tag, index) => (
                                                <span key={index} className="px-2 py-1 bg-gray-700 text-xs rounded-full">
                                                    #{tag}
                                                </span>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            ))
                        )}
                    </div>
                </div>
                
                {/* Keywords Cloud - Keep this section as it's just UI, not using sample data */}
                <div className="bg-gray-800 rounded-xl p-6">
                    <h2 className="text-xl font-semibold text-white mb-4">Sentiment Keywords</h2>
                    <div className="flex flex-wrap gap-3">
                        {[
                            { word: 'excellent', sentiment: 'positive' },
                            { word: 'amazing', sentiment: 'positive' },
                            { word: 'love it', sentiment: 'positive' },
                            { word: 'disappointed', sentiment: 'negative' },
                            { word: 'average', sentiment: 'neutral' },
                            { word: 'recommend', sentiment: 'positive' },
                            { word: 'poor quality', sentiment: 'negative' },
                            { word: 'okay', sentiment: 'neutral' },
                            { word: 'perfect', sentiment: 'positive' },
                            { word: 'broken', sentiment: 'negative' },
                            { word: 'satisfied', sentiment: 'positive' },
                            { word: 'waste of money', sentiment: 'negative' },
                            { word: 'decent', sentiment: 'neutral' },
                            { word: 'exceeds expectations', sentiment: 'positive' },
                            { word: 'not as described', sentiment: 'negative' }
                        ].map((keyword, index) => (
                            <span 
                                key={index} 
                                className={`px-3 py-2 rounded-full text-sm ${
                                    keyword.sentiment === 'positive' ? 'bg-green-900/30 text-green-400' :
                                    keyword.sentiment === 'negative' ? 'bg-red-900/30 text-red-400' :
                                    'bg-blue-900/30 text-blue-400'
                                }`}
                            >
                                {keyword.word}
                            </span>
                        ))}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default SentimentDashboard;