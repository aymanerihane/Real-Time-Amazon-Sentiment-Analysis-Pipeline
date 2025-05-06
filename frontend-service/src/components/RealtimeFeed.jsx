import { useState, useEffect } from 'react';
import PieChart from './charts/PieChart';
import ReviewList from './ReviewList';
import SentimentStats from './SentimentStats';
import TopProducts from './TopProducts';
import FeedControls from './FeedControls';

export default function RealtimeFeed({ 
  reviews, 
  filteredReviews, 
  addReviewToFeed, 
  isPaused, 
  setIsPaused, 
  asinFilter, 
  setAsinFilter,
  positiveCount,
  neutralCount,
  negativeCount
}) {
  return (
    <div className="container mx-auto px-4 py-6">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-xl font-semibold text-gray-800 dark:text-gray-200">
          Live Sentiment Analysis
        </h2>
        <FeedControls 
          isPaused={isPaused} 
          setIsPaused={setIsPaused} 
          asinFilter={asinFilter} 
          setAsinFilter={setAsinFilter}
        />
      </div>
      
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2">
          <ReviewList reviews={filteredReviews} />
        </div>
        
        <div className="space-y-6">
          <PieChart positiveCount={positiveCount} neutralCount={neutralCount} negativeCount={negativeCount} />
          <SentimentStats 
            positiveCount={positiveCount}
            neutralCount={neutralCount}
            negativeCount={negativeCount}
          />
          <TopProducts />
        </div>
      </div>
    </div>
  );
}