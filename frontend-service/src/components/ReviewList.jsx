export default function ReviewList({ reviews }) {
    return (
      <div className="bg-gray-50 dark:bg-gray-700 rounded-lg border border-gray-200 dark:border-gray-600 h-[600px] overflow-y-auto custom-scrollbar p-4">
        <div className="space-y-4">
          {reviews.length === 0 ? (
            <div className="text-center py-10 text-gray-500 dark:text-gray-400">
              <i className="fas fa-comment-alt fa-3x mb-4 text-gray-300 dark:text-gray-500"></i>
              <p>Waiting for incoming reviews...</p>
            </div>
          ) : (
            reviews.map((review, index) => (
              <ReviewItem key={index + '-' + (review.asin || '') + '-' + Date.now()} review={review} />
            ))
          )}
        </div>
      </div>
    );
  }
  
  function ReviewItem({ review }) {
    const sentimentClasses = {
      positive: 'bg-green-50 dark:bg-green-900/30',
      neutral: 'bg-blue-50 dark:bg-blue-900/30',
      negative: 'bg-red-50 dark:bg-red-900/30'
    };
  
    const sentimentIcons = {
      positive: 'fa-smile text-green-500',
      neutral: 'fa-meh text-blue-500',
      negative: 'fa-frown text-red-500'
    };
  
    const sentimentLabels = {
      positive: 'Positive',
      neutral: 'Neutral',
      negative: 'Negative'
    };

    // Safely format date
    const formatDate = (dateString) => {
      if (!dateString) return 'Just now';
      const date = new Date(dateString);
      return date instanceof Date && !isNaN(date.getTime()) ? 
        date.toLocaleTimeString() : 'Just now';
    };
    
    // Safe percentage calculation
    const getConfidenceDisplay = () => {
      if (!review.confidence && review.confidence !== 0) {
        return '';
      }
      
      const confidenceValue = parseFloat(review.confidence);
      if (isNaN(confidenceValue)) {
        return '';
      }
      
      return ` (${(confidenceValue * 100).toFixed(0)}%)`;
    };
  
    return (
      <div className={`p-4 rounded-lg shadow-sm ${sentimentClasses[review.sentiment] || 'bg-gray-50 dark:bg-gray-800'}`}>
        <div className="flex justify-between items-start mb-2">
          <div className="flex items-center space-x-2">
            <span className="bg-white dark:bg-gray-700 p-1 rounded-full shadow">
              <i className={`fas ${sentimentIcons[review.sentiment] || 'fa-comment text-gray-500'}`}></i>
            </span>
            <span className="font-medium dark:text-gray-200">
              {sentimentLabels[review.sentiment] || 'Unknown'}
              {getConfidenceDisplay()}
            </span>
          </div>
          <div className="text-sm text-gray-500 dark:text-gray-400">
            {formatDate(review.date || review.processedAt)}
          </div>
        </div>
        <div className="mb-2">
          <span className="bg-gray-100 dark:bg-gray-600 text-gray-800 dark:text-gray-200 text-xs px-2 py-1 rounded">
            ASIN: {review.asin || 'N/A'}
          </span>
        </div>
        <p className="text-gray-700 dark:text-gray-300">"{review.reviewText || 'No text provided'}"</p>
      </div>
    );
  }