import { useEffect, useState } from 'react';
import { connectWebSocket, addMessageHandler, removeMessageHandler, globalDataStore } from '../utils/dataUtils';

export default function TopProducts() {
  const [topProducts, setTopProducts] = useState([]);

  useEffect(() => {
    // Connect to WebSocket
    connectWebSocket();

    // Initial load from global store
    updateTopProductsFromGlobalStore();

    // Handle incoming messages
    const handleMessage = (message) => {
      if (message.type === 'new_sentiment' || message.type === 'new_review') {
        // Update from global store when new data arrives
        updateTopProductsFromGlobalStore();
      }
    };

    addMessageHandler(handleMessage);

    return () => {
      removeMessageHandler(handleMessage);
    };
  }, []);

  // Function to update top products from global store
  const updateTopProductsFromGlobalStore = () => {
    // Map the global store data to our display format
    const formattedTopProducts = Object.entries(globalDataStore.asinReviewData)
      .map(([asin, data]) => {
        const total = Object.values(data.sentimentCounts).reduce((sum, count) => sum + count, 0);
        const positivePercent = total > 0 ? Math.round((data.sentimentCounts.positive / total) * 100) : 0;
        
        return {
          asin,
          title: data.title || `Product ${asin}`,
          positivePercent,
          total
        };
      })
      .sort((a, b) => b.total - a.total)
      .slice(0, 5);
    
    setTopProducts(formattedTopProducts);
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 shadow-sm">
      <h3 className="font-medium text-gray-700 dark:text-gray-300 mb-3">Top Products</h3>
      <div className="space-y-2">
        {topProducts.length === 0 ? (
          <div className="text-sm text-gray-500 dark:text-gray-400">No product data available yet</div>
        ) : (
          topProducts.map(product => (
            <div key={product.asin} className="flex justify-between items-center">
              <div className="text-sm font-medium dark:text-gray-300 truncate max-w-[200px]" title={`${product.asin}: ${product.title}`}>
                {product.title}
              </div>
              <div className="flex items-center space-x-2">
                <span className="text-xs text-green-500">{product.positivePercent}%</span>
                <div className="w-20 bg-gray-200 dark:bg-gray-600 rounded-full h-1.5">
                  <div 
                    className="bg-green-500 h-1.5 rounded-full" 
                    style={{ width: `${product.positivePercent}%` }}
                  ></div>
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}