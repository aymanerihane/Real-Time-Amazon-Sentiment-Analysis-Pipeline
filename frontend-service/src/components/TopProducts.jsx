import { useEffect, useState } from 'react';
import { connectWebSocket, addMessageHandler, removeMessageHandler } from '../utils/dataUtils';

export default function TopProducts() {
  const [products, setProducts] = useState({});

  useEffect(() => {
    // Connect to WebSocket
    connectWebSocket();

    // Handle incoming messages
    const handleMessage = (message) => {
      if (message.type === 'new_sentiment' || message.type === 'new_review') {
        const data = message.data || message;
        if (!data || !data.asin) return;
        
        setProducts(prevProducts => {
          const newProducts = { ...prevProducts };
          const asin = data.asin;
          
          if (!newProducts[asin]) {
            newProducts[asin] = {
              asin: asin,
              title: data.title || data.summary || `Product ${asin}`,
              positive: 0,
              neutral: 0,
              negative: 0
            };
          }
          
          // Determine sentiment count to increment
          let sentimentType = 'neutral';
          if (data.sentiment === 2 || data.sentiment === 'positive' || data.sentiment_label === 'Positive') {
            sentimentType = 'positive';
          } else if (data.sentiment === 0 || data.sentiment === 'negative' || data.sentiment_label === 'Negative') {
            sentimentType = 'negative';
          }
          
          newProducts[asin][sentimentType]++;
          return newProducts;
        });
      }
    };

    addMessageHandler(handleMessage);

    return () => {
      removeMessageHandler(handleMessage);
    };
  }, []);

  // Calculate top products by total reviews and positive percentage
  const topProducts = Object.entries(products)
    .map(([asin, data]) => {
      const total = data.positive + data.neutral + data.negative;
      const positivePercent = total > 0 ? Math.round((data.positive / total) * 100) : 0;
      
      return {
        asin,
        title: data.title || `Product ${asin}`,
        positivePercent,
        total
      };
    })
    .sort((a, b) => b.total - a.total)
    .slice(0, 5);

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