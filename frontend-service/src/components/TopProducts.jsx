import { useEffect, useState } from 'react';
import { connectWebSocket, addMessageHandler, removeMessageHandler } from '../utils/dataUtils';

export default function TopProducts() {
  const [products, setProducts] = useState({});

  useEffect(() => {
    // Connect to WebSocket
    connectWebSocket();

    // Handle incoming messages
    const handleMessage = (message) => {
      if (message.type === 'new_sentiment') {
        setProducts(prevProducts => {
          const newProducts = { ...prevProducts };
          const asin = message.data.asin;
          
          if (!newProducts[asin]) {
            newProducts[asin] = {
              title: message.data.title,
              positive: 0,
              neutral: 0,
              negative: 0
            };
          }
          
          newProducts[asin][message.data.sentiment]++;
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
        title: data.title,
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
        {topProducts.map(product => (
          <div key={product.asin} className="flex justify-between items-center">
            <div className="text-sm font-medium dark:text-gray-300 truncate max-w-[200px]" title={product.title}>
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
        ))}
      </div>
    </div>
  );
}