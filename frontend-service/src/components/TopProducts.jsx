import { sampleASINs } from '../utils/dataUtils';

export default function TopProducts() {
  const topProducts = sampleASINs.map(asin => {
    const positive = Math.floor(Math.random() * 50);
    const neutral = Math.floor(Math.random() * 30);
    const negative = Math.floor(Math.random() * 20);
    const total = positive + neutral + negative;
    const positivePercent = total > 0 ? Math.round((positive / total) * 100) : 0;
    
    return {
      asin,
      positivePercent
    };
  });

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 shadow-sm">
      <h3 className="font-medium text-gray-700 dark:text-gray-300 mb-3">Top Products</h3>
      <div className="space-y-2">
        {topProducts.map(product => (
          <div key={product.asin} className="flex justify-between items-center">
            <div className="text-sm font-medium dark:text-gray-300">{product.asin}</div>
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