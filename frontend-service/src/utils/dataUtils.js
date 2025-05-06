import { DateTime } from 'luxon';

export const generateSampleReview = () => {
    const sentiments = ['positive', 'neutral', 'negative'];
    const sampleASINs = ['B00004Y2UT', 'B00005N5PF', 'B00008W5L3', 'B00002N6K1', 'B00004TUC8'];
    
    const positivePhrases = [
      "This product is amazing! It exceeded all my expectations.",
      "Highly recommend this item. Works perfectly and looks great.",
      "Best purchase I've made in a long time. Very satisfied!"
    ];
    
    const neutralPhrases = [
      "The product is okay. Does what it's supposed to do.",
      "It's fine, but nothing special. I expected a bit more.",
      "Average quality. Not bad, but not great either."
    ];
    
    const negativePhrases = [
      "Very disappointed with this purchase. Broke after a week.",
      "Poor quality and not worth the price. Would not recommend.",
      "Did not meet expectations at all. Returning this item."
    ];
    
    const sentiment = sentiments[Math.floor(Math.random() * sentiments.length)];
    const asin = sampleASINs[Math.floor(Math.random() * sampleASINs.length)];
    
    let reviewText;
    if (sentiment === 'positive') {
      reviewText = positivePhrases[Math.floor(Math.random() * positivePhrases.length)];
    } else if (sentiment === 'neutral') {
      reviewText = neutralPhrases[Math.floor(Math.random() * neutralPhrases.length)];
    } else {
      reviewText = negativePhrases[Math.floor(Math.random() * negativePhrases.length)];
    }
    
    const confidence = (Math.random() * 0.5 + 0.5).toFixed(2);
    
    return {
      id: Date.now(),
      asin,
      reviewText,
      sentiment,
      confidence,
      timestamp: new Date().toISOString()
    };
  };
  
  export const generateTimeSeriesData = (days, min, max) => {
    const data = [];
    const now = DateTime.now();
    
    for (let i = days; i >= 0; i--) {
      const date = now.minus({ days: i });
      data.push({
        x: date.toJSDate(),
        y: Math.floor(Math.random() * (max - min + 1)) + min
      });
    }
    
    return data;
  };
  
  export const sampleASINs = ['B00004Y2UT', 'B00005N5PF', 'B00008W5L3', 'B00002N6K1', 'B00004TUC8'];
  