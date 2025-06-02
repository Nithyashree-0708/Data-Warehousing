// MongoDB: Campaign Feedback & Indexing
// 1. Insert Sample Campaign Feedback

use retailsalesdashboard

db.campaign_feedback.insertMany([
  {
    campaign_id: "CAMP001",
    store_id: 1,
    product_id: 1,
    feedback: "Increased foot traffic but low conversion.",
    rating: 3,
    date: ISODate("2025-06-01")
  },
  {
    campaign_id: "CAMP002",
    store_id: 2,
    product_id: 2,
    feedback: "Great customer response, especially for discounts.",
    rating: 5,
    date: ISODate("2025-06-01")
  }
]);



// 2. Indexing for Product and Region Search


db.campaign_feedback.createIndex({ product_id: 1 });

db.stores.createIndex({ region: 1 });
