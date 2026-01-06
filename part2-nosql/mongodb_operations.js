/**
 * mongodb_operations.js Version
 */

const { MongoClient } = require("mongodb");
const fs = require("fs");

// ===== CONFIG =====
const mongoUrl = "mongodb://localhost:27017";
const dbName = "fleximart";
const collectionName = "products_catalog";

// ===== LOAD JSON DATA =====
const data = JSON.parse(
  fs.readFileSync("products_catalog.json", "utf-8")
);

// ===== MAIN FUNCTION =====
async function run() {
  const client = new MongoClient(mongoUrl);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const col = db.collection(collectionName);

    // ===== Operation 1: Load Data =====
    // Import the provided JSON file into collection 'products'
    await col.deleteMany({});
    const insertResult = await col.insertMany(data);
    console.log(
      `Inserted ${insertResult.insertedCount} documents into ${dbName}.${collectionName}`
    );

    // ===== Operation 2: Basic Query =====
    // Find Electronics products with price < 50000
    const electronics = await col
      .find(
        { category: "Electronics", price: { $lt: 50000 } },
        { projection: { _id: 0, name: 1, price: 1, stock: 1 } }
      )
      .toArray();

    console.log("\nOperation 2 Result:");
    console.table(electronics);

    // ===== Operation 3: Review Analysis =====
    // Find all products that have average rating >= 4.0
    const pipeline3 = [
      {
        $addFields: {
          avg_rating: {
            $avg: { $ifNull: ["$reviews.rating", []] }
          }
        }
      },
      { $match: { avg_rating: { $gte: 4.0 } } },
      {
        $project: {
          _id: 0,
          product_id: 1,
          name: 1,
          category: 1,
          avg_rating: { $round: ["$avg_rating", 2] }
        }
      }
    ];

    const highRated = await col.aggregate(pipeline3).toArray();
    console.log("\nOperation 3 Result:");
    console.table(highRated);

    // ===== Operation 4: Update Operation =====
    // Add a new review to product "ELEC001"
    const newReview = {
      user: "U999",
      rating: 4,
      comment: "Good value",
      date: new Date() // ISODate equivalent
    };

    const updateResult = await col.updateOne(
      { product_id: "ELEC001" },
      { $push: { reviews: newReview } }
    );

    console.log("\nOperation 4 Result:", {
      matched: updateResult.matchedCount,
      modified: updateResult.modifiedCount
    });

    const updatedProduct = await col.findOne(
      { product_id: "ELEC001" },
      { projection: { _id: 0, product_id: 1, name: 1, reviews: 1 } }
    );

    console.log("\nUpdated Product:");
    console.dir(updatedProduct, { depth: null });

    // ===== Operation 5: Complex Aggregation =====
    // Calculate average price by category
    const pipeline5 = [
      {
        $group: {
          _id: "$category",
          avg_price: { $avg: "$price" },
          product_count: { $sum: 1 }
        }
      },
      {
        $project: {
          _id: 0,
          category: "$_id",
          avg_price: { $round: ["$avg_price", 2] },
          product_count: 1
        }
      },
      { $sort: { avg_price: -1 } }
    ];

    const categoryStats = await col.aggregate(pipeline5).toArray();
    console.log("\nOperation 5 Result:");
    console.table(categoryStats);

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
    console.log("\nMongoDB connection closed");
  }
}

// ===== RUN =====
run();
