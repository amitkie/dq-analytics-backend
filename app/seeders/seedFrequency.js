// seeders/seed-categories.js

const { Sequelize, DataTypes } = require('sequelize');
const dbConfig = require("../../config/db.js");

const sequelize = new Sequelize(dbConfig.DB, dbConfig.USER, dbConfig.PASSWORD, {
  host: dbConfig.HOST,
  dialect: dbConfig.dialect,
  operatorsAliases: false,

  pool: {
    max: dbConfig.pool.max,
    min: dbConfig.pool.min,
    acquire: dbConfig.pool.acquire,
    idle: dbConfig.pool.idle
  }
});
// Import the Category model
const Category = require('../models/frequency')(sequelize, DataTypes);

// Define the categories to seed with createdAt and updatedAt
const frequencyToSeed = [
  { name: 'Monthly', createdAt: new Date(), updatedAt: new Date() },
  { name: 'Quarterly', createdAt: new Date(), updatedAt: new Date() },
];

// Seed function
const seedFrequency = async () => {
  try {
    // Sync the Category model with the database (if not already synced)
    await sequelize.sync();

    // Seed frequency
    await Promise.all(frequencyToSeed.map(category => Category.create(category)));

    console.log('Frequency have been seeded successfully.');
  } catch (error) {
    console.error('Error seeding frequency:', error);
  } finally {
    // Close the database connection
    await sequelize.close();
  }
};

// Run the seeder function
seedFrequency();
