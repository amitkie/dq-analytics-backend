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
const Category = require('../models/category')(sequelize, DataTypes);

// Define the categories to seed with createdAt and updatedAt
const categoriesToSeed = [
  // { name: 'Beauty', createdAt: new Date(), updatedAt: new Date() },
  // { name: 'Foods', createdAt: new Date(), updatedAt: new Date() },
  // { name: 'Hair Care', createdAt: new Date(), updatedAt: new Date() },
  // { name: 'Male Grooming', createdAt: new Date(), updatedAt: new Date() },
];

// Seed function
const seedCategories = async () => {
  try {
    // Sync the Category model with the database (if not already synced)
    await sequelize.sync();

    // Seed categories
    await Promise.all(categoriesToSeed.map(category => Category.create(category)));

    console.log('Categories have been seeded successfully.');
  } catch (error) {
    console.error('Error seeding categories:', error);
  } finally {
    // Close the database connection
    await sequelize.close();
  }
};

// Run the seeder function
seedCategories();
