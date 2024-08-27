// // // seeders/seed-section-platform-metrics.js

// // const { Sequelize, DataTypes } = require('sequelize');
// // const dbConfig = require('../../config/db'); // Adjust the path as per your project structure
// // const { v4: uuidv4 } = require('uuid');

// // // Define your data to seed
// // const dataToSeed = [
// //   { section: 'Ecom', platform: 'Amazon', metrics: [
// //     'Content Score', 'Average ratings', 'Reviews', 'Net sentiment of reviews', 'Availability%'
// //   ]},
// //   { section: 'Ecom', platform: 'Amazon - Search Campaigns', metrics: [
// //     'Search – Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
// //     'Order Conversion Rate', 'Click to new purchase', '% of new purchase rate',
// //     'Sales value', 'unit sold', 'Cost per order (new)', 'ACOS %'
// //   ]},
// //   { section: 'Ecom', platform: 'Amazon - Display Campaigns', metrics: [
// //     'Display – Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'DPV', 'DPVR', 'ATC',
// //     'ATCR', 'Purchases', 'Order Conversion Rate', 'Click to new purchase', '% of new purchase rate',
// //     'Sales value', 'unit sold', 'Cost per new customer (CAC)', 'ACOS %'
// //   ]},
// //   { section: 'Ecom', platform: 'Flipkart PLA Campaigns', metrics: [
// //     'Search - Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
// //     'Order Conversion Rate', 'Sales value', 'unit sold', 'ACOS %'
// //   ]},
// //   { section: 'Ecom', platform: 'Flipkart PCA Campaigns', metrics: [
// //     'Search - Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
// //     'Order Conversion Rate', 'Sales value', 'unit sold', 'ACOS %'
// //   ]},
// //   { section: 'Ecom', platform: 'Big Basket Campaigns', metrics: [
// //     'Search - Spends', 'Clicks', 'CPC', 'Purchases', 'Order Conversion Rate',
// //     'Sales value', 'unit sold', 'ACOS %'
// //   ]},
// //   { section: 'Ecom', platform: 'Blinkit Campaigns', metrics: [
// //     'Search - Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
// //     'Order Conversion Rate', 'Click to new purchase', '% of new purchase rate',
// //     'Sales value', 'unit sold', 'Cost per order (new)', 'ACOS %'
// //   ]},
// //   { section: 'Ecom', platform: 'Nykaa Campaigns', metrics: [
// //     'Search - home page banners', 'Impressions', 'CPM', 'Clicks', 'CTR', 'Order',
// //     'Order Conversion Rate', 'ACOS %(to be calculated )'
// //   ]},
// //   { section: 'Ecom', platform: 'Myntraa Campaigns', metrics: [
// //     'Search - home page banners', 'Impressions', 'CPM', 'Clicks', 'CTR', 'Order',
// //     'Order Conversion Rate', 'ACOS %(to be calculated )'
// //   ]},
// //   { section: 'Ecom', platform: 'Amazon', metrics: [
// //     'Search visibility share (Organic)', 'Search visibility share (Paid)', 'Amazon Best seller rank'
// //   ]},
// //   { section: 'Social', platform: 'SEO', metrics: [
// //     'Organic rank'
// //   ]},

// //   // This needs to be modified create individual records for each
// //   { section: 'Social', platform: 'Facebook', metrics: [
// //     'Net sentiment', 'Mentions', 'Engagement', 'Engagement %'
// //   ]},
// //   { section: 'Social', platform: 'Twitter', metrics: [
// //     'Net sentiment', 'Mentions', 'Engagement', 'Engagement %'
// //   ]},
// //   { section: 'Social', platform: 'Instagram', metrics: [
// //     'Net sentiment', 'Mentions', 'Engagement', 'Engagement %'
// //   ]},
// //   { section: 'Paid', platform: 'Gadwords', metrics: [
// //     'Spends', 'Impressions', 'Reach', 'Frequency', 'Clicks', 'VTR', 'CPM', 'Add to cart',
// //     'Click to cart %', 'Cart to checkout', 'Transactions', 'Transaction rate', 'Cost per transaction',
// //     'AOV', 'ACOS %'
// //   ]},
// //   { section: 'Paid', platform: 'Facebook', metrics: [
// //     'Spends', 'Impressions', 'Reach', 'Frequency', 'Clicks', 'VTR', 'CPM', 'Add to cart',
// //     'Click to cart %', 'Cart to checkout', 'Transactions', 'Transaction rate', 'Cost per transaction',
// //     'AOV', 'ACOS %'
// //   ]},
// //   { section: 'Paid', platform: 'DV360', metrics: [
// //     'Spends', 'Impressions', 'Reach', 'Frequency', 'Clicks', 'VTR', 'CPM', 'Add to cart',
// //     'Click to cart %', 'Cart to checkout', 'Transactions', 'Transaction rate', 'Cost per transaction',
// //     'AOV', 'ACOS %'
// //   ]},
// //   { section: 'Brand Perf', platform: 'Google Analytics', metrics: [
// //     'Unique Visitors', 'Sessions', 'Load Time (seconds)', 'Pages per sessions', 'Avg. Session Duration (mins)',
// //     'Product Views', 'Add to Basket', 'Checkout', 'Product views per session', 'Sessions to product views %',
// //     'Product views to cart %', 'Cart to Checkout %', 'Check out to Transaction %', 'Overall conversion %',
// //     'AOV', 'ACOS %', 'Repeat rate %', 'CAC'
// //   ]},
// //   { section: 'Brand Perf', platform: 'Page Speed Insights', metrics: [
// //     'Mobile page speed insights score', 'Web page speed insights score', 'Largest contentful paint (LCP) - seconds',
// //     'First input delay (FID) - milli seconds', 'Cumulative layout shift (CLS)', 'First contentful paint (FCP) - seconds',
// //     'Time to interact (TTI)- seconds', 'Speed Index - seconds', 'Total blocking time (TBT) - milli seconds'
// //   ]},
// //   { section: 'Brand Perf', platform: 'SEOptimer', metrics: [
// //     'Usability (SEO optimer)', 'Performance (SEO optimer)', 'Social (SEO optimer)', 'SEO (SEO optimer)'
// //   ]}
// // ];

// // // Function to seed data
// // const seedSectionPlatformMetrics = async () => {
// //   // Initialize Sequelize
// //   const sequelize = new Sequelize(dbConfig.DB, dbConfig.USER, dbConfig.PASSWORD, {
// //     host: dbConfig.HOST,
// //     dialect: dbConfig.dialect,
// //     operatorsAliases: false,
// //     pool: {
// //       max: dbConfig.pool.max,
// //       min: dbConfig.pool.min,
// //       acquire: dbConfig.pool.acquire,
// //       idle: dbConfig.pool.idle
// //     }
// //   });

// //   try {
// //     // Define models
// //     const Section = require('../models/section')(sequelize, DataTypes);
// //     const Platform = require('../models/platform')(sequelize, DataTypes);
// //     const Metrics = require('../models/metrics')(sequelize, DataTypes);
// //     // const SectionPlatformMetrics = require('../models/sectionPlatformMetrics')(sequelize, DataTypes);

// //     // Sync models with database
// //     await sequelize.sync();

// //     // Seed data
// //     for (const data of dataToSeed) {
// //       // Find or create section
// //       let section = await Section.findOne({ where: { name: data.section } });
// //       if (!section) {
// //         section = await Section.create({ name: data.section });
// //       }

// //       // Find or create platform
// //       let platform = await Platform.findOne({ where: { name: data.platform } });
// //       if (!platform) {
// //         platform = await Platform.create({ name: data.platform , section_id:section.id});
// //       }

// //       // Seed metrics for the platform
// //       await Promise.all(data.metrics.map(async metricName => {
// //         let metric = await Metrics.findOne({ where: { name: metricName } });
// //         if (!metric) {
// //           metric = await Metrics.create({ name: metricName, platform_id:platform.id  });
// //         }

// //         // Link section, platform, and metrics
// //       }));

// //       console.log(`Section '${section.name}', Platform '${platform.name}' and Metrics seeded successfully.`);
// //     }

// //     console.log('All data seeded successfully.');
// //   } catch (error) {
// //     console.error('Error seeding data:', error);
// //   } finally {
// //     // Close Sequelize connection
// //     await sequelize.close();
// //   }
// // };

// // // Run the seeding function
// // seedSectionPlatformMetrics();

// const { Sequelize, DataTypes } = require('sequelize');
// const dbConfig = require('../../config/db'); // Adjust the path as per your project structure

// // Define your data to seed
// const dataToSeed = [
//   { section: 'Ecom', platform: 'Amazon', metrics: [
//     'Content Score', 'Average ratings', 'Reviews', 'Net sentiment of reviews', 'Availability%'
//   ]},
//   { section: 'Ecom', platform: 'Amazon - Search Campaigns', metrics: [
//     'Search – Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
//     'Order Conversion Rate', 'Click to new purchase', '% of new purchase rate',
//     'Sales value', 'unit sold', 'Cost per order (new)', 'ACOS %'
//   ]},
//   { section: 'Ecom', platform: 'Amazon - Display Campaigns', metrics: [
//     'Display – Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'DPV', 'DPVR', 'ATC',
//     'ATCR', 'Purchases', 'Order Conversion Rate', 'Click to new purchase', '% of new purchase rate',
//     'Sales value', 'unit sold', 'Cost per new customer (CAC)', 'ACOS %'
//   ]},
//   { section: 'Ecom', platform: 'Flipkart PLA Campaigns', metrics: [
//     'Search - Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
//     'Order Conversion Rate', 'Sales value', 'unit sold', 'ACOS %'
//   ]},
//   { section: 'Ecom', platform: 'Flipkart PCA Campaigns', metrics: [
//     'Search - Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
//     'Order Conversion Rate', 'Sales value', 'unit sold', 'ACOS %'
//   ]},
//   { section: 'Ecom', platform: 'Big Basket Campaigns', metrics: [
//     'Search - Spends', 'Clicks', 'CPC', 'Purchases', 'Order Conversion Rate',
//     'Sales value', 'unit sold', 'ACOS %'
//   ]},
//   { section: 'Ecom', platform: 'Blinkit Campaigns', metrics: [
//     'Search - Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
//     'Order Conversion Rate', 'Click to new purchase', '% of new purchase rate',
//     'Sales value', 'unit sold', 'Cost per order (new)', 'ACOS %'
//   ]},
//   { section: 'Ecom', platform: 'Nykaa Campaigns', metrics: [
//     'Search - home page banners', 'Impressions', 'CPM', 'Clicks', 'CTR', 'Order',
//     'Order Conversion Rate', 'ACOS %(to be calculated )'
//   ]},
//   { section: 'Ecom', platform: 'Myntraa Campaigns', metrics: [
//     'Search - home page banners', 'Impressions', 'CPM', 'Clicks', 'CTR', 'Order',
//     'Order Conversion Rate', 'ACOS %(to be calculated )'
//   ]},
//   { section: 'Ecom', platform: 'Amazon', metrics: [
//     'Search visibility share (Organic)', 'Search visibility share (Paid)', 'Amazon Best seller rank'
//   ]},
//   { section: 'Social', platform: 'SEO', metrics: [
//     'Organic rank'
//   ]},
//   { section: 'Social', platform: 'Facebook', metrics: [
//     'Net sentiment', 'Mentions', 'Engagement', 'Engagement %'
//   ]},
//   { section: 'Social', platform: 'Twitter', metrics: [
//     'Net sentiment', 'Mentions', 'Engagement', 'Engagement %'
//   ]},
//   { section: 'Social', platform: 'Instagram', metrics: [
//     'Net sentiment', 'Mentions', 'Engagement', 'Engagement %'
//   ]},
//   { section: 'Paid', platform: 'Gadwords', metrics: [
//     'Spends', 'Impressions', 'Reach', 'Frequency', 'Clicks', 'VTR', 'CPM', 'Add to cart',
//     'Click to cart %', 'Cart to checkout', 'Transactions', 'Transaction rate', 'Cost per transaction',
//     'AOV', 'ACOS %'
//   ]},
//   { section: 'Paid', platform: 'Facebook', metrics: [
//     'Spends', 'Impressions', 'Reach', 'Frequency', 'Clicks', 'VTR', 'CPM', 'Add to cart',
//     'Click to cart %', 'Cart to checkout', 'Transactions', 'Transaction rate', 'Cost per transaction',
//     'AOV', 'ACOS %'
//   ]},
//   { section: 'Paid', platform: 'DV360', metrics: [
//     'Spends', 'Impressions', 'Reach', 'Frequency', 'Clicks', 'VTR', 'CPM', 'Add to cart',
//     'Click to cart %', 'Cart to checkout', 'Transactions', 'Transaction rate', 'Cost per transaction',
//     'AOV', 'ACOS %'
//   ]},
//   { section: 'Brand Perf', platform: 'Google Analytics', metrics: [
//     'Unique Visitors', 'Sessions', 'Load Time (seconds)', 'Pages per sessions', 'Avg. Session Duration (mins)',
//     'Product Views', 'Add to Basket', 'Checkout', 'Product views per session', 'Sessions to product views %',
//     'Product views to cart %', 'Cart to Checkout %', 'Check out to Transaction %', 'Overall conversion %',
//     'AOV', 'ACOS %', 'Repeat rate %', 'CAC'
//   ]},
//   { section: 'Brand Perf', platform: 'Page Speed Insights', metrics: [
//     'Mobile page speed insights score', 'Web page speed insights score', 'Largest contentful paint (LCP) - seconds',
//     'First input delay (FID) - milli seconds', 'Cumulative layout shift (CLS)', 'First contentful paint (FCP) - seconds',
//     'Time to interact (TTI)- seconds', 'Speed Index - seconds', 'Total blocking time (TBT) - milli seconds'
//   ]},
//   { section: 'Brand Perf', platform: 'SEOptimer', metrics: [
//     'Usability (SEO optimer)', 'Performance (SEO optimer)', 'Social (SEO optimer)', 'SEO (SEO optimer)'
//   ]}
// ];

// // Function to seed data
// const seedSectionPlatformMetrics = async () => {
//   // Initialize Sequelize
//   const sequelize = new Sequelize(dbConfig.DB, dbConfig.USER, dbConfig.PASSWORD, {
//     host: dbConfig.HOST,
//     dialect: dbConfig.dialect,
//     operatorsAliases: false,
//     pool: {
//       max: dbConfig.pool.max,
//       min: dbConfig.pool.min,
//       acquire: dbConfig.pool.acquire,
//       idle: dbConfig.pool.idle
//     }
//   });

//   try {
//     // Define models
//     const Section = require('../models/section')(sequelize, DataTypes);
//     const Platform = require('../models/platform')(sequelize, DataTypes);
//     const Metric = require('../models/metrics')(sequelize, DataTypes);

//     // Sync models with database
//     await sequelize.sync();

//     // Seed data
//     for (const data of dataToSeed) {
//       // Find or create section
//       let section = await Section.findOne({ where: { name: data.section } });
//       if (!section) {
//         section = await Section.create({ name: data.section });
//       }

//       // Find or create platform
//       let platform = await Platform.findOne({ where: { name: data.platform } });
//       if (!platform) {
//         platform = await Platform.create({ name: data.platform, section_id: section.id });
//       } else {
//         // Update platform with the section_id if it already exists
//         await platform.update({ section_id: section.id });
//       }

//       // Seed metrics for the platform
//       for (const metricName of data.metrics) {
//         let metric = await Metric.findOne({ where: { name: metricName } });
//         if (!metric) {
//           await Metric.create({ name: metricName, platform_id: platform.id, section_id: section.id });
//         } else {
//           // Update existing metrics to ensure platform_id and section_id are set
//           await metric.update({ platform_id: platform.id, section_id: section.id });
//         }
//       }

//       console.log(`Section '${section.name}', Platform '${platform.name}' and Metrics seeded successfully.`);
//     }

//     console.log('All data seeded successfully.');
//   } catch (error) {
//     console.error('Error seeding data:', error);
//   } finally {
//     // Close Sequelize connection
//     await sequelize.close();
//   }
// };

// // Run the seeding function
// seedSectionPlatformMetrics();

const { Sequelize, DataTypes } = require("sequelize");
const dbConfig = require("../../config/db"); // Adjust the path to your db configuration

// Initialize Sequelize
const sequelize = new Sequelize(dbConfig.DB, dbConfig.USER, dbConfig.PASSWORD, {
  host: dbConfig.HOST,
  dialect: dbConfig.dialect,
  operatorsAliases: false,
  port: dbConfig.PORT,
  pool: {
    max: dbConfig.pool.max,
    min: dbConfig.pool.min,
    acquire: dbConfig.pool.acquire,
    idle: dbConfig.pool.idle,
  },
  dialectOptions: {
    ssl: {
      require: true,
      rejectUnauthorized: false, // Set to true for production
    },
  },
});

// Import models
const Section = require("../models/section")(sequelize, DataTypes);
const Platform = require("../models/platform")(sequelize, DataTypes);
const Metrics = require("../models/metrics")(sequelize, DataTypes);

// Function to insert data
const insertData = async () => {
  try {
    // Sync models with the database
    await sequelize.sync();

    // Insert Platforms
    const platforms = [
      { id: 1, name: "Amazon" },
      { id: 2, name: "Amazon - Display Campaigns" },
      { id: 3, name: "Amazon - Search Campaigns" },
      { id: 4, name: "Big Basket Campaigns" },
      { id: 5, name: "Blinkit Campaigns" },
      { id: 6, name: "DV360" },
      { id: 7, name: "Facebook" },
      { id: 8, name: "Flipkart PCA Campaigns" },
      { id: 9, name: "Flipkart PLA Campaigns" },
      { id: 10, name: "Gadwords" },
      { id: 11, name: "Google Analytics" },
      { id: 12, name: "Instagram" },
      { id: 13, name: "Myntraa Campaigns" },
      { id: 14, name: "Nykaa Campaigns" },
      { id: 15, name: "Page Speed Insights" },
      { id: 16, name: "SEO" },
      { id: 17, name: "SEOptimer" },
      { id: 18, name: "Twitter" },
    ];
    await Platform.bulkCreate(platforms, { ignoreDuplicates: true });

    // Insert Sections
    const sections = [
      { id: 1, name: "Ecom", platform_id: 1 },
      { id: 2, name: "Ecom", platform_id: 2 },
      { id: 3, name: "Ecom", platform_id: 3 },
      { id: 4, name: "Ecom", platform_id: 4 },
      { id: 5, name: "Ecom", platform_id: 5 },
      { id: 6, name: "Paid", platform_id: 6 },
      { id: 7, name: "Social", platform_id: 7 },
      { id: 8, name: "Paid", platform_id: 7 },
      { id: 9, name: "Ecom", platform_id: 8 },
      { id: 10, name: "Ecom", platform_id: 9 },
      { id: 11, name: "Paid", platform_id: 10 },
      { id: 12, name: "Brand Perf", platform_id: 11 },
      { id: 13, name: "Social", platform_id: 12 },
      { id: 14, name: "Ecom", platform_id: 13 },
      { id: 15, name: "Ecom", platform_id: 14 },
      { id: 16, name: "Brand Perf", platform_id: 15 },
      { id: 17, name: "Social", platform_id: 16 },
      { id: 18, name: "Brand Perf", platform_id: 17 },
      { id: 19, name: "Social", platform_id: 18 },
    ];
    await Section.bulkCreate(sections, { ignoreDuplicates: true });

    // Insert Metrics
    const metrics = [
      { id: 1, name: "Average ratings", platform_id: 1, section_id: 1 },
      { id: 2, name: "Reviews", platform_id: 1, section_id: 1 },
      {
        id: 3,
        name: "Net sentiment of reviews",
        platform_id: 1,
        section_id: 1,
      },
      { id: 4, name: "Availability%", platform_id: 1, section_id: 1 },
      { id: 5, name: "Content Score", platform_id: 1, section_id: 1 },
      { id: 6, name: "Impressions", platform_id: 2, section_id: 2 },
      { id: 7, name: "Clicks", platform_id: 2, section_id: 2 },
      { id: 8, name: "CTR", platform_id: 2, section_id: 2 },
      { id: 9, name: "Ad clicks", platform_id: 2, section_id: 2 },
      { id: 10, name: "CPC", platform_id: 2, section_id: 2 },
      { id: 11, name: "ROAS", platform_id: 2, section_id: 2 },
      { id: 12, name: "Impressions", platform_id: 3, section_id: 3 },
      { id: 13, name: "Clicks", platform_id: 3, section_id: 3 },
      { id: 14, name: "CTR", platform_id: 3, section_id: 3 },
      { id: 15, name: "Ad clicks", platform_id: 3, section_id: 3 },
      { id: 16, name: "CPC", platform_id: 3, section_id: 3 },
      { id: 17, name: "ROAS", platform_id: 3, section_id: 3 },
      { id: 18, name: "Impressions", platform_id: 4, section_id: 4 },
      { id: 19, name: "Clicks", platform_id: 4, section_id: 4 },
      { id: 20, name: "CTR", platform_id: 4, section_id: 4 },
      { id: 21, name: "Ad clicks", platform_id: 4, section_id: 4 },
      { id: 22, name: "CPC", platform_id: 4, section_id: 4 },
      { id: 23, name: "ROAS", platform_id: 4, section_id: 4 },
      { id: 24, name: "Impressions", platform_id: 5, section_id: 5 },
      { id: 25, name: "Clicks", platform_id: 5, section_id: 5 },
      { id: 26, name: "CTR", platform_id: 5, section_id: 5 },
      { id: 27, name: "Ad clicks", platform_id: 5, section_id: 5 },
      { id: 28, name: "CPC", platform_id: 5, section_id: 5 },
      { id: 29, name: "ROAS", platform_id: 5, section_id: 5 },
      { id: 30, name: "Impressions", platform_id: 6, section_id: 6 },
      { id: 31, name: "Clicks", platform_id: 6, section_id: 6 },
      { id: 32, name: "CTR", platform_id: 6, section_id: 6 },
      { id: 33, name: "Ad clicks", platform_id: 6, section_id: 6 },
      { id: 34, name: "CPC", platform_id: 6, section_id: 6 },
      { id: 35, name: "ROAS", platform_id: 6, section_id: 6 },
      { id: 36, name: "Reach", platform_id: 7, section_id: 7 },
      { id: 37, name: "Page impressions", platform_id: 7, section_id: 7 },
      { id: 38, name: "Engagements", platform_id: 7, section_id: 7 },
      { id: 39, name: "Ad clicks", platform_id: 7, section_id: 8 },
      { id: 40, name: "CPC", platform_id: 7, section_id: 8 },
      { id: 41, name: "CTR", platform_id: 7, section_id: 8 },
      { id: 42, name: "ROAS", platform_id: 7, section_id: 8 },
      { id: 43, name: "Impressions", platform_id: 8, section_id: 9 },
      { id: 44, name: "Clicks", platform_id: 8, section_id: 9 },
      { id: 45, name: "CTR", platform_id: 8, section_id: 9 },
      { id: 46, name: "Ad clicks", platform_id: 8, section_id: 9 },
      { id: 47, name: "CPC", platform_id: 8, section_id: 9 },
      { id: 48, name: "ROAS", platform_id: 8, section_id: 9 },
      { id: 49, name: "Impressions", platform_id: 9, section_id: 10 },
      { id: 50, name: "Clicks", platform_id: 9, section_id: 10 },
      { id: 51, name: "CTR", platform_id: 9, section_id: 10 },
      { id: 52, name: "Ad clicks", platform_id: 9, section_id: 10 },
      { id: 53, name: "CPC", platform_id: 9, section_id: 10 },
      { id: 54, name: "ROAS", platform_id: 9, section_id: 10 },
      { id: 55, name: "Impressions", platform_id: 10, section_id: 11 },
      { id: 56, name: "Clicks", platform_id: 10, section_id: 11 },
      { id: 57, name: "CTR", platform_id: 10, section_id: 11 },
      { id: 58, name: "Ad clicks", platform_id: 10, section_id: 11 },
      { id: 59, name: "CPC", platform_id: 10, section_id: 11 },
      { id: 60, name: "ROAS", platform_id: 10, section_id: 11 },
      { id: 61, name: "Brand Traffic", platform_id: 11, section_id: 12 },
      { id: 62, name: "Impressions", platform_id: 11, section_id: 12 },
      { id: 63, name: "Organic Traffic", platform_id: 11, section_id: 12 },
      { id: 64, name: "New Users", platform_id: 11, section_id: 12 },
      { id: 65, name: "Page Views", platform_id: 11, section_id: 12 },
      { id: 66, name: "Bounce Rate", platform_id: 11, section_id: 12 },
      { id: 67, name: "Brand Conversion", platform_id: 11, section_id: 12 },
      { id: 68, name: "Total Users", platform_id: 11, section_id: 12 },
      { id: 69, name: "Reach", platform_id: 12, section_id: 13 },
      { id: 70, name: "Page impressions", platform_id: 12, section_id: 13 },
      { id: 71, name: "Engagements", platform_id: 12, section_id: 13 },
      { id: 72, name: "Impressions", platform_id: 13, section_id: 14 },
      { id: 73, name: "Clicks", platform_id: 13, section_id: 14 },
      { id: 74, name: "CTR", platform_id: 13, section_id: 14 },
      { id: 75, name: "Ad clicks", platform_id: 13, section_id: 14 },
      { id: 76, name: "CPC", platform_id: 13, section_id: 14 },
      { id: 77, name: "ROAS", platform_id: 13, section_id: 14 },
      { id: 78, name: "Impressions", platform_id: 14, section_id: 15 },
      { id: 79, name: "Clicks", platform_id: 14, section_id: 15 },
      { id: 80, name: "CTR", platform_id: 14, section_id: 15 },
      { id: 81, name: "Ad clicks", platform_id: 14, section_id: 15 },
      { id: 82, name: "CPC", platform_id: 14, section_id: 15 },
      { id: 83, name: "ROAS", platform_id: 14, section_id: 15 },
      {
        id: 84,
        name: "No. of URLs with good CWV",
        platform_id: 15,
        section_id: 16,
      },
      {
        id: 85,
        name: "No. of URLs with bad CWV",
        platform_id: 15,
        section_id: 16,
      },
      { id: 86, name: "LCP Score", platform_id: 15, section_id: 16 },
      { id: 87, name: "FID Score", platform_id: 15, section_id: 16 },
      { id: 88, name: "CLS Score", platform_id: 15, section_id: 16 },
      { id: 89, name: "Page Speed Score", platform_id: 15, section_id: 16 },
      { id: 90, name: "Impressions", platform_id: 16, section_id: 17 },
      { id: 91, name: "Clicks", platform_id: 16, section_id: 17 },
      { id: 92, name: "CTR", platform_id: 16, section_id: 17 },
      {
        id: 93,
        name: "Pages crawled per day",
        platform_id: 16,
        section_id: 17,
      },
      { id: 94, name: "Errors", platform_id: 16, section_id: 17 },
      { id: 95, name: "Warnings", platform_id: 17, section_id: 18 },
      { id: 96, name: "Passed tests", platform_id: 17, section_id: 18 },
      { id: 97, name: "Reach", platform_id: 18, section_id: 19 },
      { id: 98, name: "Page impressions", platform_id: 18, section_id: 19 },
      { id: 99, name: "Engagements", platform_id: 18, section_id: 19 },
    ];
    await Metrics.bulkCreate(metrics, { ignoreDuplicates: true });

    console.log("Data inserted successfully.");
  } catch (error) {
    console.error("Error inserting data:", error);
  } finally {
    await sequelize.close(); // Close the connection to the database
  }
};

// Run the insertData function
insertData();
