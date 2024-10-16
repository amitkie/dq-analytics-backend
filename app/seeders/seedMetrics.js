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


const xlsx = require('xlsx');
const fs = require('fs');
const { Sequelize, DataTypes } = require("sequelize");
const dbConfig = require("../../config/db"); 
const excelFilePath = "./metrics.xlsx";
// Adjust the path to your db configuration
// function extractMetricsFromExcel(filePath) {
//   // Read the Excel file
//   const workbook = xlsx.readFile(filePath);
  
//   // Get the first sheet
//   const sheetName = workbook.SheetNames[0];
//   const sheet = workbook.Sheets[sheetName];
  
//   // Convert the sheet data to JSON
//   const data = xlsx.utils.sheet_to_json(sheet);

//   // Create an array of metrics
//   const metrics = data.map(row => ({
//     id: row.id,
//     name: row.name,
//     platform_id: row.platform_id,
//     section_id: row.section_id,
//   }));

//   return metrics;
// }
// const metricsArray = extractMetricsFromExcel(excelFilePath);
// fs.writeFileSync('metrics_output.json', JSON.stringify(metricsArray, null, 2));

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
    const metrics = 
    [
      {
        "id": 1,
        "name": "Average ratings",
        "platform_id": 1,
        "section_id": 1
      },
      {
        "id": 2,
        "name": "Reviews",
        "platform_id": 1,
        "section_id": 1
      },
      {
        "id": 3,
        "name": "Net sentiment of reviews",
        "platform_id": 1,
        "section_id": 1
      },
      {
        "id": 4,
        "name": "Availability%",
        "platform_id": 1,
        "section_id": 1
      },
      {
        "id": 5,
        "name": "Search visibility share \n (Organic)",
        "platform_id": 1,
        "section_id": 1
      },
      {
        "id": 6,
        "name": "Search visibility share \n (Paid)",
        "platform_id": 1,
        "section_id": 1
      },
      {
        "id": 7,
        "name": "Amazon Best seller rank",
        "platform_id": 1,
        "section_id": 1
      },
      {
        "id": 8,
        "name": "Impressions",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 9,
        "name": "Clicks",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 10,
        "name": "CTR",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 11,
        "name": "CPC",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 12,
        "name": "Display â€“ Spends",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 13,
        "name": "CPM",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 14,
        "name": "DPV",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 15,
        "name": "DPVR",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 16,
        "name": "ATC",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 17,
        "name": "ATCR",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 18,
        "name": "Purchases",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 19,
        "name": "Order Conversion Rate",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 20,
        "name": "Click to new purchase",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 21,
        "name": "% of new purchase rate",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 22,
        "name": "Sales value",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 23,
        "name": "unit sold",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 24,
        "name": "Cost per new customer (CAC)",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 25,
        "name": "ACOS %",
        "platform_id": 2,
        "section_id": 2
      },
      {
        "id": 26,
        "name": "Impressions",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 27,
        "name": "Clicks",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 28,
        "name": "CTR",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 29,
        "name": "CPC",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 30,
        "name": "Search â€“ Spends",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 31,
        "name": "CPM",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 32,
        "name": "Purchases",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 33,
        "name": "Order Conversion Rate",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 34,
        "name": "Click to new purchase",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 35,
        "name": "% of new purchase rate",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 36,
        "name": "Sales value",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 37,
        "name": "unit sold",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 38,
        "name": "Cost per order (new)",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 39,
        "name": "ACOS %",
        "platform_id": 3,
        "section_id": 3
      },
      {
        "id": 40,
        "name": "Clicks",
        "platform_id": 4,
        "section_id": 4
      },
      {
        "id": 41,
        "name": "CPC",
        "platform_id": 4,
        "section_id": 4
      },
      {
        "id": 42,
        "name": "Search - Spends",
        "platform_id": 4,
        "section_id": 4
      },
      {
        "id": 43,
        "name": "Purchases",
        "platform_id": 4,
        "section_id": 4
      },
      {
        "id": 44,
        "name": "Order Conversion Rate",
        "platform_id": 4,
        "section_id": 4
      },
      {
        "id": 45,
        "name": "Sales value",
        "platform_id": 4,
        "section_id": 4
      },
      {
        "id": 46,
        "name": "unit sold",
        "platform_id": 4,
        "section_id": 4
      },
      {
        "id": 47,
        "name": "ACOS %",
        "platform_id": 4,
        "section_id": 4
      },
      {
        "id": 48,
        "name": "Impressions",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 49,
        "name": "Clicks",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 50,
        "name": "Search - Spends",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 51,
        "name": "CTR",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 52,
        "name": "CPC",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 53,
        "name": "CPM",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 54,
        "name": "Purchases",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 55,
        "name": "Order Conversion Rate",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 56,
        "name": "Click to new purchase",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 57,
        "name": "% of new purchase rate",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 58,
        "name": "Sales value",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 59,
        "name": "unit sold",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 60,
        "name": "Cost per order",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 61,
        "name": "ACOS %",
        "platform_id": 5,
        "section_id": 5
      },
      {
        "id": 62,
        "name": "Impressions",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 63,
        "name": "Clicks",
        "platform_id": 6
      },
      {
        "id": 64,
        "name": "Spends",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 65,
        "name": "Reach",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 66,
        "name": "Frequency",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 67,
        "name": "VTR",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 68,
        "name": "CPM",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 69,
        "name": "Add to cart",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 70,
        "name": "Click to cart %",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 71,
        "name": "Cart to checkout",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 72,
        "name": "Transactions",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 73,
        "name": "Transaction rate",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 74,
        "name": "Cost per transaction",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 75,
        "name": "AOV",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 76,
        "name": "ACOS %",
        "platform_id": 6,
        "section_id": 6
      },
      {
        "id": 77,
        "name": "Reach",
        "platform_id": 7,
        "section_id": 7
      },
      {
        "id": 78,
        "name": "Net sentiment",
        "platform_id": 7,
        "section_id": 7
      },
      {
        "id": 79,
        "name": "Mentions",
        "platform_id": 7,
        "section_id": 7
      },
      {
        "id": 80,
        "name": "Engagement",
        "platform_id": 7,
        "section_id": 7
      },
      {
        "id": 81,
        "name": "Engagement %",
        "platform_id": 7,
        "section_id": 7
      },
      {
        "id": 82,
        "name": "Spends",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 83,
        "name": "Impressions",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 84,
        "name": "Frequency",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 85,
        "name": "Clicks",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 86,
        "name": "VTR",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 87,
        "name": "CPM",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 88,
        "name": "Add to cart",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 89,
        "name": "Click to cart %",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 90,
        "name": "Cart to checkout",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 91,
        "name": "Transactions",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 92,
        "name": "Transaction rate",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 93,
        "name": "Cost per transaction",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 94,
        "name": "AOV",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 95,
        "name": "ACOS %",
        "platform_id": 7,
        "section_id": 8
      },
      {
        "id": 96,
        "name": "Impressions",
        "platform_id": 8,
        "section_id": 9
      },
      {
        "id": 97,
        "name": "Clicks",
        "platform_id": 8,
        "section_id": 9
      },
      {
        "id": 98,
        "name": "CTR",
        "platform_id": 8,
        "section_id": 9
      },
      {
        "id": 99,
        "name": "CPC",
        "platform_id": 8
      },
      {
        "id": 100,
        "name": "Search - Spends",
        "platform_id": 8,
        "section_id": 9
      },
      {
        "id": 101,
        "name": "Purchases",
        "platform_id": 8,
        "section_id": 9
      },
      {
        "id": 102,
        "name": "Order Conversion Rate",
        "platform_id": 8,
        "section_id": 9
      },
      {
        "id": 103,
        "name": "Sales value",
        "platform_id": 8,
        "section_id": 9
      },
      {
        "id": 104,
        "name": "unit sold",
        "platform_id": 8,
        "section_id": 9
      },
      {
        "id": 105,
        "name": "ACOS %",
        "platform_id": 8,
        "section_id": 9
      },
      {
        "id": 106,
        "name": "Impressions",
        "platform_id": 9,
        "section_id": 10
      },
      {
        "id": 107,
        "name": "Clicks",
        "platform_id": 9,
        "section_id": 10
      },
      {
        "id": 108,
        "name": "CPC",
        "platform_id": 9,
        "section_id": 10
      },
      {
        "id": 109,
        "name": "Search - Spends",
        "platform_id": 9,
        "section_id": 10
      },
      {
        "id": 110,
        "name": "CPM",
        "platform_id": 9,
        "section_id": 10
      },
      {
        "id": 111,
        "name": "Purchases",
        "platform_id": 9,
        "section_id": 10
      },
      {
        "id": 112,
        "name": "Order Conversion Rate",
        "platform_id": 9,
        "section_id": 10
      },
      {
        "id": 113,
        "name": "Sales value",
        "platform_id": 9,
        "section_id": 10
      },
      {
        "id": 114,
        "name": "unit sold",
        "platform_id": 9,
        "section_id": 10
      },
      {
        "id": 115,
        "name": "ACOS %",
        "platform_id": 9,
        "section_id": 10
      },
      {
        "id": 116,
        "name": "Impressions",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 117,
        "name": "Clicks",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 118,
        "name": "Spends",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 119,
        "name": "Reach",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 120,
        "name": "Frequency",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 121,
        "name": "VTR",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 122,
        "name": "CPM",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 123,
        "name": "Add to cart",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 124,
        "name": "Click to cart %",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 125,
        "name": "Cart to checkout",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 126,
        "name": "Transactions",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 127,
        "name": "Transaction rate",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 128,
        "name": "Cost per transaction",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 129,
        "name": "AOV",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 130,
        "name": "ACOS %",
        "platform_id": 10,
        "section_id": 11
      },
      {
        "id": 131,
        "name": "Unique Visitors",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 132,
        "name": "Sessions",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 133,
        "name": "Load Time (seconds)",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 134,
        "name": "Pages per sessions",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 135,
        "name": "Avg. Session Duration (mins)",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 136,
        "name": "Product Views",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 137,
        "name": "Add to Basket",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 138,
        "name": "Checkout",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 139,
        "name": "Product views per session",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 140,
        "name": "Sessions to product views %",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 141,
        "name": "Product views to cart %",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 142,
        "name": "Cart to Checkout %",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 143,
        "name": "Check out to Transaction %",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 144,
        "name": "Overall conversion %",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 145,
        "name": "AOV",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 146,
        "name": "ACOS %",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 147,
        "name": "Repeat rate %",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 148,
        "name": "CAC",
        "platform_id": 11,
        "section_id": 12
      },
      {
        "id": 149,
        "name": "Net sentiment",
        "platform_id": 12,
        "section_id": 13
      },
      {
        "id": 150,
        "name": "Mentions",
        "platform_id": 12,
        "section_id": 13
      },
      {
        "id": 151,
        "name": "Engagement",
        "platform_id": 12,
        "section_id": 13
      },
      {
        "id": 152,
        "name": "Engagement %",
        "platform_id": 12,
        "section_id": 13
      },
      {
        "id": 153,
        "name": "Impressions",
        "platform_id": 13,
        "section_id": 14
      },
      {
        "id": 154,
        "name": "Clicks",
        "platform_id": 13,
        "section_id": 14
      },
      {
        "id": 155,
        "name": "CTR",
        "platform_id": 13,
        "section_id": 14
      },
      {
        "id": 156,
        "name": "Search - home page banners",
        "platform_id": 13,
        "section_id": 14
      },
      {
        "id": 157,
        "name": "CPM",
        "platform_id": 13,
        "section_id": 14
      },
      {
        "id": 158,
        "name": "Order",
        "platform_id": 13,
        "section_id": 14
      },
      {
        "id": 159,
        "name": "Order Conversion Rate",
        "platform_id": 13,
        "section_id": 14
      },
      {
        "id": 160,
        "name": "ACOS %(to be calculated )",
        "platform_id": 13,
        "section_id": 14
      },
      {
        "id": 161,
        "name": "Impressions",
        "platform_id": 14,
        "section_id": 15
      },
      {
        "id": 162,
        "name": "Clicks",
        "platform_id": 14,
        "section_id": 15
      },
      {
        "id": 163,
        "name": "CTR",
        "platform_id": 14,
        "section_id": 15
      },
      {
        "id": 164,
        "name": "Search - home page banners",
        "platform_id": 14,
        "section_id": 15
      },
      {
        "id": 165,
        "name": "CPM",
        "platform_id": 14,
        "section_id": 15
      },
      {
        "id": 166,
        "name": "Order",
        "platform_id": 14,
        "section_id": 15
      },
      {
        "id": 167,
        "name": "Order Conversion Rate",
        "platform_id": 14,
        "section_id": 15
      },
      {
        "id": 168,
        "name": "ACOS %(to be calculated )",
        "platform_id": 14,
        "section_id": 15
      },
      {
        "id": 169,
        "name": "Mobile page speed insights score",
        "platform_id": 15,
        "section_id": 16
      },
      {
        "id": 170,
        "name": "Web page speed insights score",
        "platform_id": 15,
        "section_id": 16
      },
      {
        "id": 171,
        "name": "Largest contentful paint (LCP) - seconds",
        "platform_id": 15,
        "section_id": 16
      },
      {
        "id": 172,
        "name": "First input delay (FID) - milli seconds",
        "platform_id": 15,
        "section_id": 16
      },
      {
        "id": 173,
        "name": "Cumulative layout shift (CLS)",
        "platform_id": 15,
        "section_id": 16
      },
      {
        "id": 177,
        "name": "First contentful paint (FCP) - seconds",
        "platform_id": 15,
        "section_id": 16
      },
      {
        "id": 178,
        "name": "Time to interact (TTI)- seconds",
        "platform_id": 15,
        "section_id": 16
      },
      {
        "id": 179,
        "name": "Speed Index - seconds",
        "platform_id": 15,
        "section_id": 16
      },
      {
        "id": 180,
        "name": "Total blocking time (TBT) - milli seconds",
        "platform_id": 15,
        "section_id": 16
      },
      {
        "id": 181,
        "name": "Organic rank",
        "platform_id": 16,
        "section_id": 17
      },
      {
        "id": 182,
        "name": "Usability (SEO optimer)",
        "platform_id": 17,
        "section_id": 18
      },
      {
        "id": 183,
        "name": "Performance (SEO optimer)",
        "platform_id": 17,
        "section_id": 18
      },
      {
        "id": 184,
        "name": "Social (SEO optimer)",
        "platform_id": 17,
        "section_id": 18
      },
      {
        "id": 185,
        "name": "SEO (SEO optimer)",
        "platform_id": 17,
        "section_id": 18
      },
      {
        "id": 186,
        "name": "Net sentiment",
        "platform_id": 18,
        "section_id": 19
      },
      {
        "id": 187,
        "name": "Mentions",
        "platform_id": 18,
        "section_id": 19
      },
      {
        "id": 188,
        "name": "Engagement",
        "platform_id": 18,
        "section_id": 19
      },
      {
        "id": 189,
        "name": "Engagement %",
        "platform_id": 18,
        "section_id": 19
      }
    ]   
    await Metrics.bulkCreate(metrics, { ignoreDuplicates: true });

    // console.log("Data inserted successfully.");
  } catch (error) {
  } finally {
    await sequelize.close(); // Close the connection to the database
  }
};

// Run the insertData function
insertData();
