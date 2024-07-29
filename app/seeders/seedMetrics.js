// seeders/seed-section-platform-metrics.js

const { Sequelize, DataTypes } = require('sequelize');
const dbConfig = require('../../config/db'); // Adjust the path as per your project structure
const { v4: uuidv4 } = require('uuid');

// Define your data to seed
const dataToSeed = [
  { section: 'Ecom', platform: 'Amazon', metrics: [
    'Content Score', 'Average ratings', 'Reviews', 'Net sentiment of reviews', 'Availability%'
  ]},
  { section: 'Ecom', platform: 'Amazon - Search Campaigns', metrics: [
    'Search – Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
    'Order Conversion Rate', 'Click to new purchase', '% of new purchase rate',
    'Sales value', 'unit sold', 'Cost per order (new)', 'ACOS %'
  ]},
  { section: 'Ecom', platform: 'Amazon - Display Campaigns', metrics: [
    'Display – Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'DPV', 'DPVR', 'ATC',
    'ATCR', 'Purchases', 'Order Conversion Rate', 'Click to new purchase', '% of new purchase rate',
    'Sales value', 'unit sold', 'Cost per new customer (CAC)', 'ACOS %'
  ]},
  { section: 'Ecom', platform: 'Flipkart PLA Campaigns', metrics: [
    'Search - Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
    'Order Conversion Rate', 'Sales value', 'unit sold', 'ACOS %'
  ]},
  { section: 'Ecom', platform: 'Flipkart PCA Campaigns', metrics: [
    'Search - Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
    'Order Conversion Rate', 'Sales value', 'unit sold', 'ACOS %'
  ]},
  { section: 'Ecom', platform: 'Big Basket Campaigns', metrics: [
    'Search - Spends', 'Clicks', 'CPC', 'Purchases', 'Order Conversion Rate',
    'Sales value', 'unit sold', 'ACOS %'
  ]},
  { section: 'Ecom', platform: 'Blinkit Campaigns', metrics: [
    'Search - Spends', 'Impressions', 'CPM', 'Clicks', 'CTR', 'CPC', 'Purchases',
    'Order Conversion Rate', 'Click to new purchase', '% of new purchase rate',
    'Sales value', 'unit sold', 'Cost per order (new)', 'ACOS %'
  ]},
  { section: 'Ecom', platform: 'Nykaa Campaigns', metrics: [
    'Search - home page banners', 'Impressions', 'CPM', 'Clicks', 'CTR', 'Order',
    'Order Conversion Rate', 'ACOS %(to be calculated )'
  ]},
  { section: 'Ecom', platform: 'Myntraa Campaigns', metrics: [
    'Search - home page banners', 'Impressions', 'CPM', 'Clicks', 'CTR', 'Order',
    'Order Conversion Rate', 'ACOS %(to be calculated )'
  ]},
  { section: 'Ecom', platform: 'Amazon', metrics: [
    'Search visibility share (Organic)', 'Search visibility share (Paid)', 'Amazon Best seller rank'
  ]},
  { section: 'Social', platform: 'SEO', metrics: [
    'Organic rank'
  ]},

  // This needs to be modified create individual records for each
  { section: 'Social', platform: 'Facebook', metrics: [
    'Net sentiment', 'Mentions', 'Engagement', 'Engagement %'
  ]},
  { section: 'Social', platform: 'Twitter', metrics: [
    'Net sentiment', 'Mentions', 'Engagement', 'Engagement %'
  ]},
  { section: 'Social', platform: 'Instagram', metrics: [
    'Net sentiment', 'Mentions', 'Engagement', 'Engagement %'
  ]},
  { section: 'Paid', platform: 'Gadwords', metrics: [
    'Spends', 'Impressions', 'Reach', 'Frequency', 'Clicks', 'VTR', 'CPM', 'Add to cart',
    'Click to cart %', 'Cart to checkout', 'Transactions', 'Transaction rate', 'Cost per transaction',
    'AOV', 'ACOS %'
  ]},
  { section: 'Paid', platform: 'Facebook', metrics: [
    'Spends', 'Impressions', 'Reach', 'Frequency', 'Clicks', 'VTR', 'CPM', 'Add to cart',
    'Click to cart %', 'Cart to checkout', 'Transactions', 'Transaction rate', 'Cost per transaction',
    'AOV', 'ACOS %'
  ]},
  { section: 'Paid', platform: 'DV360', metrics: [
    'Spends', 'Impressions', 'Reach', 'Frequency', 'Clicks', 'VTR', 'CPM', 'Add to cart',
    'Click to cart %', 'Cart to checkout', 'Transactions', 'Transaction rate', 'Cost per transaction',
    'AOV', 'ACOS %'
  ]},
  { section: 'Brand Perf', platform: 'Google Analytics', metrics: [
    'Unique Visitors', 'Sessions', 'Load Time (seconds)', 'Pages per sessions', 'Avg. Session Duration (mins)',
    'Product Views', 'Add to Basket', 'Checkout', 'Product views per session', 'Sessions to product views %',
    'Product views to cart %', 'Cart to Checkout %', 'Check out to Transaction %', 'Overall conversion %',
    'AOV', 'ACOS %', 'Repeat rate %', 'CAC'
  ]},
  { section: 'Brand Perf', platform: 'Page Speed Insights', metrics: [
    'Mobile page speed insights score', 'Web page speed insights score', 'Largest contentful paint (LCP) - seconds',
    'First input delay (FID) - milli seconds', 'Cumulative layout shift (CLS)', 'First contentful paint (FCP) - seconds',
    'Time to interact (TTI)- seconds', 'Speed Index - seconds', 'Total blocking time (TBT) - milli seconds'
  ]},
  { section: 'Brand Perf', platform: 'SEOptimer', metrics: [
    'Usability (SEO optimer)', 'Performance (SEO optimer)', 'Social (SEO optimer)', 'SEO (SEO optimer)'
  ]}
];

// Function to seed data
const seedSectionPlatformMetrics = async () => {
  // Initialize Sequelize
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

  try {
    // Define models
    const Section = require('../models/section')(sequelize, DataTypes);
    const Platform = require('../models/platform')(sequelize, DataTypes);
    const Metrics = require('../models/metrics')(sequelize, DataTypes);
    // const SectionPlatformMetrics = require('../models/sectionPlatformMetrics')(sequelize, DataTypes);

    // Sync models with database
    await sequelize.sync();

    // Seed data
    for (const data of dataToSeed) {
      // Find or create section
      let section = await Section.findOne({ where: { name: data.section } });
      if (!section) {
        section = await Section.create({ name: data.section });
      }

      // Find or create platform
      let platform = await Platform.findOne({ where: { name: data.platform } });
      if (!platform) {
        platform = await Platform.create({ name: data.platform , section_id:section.id});
      }

      // Seed metrics for the platform
      await Promise.all(data.metrics.map(async metricName => {
        let metric = await Metrics.findOne({ where: { name: metricName } });
        if (!metric) {
          metric = await Metrics.create({ name: metricName, platform_id:platform.id  });
        }

        // Link section, platform, and metrics
      }));

      console.log(`Section '${section.name}', Platform '${platform.name}' and Metrics seeded successfully.`);
    }

    console.log('All data seeded successfully.');
  } catch (error) {
    console.error('Error seeding data:', error);
  } finally {
    // Close Sequelize connection
    await sequelize.close();
  }
};

// Run the seeding function
seedSectionPlatformMetrics();
