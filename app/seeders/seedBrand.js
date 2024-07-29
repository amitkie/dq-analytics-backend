const { Sequelize, DataTypes } = require('sequelize');
const dbConfig = require("../../config/db.js");

// Sequelize initialization
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

// Import models
const Category = require('../models/category')(sequelize, DataTypes);
const Brand = require('../models/brand')(sequelize, DataTypes);

// Data to seed (Category and Brand mappings)
const dataToSeed = [
  { category: 'Beauty', brands: [
    'Parachute Advansed Body Lotion', 'PureSense', 'Coco Soul Beauty', 'Bio Oil', 'Livon',
    'Colourbar', 'Wow', 'Mama Earth', 'Lakme', 'Maybelline', 'Plum', 'Bella Vita',
    'Neutrogena', 'Revlon', 'Nivea', 'Love Beauty', 'Faces', 'Vaseline',
    'Forest Essentials', 'Himalaya', 'Biotique', 'Dot & Key', 'The Body Shop',
    'The Derma Co', 'UrbanBoitanics', 'Kama Ayurveda', 'Moms Co', 'Olay India',
    'L\'Oreal', 'Streax', 'Khadi', 'Mcaffeine', 'Tresseme', 'Medimix',
    'Juicy Chemistry', 'Garnier', 'Reequil', 'Kaya Clinic', 'Dr Sheth\'s',
    'Ponds', 'Fiama', 'Santoor', 'SoulTree', 'Pears', 'Just Herbs', 'Arata',
    'Dove', 'Yardley', 'Rexona', 'BBLUNT', 'StBotanica', 'Herbal Essence',
    'Palmolive', 'Organic Riot', 'Cinthol', 'Hamam', 'Liril', 'Rey Naturals',
    'Lux', 'Dettol', 'Sugar Cosmetics', 'Earth Rhythm', 'Bare Anatomy',
    'The Beauty Co.', 'Pilgrim', 'Renee Cosmetics', 'Minimalist', 'Deconstruct',
    'Skinkraft', 'Chemist At Play', 'Traya', 'Vedix'
  ]},
  { category: 'Foods', brands: [
    'Immuniveda - Rest of the portfolio', 'Saffola Fittify', 'Saffola Aura',
    'Saffola Oodles', 'Saffola Oil', 'Saffola Honey', 'Saffola Oats',
    'Immuniveda - Chyawanprash', 'Saffola Mealmaker Soya', 'Saffola Peanut Butter',
    'Tata', 'Nutrela', 'Herbalife', 'Nescafe', 'Fast&Up', 'Organic India',
    'Oziva', 'Dabur Immunity', 'Yippee Noodles', 'Vahdam', 'Oreo', 'Baidyanath',
    'Sunfeast', 'Neuherbs', 'Maggi', 'PediaSure', 'Veeba', 'Society Tea',
    'Cadbury Dairy Milk Silk', 'Quaker oats', 'Knorr', 'Kapiva', 'Aashirvaad',
    'Fortune', 'Cureveda', 'Raw Pressery', 'Dr Vaidya\'s', 'Wingreen', 'Zandu',
    'Kellogg\'s', 'Too Yumm', 'Max Care', 'SunDrop Oil', 'Yogabar', 'True Elements',
    'Slurrp Farm', 'Happilo', 'Open Secret', 'Wellbeing Nutrition', 'Himlayan Organics',
    'Habbit Health', 'Gynoveda'
  ]},
  { category: 'Hair Care', brands: [
    'Parachute Advansed Coconut Oil Gold', 'Parachute Advansed Jasmine',
    'Parachute Advansed Ayurvedic', 'Parachute Advansed Aloe vera',
    'Parachute Advansed Hot Oil', 'Hair & Care', 'Nihar Naturals Hair Oil',
    'Nihar Shanti Amla', 'Parachute Advansed Onion', 'Jataa', 'Sunsilk',
    'Clinic Plus', 'Dabur Vatika', 'Godrej', 'Dabur Amla', 'Emami 7',
    'Pantene', 'Navratna', 'Kesh King', 'Bajaj Almond', 'Head & Shoulders',
    'Indulekha', 'Vegetal', 'Dabur Almond', 'Vasmol', 'VVD'
  ]},
  { category: 'Male Grooming', brands: [
    'Beardo', 'Parachute Advansed Men', 'Set Wet', 'The Man Company', 'Engage',
    'Bombay Shaving', 'Wild Stone', 'Park Avenue', 'Gatsby', 'UrbanGabru',
    'Ustraa', 'Fogg', 'Fair and Handsome', 'Gillette', 'Axe', 'Phillips',
    'Man Matters'
  ]}
];

// Seed function
const seedCategoriesAndBrands = async () => {
  try {
    // Sync models with the database (if not already synced)
    await sequelize.sync();

    // Loop through each category and seed brands
    for (const data of dataToSeed) {
      // Create category and get its instance
      const category = await Category.create({ name: data.category });

      // Seed brands for the category
      await Promise.all(data.brands.map(async brandName => {
        await Brand.create({
          name: brandName,
          category_id: category.id
        });
      }));

      console.log(`Category '${category.name}' and brands seeded successfully.`);
    }

    console.log('Categories and brands seeding completed.');
  } catch (error) {
    console.error('Error seeding categories and brands:', error);
  } finally {
    // Close the database connection
    await sequelize.close();
  }
};

// Run the seeder function
seedCategoriesAndBrands();
