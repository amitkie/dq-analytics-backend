const dbConfig = require("../../config/db.js");

const Sequelize = require("sequelize");
const sequelize = new Sequelize(dbConfig.DB, dbConfig.USER, dbConfig.PASSWORD, {
  host: dbConfig.HOST,
  dialect: dbConfig.dialect,
  operatorsAliases: false,
  port: dbConfig.PORT,
  pool: {
    max: dbConfig.pool.max,
    min: dbConfig.pool.min,
    acquire: dbConfig.pool.acquire,
    idle: dbConfig.pool.idle
  },
  dialectOptions: {
    ssl: {
      require: true,
      rejectUnauthorized: false // Set to true for production
    }
  }
});

const db = {};

db.Sequelize = Sequelize;
db.sequelize = sequelize;


console.log(db.sequelize);
db.benchmarks = require("./benchmark.js")(sequelize, Sequelize);
db.frequencies = require("./frequency.js")(sequelize, Sequelize);
db.brands = require("./brand.js")(sequelize, Sequelize);
db.categories = require("./category.js")(sequelize, Sequelize);
db.metrics = require("./metrics.js")(sequelize, Sequelize);
db.platform = require("./platform.js")(sequelize, Sequelize);
db.sections = require("./section.js")(sequelize, Sequelize);
db.users = require("./user.js")(sequelize, Sequelize);
db.userActivities = require("./userActivity.js")(sequelize, Sequelize);
db.userDetails = require("./userDetails.js")(sequelize, Sequelize);
db.userProjects = require("./userProjects.js")(sequelize, Sequelize);
db.payments = require("./payment.js")(sequelize, Sequelize);
db.userUrls = require("./userUrl.js")(sequelize, Sequelize);
db.userAnalytic = require("./userAnalytics.js")(sequelize, Sequelize);
db.projectBenchmark = require("./projectBenchmarks.js")(sequelize, Sequelize);
db.projectBenchmarkSection = require("./projectBenchmarkSection.js")(sequelize, Sequelize);
db.projectBenchmarkPlatform = require("./projectBenchmarkPlatform.js")(sequelize, Sequelize);
db.userProjectDQScore = require("./userProjectDQScore.js")(sequelize, Sequelize);







module.exports = db;




// const dbConfig = require("../../config/db.js");
// const Sequelize = require("sequelize");

// const sequelize = new Sequelize(dbConfig.DB, dbConfig.USER, dbConfig.PASSWORD, {
//   host: dbConfig.HOST,
//   dialect: dbConfig.dialect,
//   operatorsAliases: false,

//   pool: {
//     max: dbConfig.pool.max,
//     min: dbConfig.pool.min,
//     acquire: dbConfig.pool.acquire,
//     idle: dbConfig.pool.idle
//   }
// });

// const db = {};

// db.Sequelize = Sequelize;
// db.sequelize = sequelize;

// // Import all models
// db.Benchmarks = require("./benchmark.js")(sequelize, Sequelize);
// db.Frequencies = require("./frequency.js")(sequelize, Sequelize);
// db.Brands = require("./brand.js")(sequelize, Sequelize);
// db.Categories = require("./category.js")(sequelize, Sequelize);
// db.Metrics = require("./metrics.js")(sequelize, Sequelize);
// db.Platforms = require("./platform.js")(sequelize, Sequelize);
// db.Sections = require("./section.js")(sequelize, Sequelize);
// db.Users = require("./user.js")(sequelize, Sequelize);
// db.UserActivities = require("./userActivity.js")(sequelize, Sequelize);
// db.UserDetails = require("./userDetails.js")(sequelize, Sequelize);
// db.UserProjects = require("./userProjects.js")(sequelize, Sequelize);
// db.Payments = require("./payment.js")(sequelize, Sequelize);

// // Setup associations (if any)
// Object.keys(db).forEach(modelName => {
//   if (db[modelName].associate) {
//     db[modelName].associate(db);
//   }
// });

// module.exports = db;