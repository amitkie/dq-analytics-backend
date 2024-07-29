const dbConfig = require("../../config/db.js");

const Sequelize = require("sequelize");
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

const db = {};

db.Sequelize = Sequelize;
db.sequelize = sequelize;

db.benchmarks = require("./benchmark.js")(sequelize, Sequelize);
db.frequencies = require("./frequency.js")(sequelize, Sequelize);
db.brands = require("./brand.js")(sequelize, Sequelize);
db.categories = require("./category.js")(sequelize, Sequelize);
db.metrics = require("./metrics.js")(sequelize, Sequelize);
db.platform = require("./platform.js")(sequelize, Sequelize);
db.brands = require("./brand.js")(sequelize, Sequelize);
db.sections = require("./section.js")(sequelize, Sequelize);
db.users = require("./user.js")(sequelize, Sequelize);
db.userActivities = require("./userActivity.js")(sequelize, Sequelize);
db.userDetails = require("./userDetails.js")(sequelize, Sequelize);
db.userProjects = require("./userProjects.js")(sequelize, Sequelize);
db.payments = require("./payment.js")(sequelize, Sequelize);


module.exports = db;