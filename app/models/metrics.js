module.exports = (sequelize, Sequelize) => {
  const Metric = sequelize.define("metric", {
    name: {
      type: Sequelize.STRING
    },
    platform_id: {
      type: Sequelize.INTEGER,
      allowNull: false,
      references: {
        model: 'platforms', // Assuming your Category model is named 'Category'
        key: 'id'
      }
    },
  });

  return Metric;
};