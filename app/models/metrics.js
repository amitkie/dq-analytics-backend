module.exports = (sequelize, Sequelize) => {
  const Metric = sequelize.define("metrics", {
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
    section_id: {
      type: Sequelize.INTEGER,
      allowNull: true,
      references: {
        model: 'sections', // Assuming your Category model is named 'Category'
        key: 'id'
      }
    },
  });

  Metric.associate = function(models) {
    Metric.belongsTo(models.Platform, { foreignKey: 'platform_id', as: 'platforms' });
    Metric.belongsTo(models.Section, { foreignKey: 'section_id', as: 'sections' });
  };

  return Metric;
};