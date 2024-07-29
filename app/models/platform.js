module.exports = (sequelize, Sequelize) => {
  const Platform = sequelize.define("platform", {
    name: {
      type: Sequelize.STRING
    },
    section_id: {
      type: Sequelize.INTEGER,
      allowNull: false,
      references: {
        model: 'sections', // Assuming your Category model is named 'Category'
        key: 'id'
      }
    },
  });

  return Platform;
};