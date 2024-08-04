module.exports = (sequelize, Sequelize) => {
  const Brand = sequelize.define("brands", {
    name: {
      type: Sequelize.STRING
    },
    category_id: {
      type: Sequelize.INTEGER,
      allowNull: false,
      references: {
        model: 'categories', // Assuming your Category model is named 'Category'
        key: 'id'
      }
    },
  });

  return Brand;
};