module.exports = (sequelize, Sequelize) => {
    const Section = sequelize.define("sections", {
      name: {
        type: Sequelize.STRING
      },
      platform_id: {
        type: Sequelize.INTEGER,
        allowNull: true,
        references: {
          model: 'platforms', // Assuming your Category model is named 'Category'
          key: 'id'
        }
      },
    });
  
    return Section;
  };