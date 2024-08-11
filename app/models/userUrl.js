module.exports = (sequelize, Sequelize) => {
    const UserUrl = sequelize.define("userUrls", {
      tab_name: {
        type: Sequelize.STRING
      },

      urls: {
        type: Sequelize.ARRAY(Sequelize.STRING), // Array of strings
        defaultValue: []
      },

      user_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'users', 
          key: 'id'
        }
      },
    });

    return UserUrl;
};
