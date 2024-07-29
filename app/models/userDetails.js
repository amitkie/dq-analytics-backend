module.exports = (sequelize, Sequelize) => {
    const UserDetails = sequelize.define("userDetails", {
      name: {
        type: Sequelize.STRING
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
  
    return UserDetails;
  };