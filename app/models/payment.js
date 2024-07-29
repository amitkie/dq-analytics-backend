module.exports = (sequelize, Sequelize) => {
    const UserActivity = sequelize.define("payment", {
      subscription_name: {
        type: Sequelize.STRING
      },
      amount: {
        type: Sequelize.FLOAT
      },
      storage:{
        type:Sequelize.INTEGER
      },
      connection_allowed:{
        type:Sequelize.INTEGER,
      },
      payment_status:{
        type:Sequelize.STRING,
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
  
    return UserActivity;
  };