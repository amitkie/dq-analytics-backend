module.exports = (sequelize, Sequelize) => {
    const Payment = sequelize.define("payments", {
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
  
    return Payment;
  };