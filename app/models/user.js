module.exports = (sequelize, Sequelize) => {
  const User = sequelize.define("User", {
    first_name: {
      type: Sequelize.STRING
    },
    last_name: {
      type: Sequelize.STRING
    },
    domain:{
      type:Sequelize.STRING
    },
    email:{
      type:Sequelize.STRING,
    },
    password:{
      type:Sequelize.STRING
    }
  });

  return User;
};