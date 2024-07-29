module.exports = (sequelize, Sequelize) => {
  const Frequency = sequelize.define("frequency", {
    name:{
      type: Sequelize.STRING
    }
  });

  return Frequency;
};
