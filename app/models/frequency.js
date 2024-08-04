module.exports = (sequelize, Sequelize) => {
  const Frequency = sequelize.define("frequencies", {
    name:{
      type: Sequelize.STRING
    }
  });

  return Frequency;
};
