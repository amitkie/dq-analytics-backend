module.exports = (sequelize, Sequelize) => {
  const Platform = sequelize.define("platforms", {
    name: {
      type: Sequelize.STRING
    },
  });

  return Platform;
};