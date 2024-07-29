module.exports = (sequelize, Sequelize) => {
  const Benchmark = sequelize.define("benchmark", {
    title: {
      type: Sequelize.STRING
    },
    description: {
      type: Sequelize.STRING
    },
    published: {
      type: Sequelize.BOOLEAN
    }
  });

  return Benchmark;
};