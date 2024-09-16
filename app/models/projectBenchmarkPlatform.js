module.exports = (sequelize, Sequelize) => {
    const ProjectBenchmarkPlatforms = sequelize.define('ProjectBenchmarkPlatforms', {
      projectBenchmark_id: {
        type: Sequelize.INTEGER,
        references: {
          model: 'projectBenchmarks',
          key: 'id'
        },
        onDelete: 'CASCADE'
      },
      platform_id: {
        type: Sequelize.INTEGER,
        references: {
          model: 'platforms',
          key: 'id'
        },
        onDelete: 'CASCADE'
      }
    });
  
    return ProjectBenchmarkPlatforms;
  };
  