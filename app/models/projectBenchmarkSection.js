module.exports = (sequelize, Sequelize) => {
    const ProjectBenchmarkSections = sequelize.define('ProjectBenchmarkSections', {
      projectBenchmark_id: {
        type: Sequelize.INTEGER,
        references: {
          model: 'projectBenchmarks',
          key: 'id'
        },
        onDelete: 'CASCADE'
      },
      section_id: {
        type: Sequelize.INTEGER,
        references: {
          model: 'sections',
          key: 'id'
        },
        onDelete: 'CASCADE'
      }
    });
  
    return ProjectBenchmarkSections;
  };
  