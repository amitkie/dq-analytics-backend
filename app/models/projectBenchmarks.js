module.exports = (sequelize, Sequelize) => {
    // ProjectBenchmarks model
    const ProjectBenchmarks = sequelize.define("project_benchmarks", {
      project_id: {
        type: Sequelize.INTEGER,
        allowNull: true,
        references: {
          model: 'userProjects', 
          key: 'id'
        }
      },
      sectionId:{
        type:Sequelize.INTEGER,
        allowNull:false
      },
      platformId:{
        type:Sequelize.INTEGER,
        allowNull:false
      },
      isOverall: {
        type: Sequelize.BOOLEAN,
        defaultValue: false,
      },
      isCategory: {
        type: Sequelize.BOOLEAN,
        defaultValue: false,
      },
      metricId: {
        type: Sequelize.INTEGER,
        allowNull: true
      },
      weights: {
        type: Sequelize.FLOAT,
        allowNull: true
      },
      categoryIds:{
        type: Sequelize.ARRAY(Sequelize.INTEGER),
        allowNull:true
      },
      brandIds:{
        type: Sequelize.ARRAY(Sequelize.INTEGER),
        allowNull:true
      },
      benchmarks: {
        type: Sequelize.JSON,
        allowNull: true
      },
      created_at: {
        type: Sequelize.DATE,
        defaultValue: Sequelize.NOW,
      },
      updated_at: {
        type: Sequelize.DATE,
        defaultValue: Sequelize.NOW,
      }
    });
  
    // Create the associations for many-to-many
    ProjectBenchmarks.associate = function(models) {
      // Many-to-Many with Sections
      ProjectBenchmarks.belongsToMany(models.Section, {
        through: 'ProjectBenchmarkSections',
        foreignKey: 'projectBenchmark_id',
        otherKey: 'section_id',
      });
  
      // Many-to-Many with Platforms
      ProjectBenchmarks.belongsToMany(models.Platform, {
        through: 'ProjectBenchmarkPlatforms',
        foreignKey: 'projectBenchmark_id',
        otherKey: 'platform_id',
      });
    };
  
    return ProjectBenchmarks;
  };
  