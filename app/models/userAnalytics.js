module.exports = (sequelize, Sequelize) => {
    const UserAnalytics = sequelize.define("userAnalytics", {
        project_id:{
            type: Sequelize.INTEGER,
            allowNull: false,
            references: {
              model: 'userProjects', 
              key: 'id'
            }
        },
      metric_id: {
        type: Sequelize.INTEGER,
        allowNull: true,
        references: {
            model: 'metrics', 
            key: 'id'
          }
      },
      platform_id: {
        type: Sequelize.INTEGER,
        allowNull: true,
        references: {
            model: 'platforms', 
            key: 'id'
          }
      },
      section_id: {
        type: Sequelize.INTEGER,
        allowNull: true,
        references: {
            model: 'sections', 
            key: 'id'
          }
      },
      weights:{
        type: Sequelize.FLOAT,
        allowNull: true,
      }

    });

    UserAnalytics.associate = function(models) {
      UserAnalytics.belongsTo(models.userProjects, { foreignKey: 'project_id', as: 'user' });
  
      UserAnalytics.belongsTo(models.metrics, {
        foreignKey: 'metric_id',
        as: 'metrics'
      });


      UserAnalytics.belongsTo(models.platforms, {
        foreignKey: 'platform_id',
        as: 'platforms'
      });
  
      UserAnalytics.belongsTo(models.sections, {
        foreignKey: 'section_id',
        as: 'sections'
      });
    };
  
    return UserAnalytics;
  };