module.exports = (sequelize, Sequelize) => {
    const SuperThemeMetricGroup = sequelize.define("superThemeMetricGroups", {
      name: {
        type: Sequelize.STRING
      },
      project_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'userProjects', 
          key: 'id'
        }
      },
      metric_ids: {
        type: Sequelize.ARRAY(Sequelize.STRING),
      },
      metric_group_ids: {
        type: Sequelize.ARRAY(Sequelize.STRING),
      }
    });

     
    SuperThemeMetricGroup.associate = function(models) {
        SuperThemeMetricGroup.belongsTo(models.User, { foreignKey: 'project_id', as: 'userProjects' });
       
      };
  
    return SuperThemeMetricGroup;
  };