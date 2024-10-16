module.exports = (sequelize, Sequelize) => {
    const MetricGroup = sequelize.define("metricGroups", {
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
      }
    });

     
    MetricGroup.associate = function(models) {
        MetricGroup.belongsTo(models.User, { foreignKey: 'project_id', as: 'userProjects' });
       
      };
  
    return MetricGroup;
  };