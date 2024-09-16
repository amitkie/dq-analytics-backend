module.exports = (sequelize, Sequelize) => {
    const UserProjects = sequelize.define("userProjects", {
      project_name: {
        type: Sequelize.STRING
      },
      is_benchmark_saved: {
        type: Sequelize.BOOLEAN,
        allowNull: false,
        defaultValue: false 
      },
      file_url:{
        type: Sequelize.STRING
      },
      metric_id: {
        type: Sequelize.ARRAY(Sequelize.INTEGER),
        allowNull: true
      },
      brand_id: {
        type: Sequelize.ARRAY(Sequelize.INTEGER),
        allowNull: true
      },
      category_id: {
      type: Sequelize.ARRAY(Sequelize.INTEGER),
      allowNull: true
      },
      frequency_id: {
        type: Sequelize.ARRAY(Sequelize.INTEGER),
        allowNull: true
        },
      start_date:{
        type: Sequelize.DATE,
        allowNull: true
      },
      end_date:{
        type: Sequelize.DATE,
        allowNull: true
      },
      user_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'users', 
          key: 'id'
        }
      },
    });

    UserProjects.associate = function(models) {
      UserProjects.belongsTo(models.User, { foreignKey: 'user_id', as: 'user' });
  
      UserProjects.belongsToMany(models.Metric, {
        through: 'UserProjectMetrics',
        foreignKey: 'metric_id',
        otherKey: 'id'
      });
  
      UserProjects.belongsToMany(models.Brand, {
        through: 'UserProjectBrands',
        foreignKey: 'brand_id',
        otherKey: 'id'
      });

      UserProjects.belongsToMany(models.Category, {
        through: 'UserProjectCategories',
        foreignKey: 'category_id',
        otherKey: 'id'
      });
  
      UserProjects.belongsToMany(models.Frequency, {
        through: 'UserProjectFrequencies',
        foreignKey: 'frequency_id',
        otherKey: 'id'
      });
    };
  
    return UserProjects;
  };