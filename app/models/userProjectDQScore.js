module.exports = (sequelize, Sequelize) => {
    const UserProjectDQScore = sequelize.define("userProjectDQScore", {
      user_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'users', 
          key: 'id'
        }
      },
      project_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'userProjects', 
          key: 'id'
        }
      },
      brand_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'brands',
          key: 'id'
        }
      },
      brand_name: {
        type: Sequelize.STRING,
        allowNull: true
      },
      section_name: {
        type: Sequelize.STRING,
        allowNull: true
      },
      section_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'sections',
          key: 'id'
        }
      },
      category_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'categories',
          key: 'id'
        }
      },
      category_name: {
        type: Sequelize.STRING,
        allowNull: true
      },
      dq: {
        type: Sequelize.FLOAT,
        allowNull: true
      },
      ecom_dq: {
        type: Sequelize.FLOAT,
        allowNull: true
      },
      social_dq: {
        type: Sequelize.FLOAT,
        allowNull: true
      },
      paid_dq: {
        type: Sequelize.FLOAT,
        allowNull: true
      },
      brand_perf_dq: {
        type: Sequelize.FLOAT,
        allowNull: true
      },
    });
  
    UserProjectDQScore.associate = function(models) {
      UserProjectDQScore.belongsTo(models.User, { foreignKey: 'user_id', as: 'user' });
      UserProjectDQScore.belongsTo(models.User, { foreignKey: 'project_id', as: 'userProjects' });
      UserProjects.belongsToMany(models.Metric, {
        through: 'userProjectMetrics',
        foreignKey: 'metric_id',
        otherKey: 'id'
      });
  
      UserProjects.belongsToMany(models.Brand, {
        through: 'userProjectBrands',
        foreignKey: 'brand_id',
        otherKey: 'id'
      });

      UserProjects.belongsToMany(models.Category, {
        through: 'userProjectCategories',
        foreignKey: 'category_id',
        otherKey: 'id'
      });
    };
  
    return UserProjectDQScore;
  };
  