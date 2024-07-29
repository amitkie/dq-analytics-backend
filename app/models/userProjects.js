module.exports = (sequelize, Sequelize) => {
    const UserProjects = sequelize.define("userProjects", {
      project_name: {
        type: Sequelize.STRING
      },
      file_url:{
        type: Sequelize.STRING
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
  
    return UserProjects;
  };