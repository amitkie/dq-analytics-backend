module.exports = (sequelize, Sequelize) => {
    const UserActivity = sequelize.define("userActivities", {
      tab_page_speed: {
        type: Sequelize.INTEGER
      },
      tab_facebook_insight: {
        type: Sequelize.INTEGER
      },
      tab_google_analytics:{
        type:Sequelize.INTEGER
      },
      tab_google_ads:{
        type:Sequelize.INTEGER,
      },
      tab_dv360:{
        type:Sequelize.INTEGER,
      },
      is_db_created:{
        type:Sequelize.BOOLEAN,
      },
      is_schema_table_created:{
        type:Sequelize.BOOLEAN,
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
  
    return UserActivity;
  };