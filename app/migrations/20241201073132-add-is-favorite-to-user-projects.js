'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    // Add the new column with a default value of false
    await queryInterface.addColumn('userProjects', 'is_favorite', {
      type: Sequelize.BOOLEAN,
      defaultValue: false,
      allowNull: false,
    });

    // Update all existing records to set 'is_favorite' to false (to prevent null values)
    await queryInterface.sequelize.query(
      'UPDATE "userProjects" SET "is_favorite" = false WHERE "is_favorite" IS NULL;'
    );
  },

  down: async (queryInterface, Sequelize) => {
    // Revert the changes by removing the 'is_favorite' column
    await queryInterface.removeColumn('userProjects', 'is_favorite');
  }
};
