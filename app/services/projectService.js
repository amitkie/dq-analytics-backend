const { users, userProjects } = require('../models'); // Assuming your models are exported correctly

async function createProject(userData) {
    const { project_name, user_id, metric_id, brand_id, category_id, frequency_id, platform_id } = userData;
    try {
        console.log(user_id, "-----------------------------")
        const user = await users.findOne({ where: { id:user_id } });
        if (!user) {
            throw new Error('User not found');
        }

        const project = await userProjects.create({
            project_name,
            user_id,
            metric_id,
            brand_id,
            category_id,
            frequency_id,
            platform_id
          });
          if(!project){
            throw new Error('Project not Created');
          }
          return project;
    } catch (error) {
        throw new Error(error.message);
    }

}


module.exports = {
    createProject
};
