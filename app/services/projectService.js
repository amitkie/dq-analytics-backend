const { users, userProjects, metrics, platform, sections, frequencies, categories, brands } = require('../models');
const { Op } = require('sequelize');
 // Assuming your models are exported correctly

async function createProject(userData) {
    const { project_name, user_id, metric_id, brand_id, category_id, frequency_id, platform_id } = userData;
    try {
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
          });
          if(!project){
            throw new Error('Project not Created');
          }
          return project;
    } catch (error) {
        throw new Error(error.message);
    }

}
const getProjectById = async (projectId) => {
    try {
      // Fetch the project by ID
      const project = await userProjects.findOne({
        where: { id: projectId }
      });
  
      if (!project) {
        throw new Error('Project not found');
      }
  
      // Fetch the associated metrics based on the metric_id array in UserProjects
      const metricData = await metrics.findAll({
        where: {
          id: {
            [Op.in]: project.metric_id
          }
        }
      });
  
      // Extract platform_ids and section_ids
      const platformIds = [...new Set(metricData.map(metric => metric.platform_id).filter(id => id != null))];
      const sectionIds = [...new Set(metricData.map(metric => metric.section_id).filter(id => id != null))];
  
      // Fetch platforms
      const platforms = await platform.findAll({
        where: {
          id: {
            [Op.in]: platformIds
          }
        }
      });
  
      // Fetch sections
      const sectionData = await sections.findAll({
        where: {
          id: {
            [Op.in]: sectionIds
          }
        }
      });
  
      // Fetch frequency, category, and brand names based on IDs
      const frequencyData = await frequencies.findAll({
        where: {
          id: {
            [Op.in]: project.frequency_id
          }
        }
      });
  
      const categoryData = await categories.findAll({
        where: {
          id: {
            [Op.in]: project.category_id
          }
        }
      });
  
      const brandData = await brands.findAll({
        where: {
          id: {
            [Op.in]: project.brand_id
          }
        }
      });
  
      // Format metrics with platform and section details
      const formattedMetrics = metricData.map(metric => {
        const platform = platforms.find(p => p.id === metric.platform_id);
        const section = sectionData.find(s => s.id === metric.section_id);
  
        return {
          ...metric.toJSON(),
          platform: platform ? platform.toJSON() : null,
          section: section ? section.toJSON() : null,
        };
      });
  
      // Omit specific ID fields from the project data
      const { metric_id, brand_id, category_id, frequency_id, ...projectData } = project.toJSON();
  
      // Include the metrics and other details in the project data
      return {
        ...projectData,
        metrics: formattedMetrics,
        frequencies: frequencyData.map(f => f.name),
        categories: categoryData.map(c => c.name),
        brands: brandData.map(b => b.name)
      };
    } catch (error) {
      throw error;
    }
  };


  const getProjectByUserId = async (user_id) => {
    try {
      const user = await users.findOne({ where: { id: user_id } });
  
      if (!user) {
        throw new Error('User not Found');
      }
  
      let projectData = await userProjects.findAll({
        where: { user_id: user_id }
      });
  
      // Retrieve names for frequency, category, metric, and brand
      for (let project of projectData) {
        if (project.frequency_id) {
          const frequenciesData = await frequencies.findAll({ 
            where: { id: project.frequency_id },
            attributes: ['name']
          });
          project.dataValues.frequencyNames = frequenciesData.map(frequency => frequency.name);
        }
  
        if (project.category_id) {
          const categoriesData = await categories.findAll({
            where: { id: project.category_id },
            attributes: ['name']
          });
          project.dataValues.categoryNames = categoriesData.map(category => category.name);
        }
  
        if (project.metric_id) {
          const metricsData = await metrics.findAll({
            where: { id: project.metric_id },
            attributes: ['name']
          });
          project.dataValues.metricNames = metricsData.map(metric => metric.name);
        }
  
        if (project.brand_id) {
          const brandsData = await brands.findAll({
            where: { id: project.brand_id },
            attributes: ['name']
          });
          project.dataValues.brandNames = brandsData.map(brand => brand.name);
        }
  
        // Remove the original ID arrays
        delete project.dataValues.metric_id;
        delete project.dataValues.brand_id;
        delete project.dataValues.category_id;
        delete project.dataValues.frequency_id;
      }
  
      return projectData;
  
    } catch (error) {
      throw new Error(error.message);
    }
  }
  

  


module.exports = {
    createProject,
    getProjectById,
    getProjectByUserId
};
