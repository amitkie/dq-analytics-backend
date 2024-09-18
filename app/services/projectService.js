const { users, userProjects, metrics, platform, sections, frequencies, categories, brands , userUrls, userAnalytic, projectBenchmark} = require('../models');
const { Op , Sequelize} = require('sequelize');
 // Assuming your models are exported correctly


 async function createProject(userData) {
  const { project_name, user_id, metric_id, brand_id, category_id, frequency_id,start_date, end_date } = userData;

  try {
    // Find the user
    const user = await users.findOne({ where: { id: user_id } });
    if (!user) {
      throw new Error('User not found');
    }

    // Create the project
    const project = await userProjects.create({
      project_name,
      user_id,
      metric_id,
      brand_id,
      category_id,
      frequency_id,
      start_date,
      end_date
    });
    if (!project) {
      throw new Error('Project not created');
    }

    // Find metrics
    const metricData = await metrics.findAll({
      where: {
        id: {
          [Op.in]: metric_id
        }
      }
    });

    // Retrieve all platforms and sections
    const platforms = await platform.findAll();
    const section = await sections.findAll();
    
    // Convert platforms and sections to objects for easy lookup
    const platformMap = platforms.reduce((acc, p) => {
      acc[p.id] = p.toJSON();
      return acc;
    }, {});
    const sectionMap = section.reduce((acc, s) => {
      acc[s.id] = s.toJSON();
      return acc;
    }, {});

    // Format metrics
    const formattedMetrics = metricData.map(metric => {
      const platformData = platformMap[metric.platform_id] || null;
      const sectionData = sectionMap[metric.section_id] || null;

      return {
        ...metric.toJSON(),
        platform: platformData,
        section: sectionData
      };
    });

    console.log("Formatted Metrics:", formattedMetrics);

    // Calculate weight for each entry
    const numberOfEntries = formattedMetrics.length;
    const weight = numberOfEntries > 0 ? 100 / numberOfEntries : 0;

    // Store formatted data in UserAnalytics with calculated weights
    const userAnalyticsEntries = formattedMetrics.map(metric => ({
      project_id: project.id,
      metric_id: metric.id,
      platform_id: metric.platform ? metric.platform.id : null,
      section_id: metric.section ? metric.section.id : null,
      weights: weight // Assign the calculated weight
    }));

    await userAnalytic.bulkCreate(userAnalyticsEntries);

    return project;
  } catch (error) {
    throw new Error(`Error in createProject: ${error.message}`);
  }
}

// const getProjectById = async (projectId) => {
//   try {
//     // Fetch the project by ID
//     const project = await userProjects.findOne({
//       where: { id: projectId }
//     });

//     if (!project) {
//       throw new Error('Project not found');
//     }

//     // Fetch metric combinations from UserAnalytics
//     const userAnalyticsData = await userAnalytic.findAll({
//       where: {
//         project_id: projectId
//       }
//     });

//     // Extract metric_ids, platform_ids, and section_ids
//     const metricIds = [...new Set(userAnalyticsData.map(ua => ua.metric_id).filter(id => id != null))];
//     const platformIds = [...new Set(userAnalyticsData.map(ua => ua.platform_id).filter(id => id != null))];
//     const sectionIds = [...new Set(userAnalyticsData.map(ua => ua.section_id).filter(id => id != null))];

//     // Fetch metrics
//     const metricsData = await metrics.findAll({
//       where: {
//         id: {
//           [Op.in]: metricIds
//         }
//       }
//     });

//     // Fetch platforms
//     const platforms = await platform.findAll({
//       where: {
//         id: {
//           [Op.in]: platformIds
//         }
//       }
//     });

//     // Fetch sections
//     const sectionData = await sections.findAll({
//       where: {
//         id: {
//           [Op.in]: sectionIds
//         }
//       }
//     });

//     // Fetch frequency, category, and brand names based on IDs
//     const frequencyData = await frequencies.findAll({
//       where: {
//         id: {
//           [Op.in]: project.frequency_id
//         }
//       }
//     });

//     const categoryData = await categories.findAll({
//       where: {
//         id: {
//           [Op.in]: project.category_id
//         }
//       }
//     });

//     const brandData = await brands.findAll({
//       where: {
//         id: {
//           [Op.in]: project.brand_id
//         }
//       }
//     });

//     // Map platforms and sections to objects for easy lookup
//     const platformMap = platforms.reduce((acc, p) => {
//       acc[p.id] = p.toJSON();
//       return acc;
//     }, {});
//     const sectionMap = sectionData.reduce((acc, s) => {
//       acc[s.id] = s.toJSON();
//       return acc;
//     }, {});

//     // Map metrics to objects for easy lookup
//     const metricMap = metricsData.reduce((acc, m) => {
//       acc[m.id] = m.toJSON();
//       return acc;
//     }, {});

//     // Format metrics with platform, section, and metric details
//     const formattedMetrics = userAnalyticsData.map(ua => {
//       const metric = metricMap[ua.metric_id] || {};
//       const platform = platformMap[ua.platform_id] || null;
//       const section = sectionMap[ua.section_id] || null;

//       return {
//         metric_id: ua.metric_id,
//         metric_name: metric.name || null,
//         platform: platform,
//         section: section,
//         weights: ua.weights
//       };
//     });

//     // Omit specific ID fields from the project data
//     const { metric_id, brand_id, category_id, frequency_id, ...projectData } = project.toJSON();

//     // Include the metrics and other details in the project data
//     return {
//       ...projectData,
//       metrics: formattedMetrics,
//       frequencies: frequencyData.map(f => f.name),
//       categories: categoryData.map(c => c.name),
//       brands: brandData.map(b => b.name)
//     };
//   } catch (error) {
//     throw error;
//   }
// };


// const getProjectById = async (projectId) => {
//   try {
//     // Fetch the project by ID
//     const project = await userProjects.findOne({
//       where: { id: projectId }
//     });

//     if (!project) {
//       throw new Error('Project not found');
//     }

//     // if (project.is_benchmark_saved) {
//     //   throw new Error('Project Benchmark has already been saved, you cannot create another instance.');
//     // }

//     // Fetch metric combinations from UserAnalytics
//     const userAnalyticsData = await userAnalytic.findAll({
//       where: {
//         project_id: projectId
//       }
//     });

//     console.log("userAnalytics", userAnalyticsData)

   

//     // Extract metric_ids, platform_ids, and section_ids
//     const metricIds = [...new Set(userAnalyticsData.map(ua => ua.metric_id).filter(id => id != null))];
//     const platformIds = [...new Set(userAnalyticsData.map(ua => ua.platform_id).filter(id => id != null))];
//     const sectionIds = [...new Set(userAnalyticsData.map(ua => ua.section_id).filter(id => id != null))];

//     // Fetch metrics
//     const metricsData = await metrics.findAll({
//       where: {
//         id: {
//           [Op.in]: metricIds
//         }
//       }
//     });

//     console.log(metricsData, 'metricsData')

//     // Fetch platforms
//     const platforms = await platform.findAll({
//       where: {
//         id: {
//           [Op.in]: platformIds
//         }
//       }
//     });

//     console.log(platforms, 'platforms')


//     // Fetch sections
//     const sectionData = await sections.findAll({
//       where: {
//         id: {
//           [Op.in]: sectionIds
//         }
//       }
//     });

//     console.log(sectionData, 'sectionData')
    

//     // Fetch frequency, category, and brand names based on IDs
//     const frequencyData = await frequencies.findAll({
//       where: {
//         id: {
//           [Op.in]: project.frequency_id
//         }
//       }
//     });

//     const categoryData = await categories.findAll({
//       where: {
//         id: {
//           [Op.in]: project.category_id
//         }
//       }
//     });

//     const brandData = await brands.findAll({
//       where: {
//         id: {
//           [Op.in]: project.brand_id
//         }
//       }
//     });

//     // Map platforms and sections to objects for easy lookup
//     const platformMap = platforms.reduce((acc, p) => {
//       acc[p.id] = p.toJSON();
//       return acc;
//     }, {});
//     const sectionMap = sectionData.reduce((acc, s) => {
//       acc[s.id] = s.toJSON();
//       return acc;
//     }, {});

//     // Map metrics to objects for easy lookup
//     const metricMap = metricsData.reduce((acc, m) => {
//       acc[m.id] = m.toJSON();
//       return acc;
//     }, {});

//     // Format metrics with platform, section, and metric details, and add brands and categories
//     const formattedMetrics = userAnalyticsData.map(ua => {
//       const metric = metricMap[ua.metric_id] || {};
//       const platform = platformMap[ua.platform_id] || null;
//       const section = sectionMap[ua.section_id] || null;

//       return {
//         metric_id: ua.metric_id,
//         metric_name: metric.name || null,
//         platform: platform,
//         section: section,
//         weights: ua.weights,
//         brands: brandData.map(b => ({
//           id: b.id,
//           name: b.name
//         })),
//         categories: categoryData.map(c => ({
//           id: c.id,
//           name: c.name
//         }))
//       };
//     });

//     // Omit specific ID fields from the project data
//     const { metric_id, brand_id, category_id, frequency_id, ...projectData } = project.toJSON();

//     // Include the metrics and other details in the project data
//     return {
//       ...projectData,
//       metrics: formattedMetrics,
//       frequencies: frequencyData.map(f => f.name),
//       categories: categoryData.map(c => c.name),
//       brands: brandData.map(b => b.name)
//     };
//   } catch (error) {
//     throw error;
//   }
// };

const getProjectById = async (projectId) => {
  try {
    const project = await userProjects.findOne({
      where: { id: projectId },
      attributes: ['id', 'project_name', 'user_id', 'metric_id','is_benchmark_saved', 'brand_id', 'category_id', 'frequency_id', 'start_date', 'end_date', 'createdAt', 'updatedAt']
    });

    if (!project) {
      throw new Error('Project not found');
    }
    console.log('project---------------------',project)
    const userAnalyticsData = await userAnalytic.findAll({
      where: {
        project_id: projectId,
        metric_id: {
          [Op.in]: project.metric_id 
        }
      },
      attributes: [
        'metric_id', 'platform_id', 'section_id', 'weights'
      ]
    });

    const metricIds = [...new Set(userAnalyticsData.map(ua => ua.metric_id).filter(id => id != null))];
    const platformIds = [...new Set(userAnalyticsData.map(ua => ua.platform_id).filter(id => id != null))];
    const sectionIds = [...new Set(userAnalyticsData.map(ua => ua.section_id).filter(id => id != null))];

    const metricsData = await metrics.findAll({
      where: {
        id: {
          [Op.in]: metricIds
        }
      }
    });

    // Fetch platforms
    const platforms = await platform.findAll({
      where: {
        id: {
          [Op.in]: platformIds
        }
      }
    });

    const sectionData = await sections.findAll({
      where: {
        id: {
          [Op.in]: sectionIds
        }
      }
    });

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

    const platformMap = platforms.reduce((acc, p) => {
      acc[p.id] = p.toJSON();
      return acc;
    }, {});
    const sectionMap = sectionData.reduce((acc, s) => {
      acc[s.id] = s.toJSON();
      return acc;
    }, {});

 
    const metricMap = metricsData.reduce((acc, m) => {
      acc[m.id] = m.toJSON();
      return acc;
    }, {});

    const formattedMetrics = userAnalyticsData?.map(ua => {
      const metric = metricMap[ua.metric_id] || {};
      const platform = platformMap[ua.platform_id] || null;
      const section = sectionMap[ua.section_id] || null;

      return {
        metric_id: ua.metric_id,
        metric_name: metric.name || null,
        platform: platform,
        section: section,
        weights: ua.weights,
        categories: categoryData.map(c => ({ id: c.id, name: c.name })),
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
      brands: brandData.map(b => b.name),
      isBenchmarkDataSaved: project?.is_benchmark_saved
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

  const createOrUpdateUrls = async (userId, tabName, urls) => {
    try {
      const [userUrl] = await userUrls.findOrCreate({
        where: { user_id: userId },
        defaults: { tab_name: tabName, urls: [] },
      });
  
      // Update the URLs
      userUrl.tab_name = tabName;
      userUrl.urls = [...new Set([...userUrl.urls, ...urls])]; // Prevent duplicate URLs
      await userUrl.save();
  
      return {
        message:  'URLs created successfully' ,
        urls: userUrl.urls,
      };
    } catch (error) {
      console.error('Error in createOrUpdateUrls service:', error);
      throw new Error('Error creating or updating URLs');
    }
  };

  const getUrlsByUserId = async (userId) => {
    try {
      const userUrl = await userUrls.findOne({
        where: { user_id: userId }
      });
  
      if (!userUrl) {
        throw new Error('User URL entry not found');
      }
  
      return {
        tabName: userUrl.tab_name,
        urls: userUrl.urls,
      };
    } catch (error) {
      console.error('Error in getUrlsByUserId service:', error);
      throw new Error('Error fetching URLs');
    }
  };

  const saveMetrics = async (data) => {
    console.log(data, "choceed data");
    
    try {
        const newMetrics = await projectBenchmark.bulkCreate(data);
        if(newMetrics){
          const projectId = data[0].project_id;
          if(projectId){
            const projectData = await userProjects.findOne({where: {id: projectId}});

            if(projectData){
              projectData.is_benchmark_saved = true;
          
              // Save the updated project data
              await projectData.save();
            }
          }
        }
        return  newMetrics;
    } catch (error) {
        console.error('Error in metrics service:', error);
        throw new Error(error);
    }}

  

  


module.exports = {
    createProject,
    getProjectById,
    getProjectByUserId,
    createOrUpdateUrls,
    saveMetrics,
    getUrlsByUserId
};
