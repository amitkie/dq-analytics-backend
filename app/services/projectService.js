const { users,metricGroup, userProjects, metrics, platform, sections, frequencies, categories, brands, userUrls, userAnalytic, projectBenchmark, userProjectDQScore, superThemeMetricGroup } = require('../models');
const { Op, Sequelize } = require('sequelize');


async function createProject(userData) {
  const { project_name, user_id, metric_id, brand_id, category_id, frequency_id, start_date, end_date } = userData;

  try {
    // Find the user
    const existingProject = await userProjects.findOne({
      where: { project_name },
    });
    if (existingProject) {
      throw new Error('Project name already exists. Please choose a different name.');
    }

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
async function createProject(userData) {
  const { project_name, user_id, metric_id, brand_id, category_id, frequency_id, start_date, end_date } = userData;

  try {
    // Find the user
    const existingProject = await userProjects.findOne({
      where: { project_name },
    });
    if (existingProject) {
      throw new Error('Project name already exists. Please choose a different name.');
    }

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
async function updateProject(projectId, projectData) {
  try {
    // Find the project by its ID
    const project = await userProjects.findByPk(projectId);

    if (!project) {
      throw new Error("Project not found");
    }

    // Update the project with new data
    await project.update(projectData);

    // Return the updated project details
    return project;
  } catch (error) {
    throw new Error(`Error updating project: ${error.message}`);
  }
}


async function deleteProject(projectId) {
  try {
    // Find the project by its ID
    const project = await userProjects.findByPk(projectId);  // Assuming Project is the correct model name

    if (!project) {
      throw new Error("Project not found");
    }

    // Delete the project
    await project.destroy();

    // Return the deleted project details (or confirmation message)
    return { message: 'Project deleted successfully', projectId };
  } catch (error) {
    throw new Error(`Error deleting project: ${error.message}`);
  }
}

const removeMetricFromProject = async (projectId, metricId) => {
  try {
    
    // Find the project by ID
    const project = await userProjects.findOne({ where: { id: projectId } });
    if (!project) {
      throw new Error('Project not found');
    }



    // Check if the metric exists in the metric_id array
    const metricArray = project.metric_id;
    const metricIdInt = parseInt(metricId, 10); // Ensure metricId is an integer


    if (!metricArray.includes(metricIdInt)) {
      throw new Error('Metric not found in this project');
    }

    // Remove the metric from the array
    const updatedMetricArray = metricArray.filter(id => id !== metricIdInt);

    // Update the project with the new metric_id array
    await project.update({ metric_id: updatedMetricArray });

    return { message: 'Metric removed successfully from the project' };
  } catch (error) {
    throw new Error(`Error in removeMetricFromProject: ${error.message}`);
  }
};


const checkProjectNameAvailability = async (project_name) => {
  try {
    const existingProject = await userProjects.findOne({
      where: { project_name },
    });

    if (existingProject) {
      return { available: false, message: 'Project name already exists.' };
    }
    return { available: true };
  } catch (error) {
    throw new Error(`Error checking project name availability: ${error.message}`);
  }
};

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
      attributes: ['id', 'project_name', 'user_id', 'metric_id', 'is_benchmark_saved', 'brand_id', 'category_id', 'frequency_id', 'start_date', 'end_date', 'createdAt', 'updatedAt']
    });

    if (!project) {
      throw new Error('Project not found');
    }
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
        brands: brandData.map(b => ({ id: b.id, name: b.name })),
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
      where: { user_id: user_id },
      order: [['createdAt', 'DESC']]
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
      message: 'URLs created successfully',
      urls: userUrl.urls,
    };
  } catch (error) {
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
    throw new Error('Error fetching URLs');
  }
};

const saveMetrics = async (data) => {

  try {
    const newMetrics = await projectBenchmark.bulkCreate(data);
    if (newMetrics) {
      const projectId = data[0].project_id;
      if (projectId) {
        const projectData = await userProjects.findOne({ where: { id: projectId } });

        if (projectData) {
          projectData.is_benchmark_saved = true;

          // Save the updated project data
          await projectData.save();
        }
      }
    }
    return newMetrics;
  } catch (error) {
    throw new Error(error);
  }
}

const getProjectByIds = async (user_id,frequency_id, category_ids) => {
  try {
    const query = {
      user_id: user_id 
    };
    
    if (frequency_id) {
      query.frequency_id = { [Op.contains]: [frequency_id] };
    }

    if (category_ids && Array.isArray(category_ids)) {
      query.category_id = { [Op.contains]: category_ids };
    }

    const projects = await userProjects.findAll({
      where: query,
    });

    return projects;
  } catch (error) {
    throw new Error("Error fetching projects");
  }
};

const getDateRangeByUserId = async(user_id, frequency_id) => {
  const query = {
    user_id: user_id
  }
  if (frequency_id) {
    query.frequency_id = { [Op.contains]: [frequency_id] };
  }
  
  const projectsData = await userProjects.findAll(
   { where:query}
  )
}

const createUserProjectDQScore = async (dqScoreArray) => {
  const scoreDataArray = dqScoreArray.map(score => ({
      user_id: score.user_id,
      project_id: score.project_id,
      brand_id: score.brand_id,
      brand_name: score.brand_name,
      section_name: score.section_name,
      section_id: score.section_id,
      category_id: score.category_id,
      category_name: score.category_name,
      dq: score.dq,
      ecom_dq: score.ecom_dq,
      social_dq: score.social_dq,
      paid_dq: score.paid_dq,
      brand_perf_dq: score.brand_perf_dq,
      createdAt: new Date(),
      updatedAt: new Date()
  }));

  // Perform bulk insert operation using Sequelize
  try {
      const newScores = await userProjectDQScore.bulkCreate(scoreDataArray, { returning: true });
      return newScores;
  } catch (error) {
      // Throw the error to be handled by the controller
      throw new Error(`Failed to create DQ scores: ${error.message}`);
  }
};

const saveGroupMetric = async({ name, project_id, metric_ids }) => {
  try {
    const newMetricGroup = await metricGroup.create({
      name,
      project_id,
      metric_ids
    });

    return newMetricGroup;
  } catch (error) {
    console.log(error);
    
    throw new Error('Error saving Metric Group');
  }
}

const getGroupMetrics = async (projectId) => {
  try {
    const metricGroups = await metricGroup.findAll({
      where: { project_id: projectId },
    });

    return metricGroups;
  } catch (error) {
    throw new Error('Error fetching Metric Groups');
  }
};

const saveGroupMetricTheme = async ({ name, project_id, metric_ids, metric_group_ids }) => {
  try {
    const newMetricGroup = await superThemeMetricGroup.create({
      name,
      project_id,
      metric_ids,
      metric_group_ids
    });

    return newMetricGroup;
  } catch (error) {
    throw new Error('Error saving Metric Group: ' + error.message);
  }
};

const getMetricGroupsByProjectId = async (project_id) => {
  try {
    // Fetch metric groups by project_id
    const metricGroups = await superThemeMetricGroup.findAll({
      where: { project_id }
    });

    return metricGroups;
  } catch (error) {
    throw new Error('Error fetching Metric Groups: ' + error.message);
  }
};



module.exports = {
  createProject,
  checkProjectNameAvailability,
  getProjectById,
  getProjectByUserId,
  createOrUpdateUrls,
  createUserProjectDQScore,
  saveMetrics,
  getUrlsByUserId,
  getProjectByIds,
  updateProject,
  deleteProject,
  removeMetricFromProject,
  saveGroupMetric,
  getGroupMetrics,
  saveGroupMetricTheme,
  getMetricGroupsByProjectId
};
