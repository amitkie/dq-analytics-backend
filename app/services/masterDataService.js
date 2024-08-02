

const {categories, brands, platform, benchmarks, metrics, frequencies, sections} = require('../models/index')

const { ValidationError } = require("../handlers/errorHandler");

const getAllCategories = async () => {
  try {
    const categoryData = await categories.findAll();
    console.log(categoryData, "hugug");
    if (!categoryData) {
      throw new ValidationError("CATEGORY_NOT_FOUND", "Data not found.");
    }
    return categoryData;
  } catch (error) {
    console.log(error, "erororooroororoororoororo");
    throw error;
  }
};


const getAllBrands = async () => {
  try {
    const brandData = await brands.findAll();
    if (!brandData) {
      throw new ValidationError("BRAND_NOT_FOUND", "Data not found.");
    }
    return brandData;
  } catch (error) {
    throw error;
  }
};

// const getBrandsByCategoryId = async (categoryId) => {
//   try {
//     const brandsData = await brands.findAll({where: {category_id: categoryId}});
//     if(!brandsData){
//       throw new ValidationError("BRANDS_NOT_FOUND", "Data not found.");
//     }
//     return brandsData;
//   } catch (error) {
//     throw error;
//   }
// }

const { Op } = require('sequelize');

const getBrandsByCategoryIds = async (categoryIds) => {
  try {
    const brandsData = await brands.findAll({
      where: {
        category_id: {
          [Op.in]: categoryIds
        }
      }
    });

    if (!brandsData || brandsData.length === 0) {
      throw new ValidationError("BRANDS_NOT_FOUND", "Data not found.");
    }

    return brandsData;
  } catch (error) {
    throw error;
  }
}


const getPlatformsBySectionId = async (sectionId) => {
  try {
    const platformData = await platform.findAll({where: {section_id: sectionId}});
    if(!platformData){
      throw new ValidationError("PLATFORM_NOT_FOUND", "Data not found.");
    }
    return platformData;
  } catch (error) {
    throw error;
  }
}

const getAllSection = async () => {
  try {
    const sectionData = await sections.findAll();
    if (!sectionData) {
      throw new ValidationError("SECTION_NOT_FOUND", "Data not found.");
    }
    return sectionData;
  } catch (error) {
    throw error;
  }
}
const getAllPlatform = async () => {
  try {
    const platformData = await platform.findAll();
    if (!platformData) {
      throw new ValidationError("PLATFORM_NOT_FOUND", "Data not found.");
    }
    return platformData;
  } catch (error) {
    throw error;
  }
};
const getAllBenchmark = async () => {
  try {
    const benchmarkData = await benchmarks.findAll();
    if (!benchmarkData) {
      throw new ValidationError("BENCHMARK_NOT_FOUND", "Data not found.");
    }
    return benchmarkData;
  } catch (error) {
    throw error;
  }
};
const getAllMetrics = async () => {
  try {
    const metricsData = await metrics.findAll();
    if (!metricsData) {
      throw new ValidationError("METRICS_NOT_FOUND", "Data not found.");
    }
    return metricsData;
  } catch (error) {
    throw error;
  }
};

// const getMetricsByPlatformIds = async (platformIds) => {
//   try {
//     const metricsData = await metrics.findAll({where: {platform_id: platformId}});
//     if(!metricsData){
//       throw new ValidationError("PLATFORM_NOT_FOUND", "Data not found.");
//     }
//     return metricsData;
//   } catch (error) {
//     throw error;
//   }
// }
const getMetricsByPlatformIds = async (platformIds) => {
  try {
    const metricsData = await metrics.findAll({
      where: {
        platform_id: {
          [Op.in]: platformIds
        }
      }
    });

    if (!metricsData.length) {
      throw new ValidationError("PLATFORM_NOT_FOUND", "Data not found.");
    }
    
    return metricsData;
  } catch (error) {
    throw error;
  }
};

const getAllFrequency = async () => {
  try {
    const frequencyData = await frequencies.findAll();
    if (!frequencyData) {
      throw new ValidationError("FREQUENCY_NOT_FOUND", "Data not found.");
    }
    return frequencyData;
  } catch (error) {
    throw error;
  }
};

module.exports = {
  getAllCategories,
  getAllBrands,
  getBrandsByCategoryIds,
  getAllSection,
  getPlatformsBySectionId,
  getAllPlatform,
  getAllBenchmark,
  getAllMetrics,
  getMetricsByPlatformIds,
  getAllFrequency,
};
