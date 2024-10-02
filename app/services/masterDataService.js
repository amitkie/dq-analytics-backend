const {
  categories,
  brands,
  platform,
  benchmarks,
  metrics,
  frequencies,
  sections,
} = require("../models/index");
const { Op } = require("sequelize");
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
// const getAllBrandsWithCategories = async () => {
//   try {
//     const brandData = await brands.findAll({
//       include: [
//         {
//           model: categories, // This is the categories model imported dynamically
//           as: "category", // Define the alias for the join
//           required: false, // Set to false if you want to include brands with no associated category
//           attributes: ["id", "name"], // Select only the required columns
//           where: {
//             id: {
//               [Op.ne]: null, // Example condition if needed
//             },
//           },
//         },
//       ],
//     });

//     if (!brandData || brandData.length === 0) {
//       throw new ValidationError("BRAND_NOT_FOUND", "Data not found.");
//     }

//     return brandData.map((brand) => ({
//       brandName: brand.name,
//       category: brand.category ? brand.category.name : null, // Return the category name or null if not found
//     }));
//   } catch (error) {
//     console.error("Error in getAllBrandsWithCategories:", error);
//     throw error;
//   }
// };

const getAllBrandsWithCategories = async () => {
  try {
    const brandData = await brands.findAll({
      include: [
        {
          model: categories,
          // as: "category", // Make sure this matches the alias in the association
          required: false, // Set to false if you want to include brands with no associated category
          attributes: ["id", "name"], // Select only the required columns
        },
      ],
    });

    if (!brandData || brandData.length === 0) {
      throw new ValidationError("BRAND_NOT_FOUND", "Data not found.");
    }

    return brandData.map((brand) => ({
      brandName: brand.name,
      category: brand.category ? brand.category.name : null, // Return the category name or null if not found
    }));
  } catch (error) {
    console.error("Error in getAllBrandsWithCategories:", error);
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

const getBrandsByCategoryIds = async (categoryIds) => {
  try {
    const brandsData = await brands.findAll({
      where: {
        category_id: {
          [Op.in]: categoryIds,
        },
      },
    });

    if (!brandsData || brandsData.length === 0) {
      throw new ValidationError("BRANDS_NOT_FOUND", "Data not found.");
    }

    return brandsData;
  } catch (error) {
    throw error;
  }
};

const getPlatformsBySectionId = async (sectionId) => {
  try {
    const platformData = await platform.findAll({
      where: { section_id: sectionId },
    });
    if (!platformData) {
      throw new ValidationError("PLATFORM_NOT_FOUND", "Data not found.");
    }
    return platformData;
  } catch (error) {
    throw error;
  }
};

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
};
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

const findSectionsByPlatformIds = async (platformIds) => {
  console.log("Platform IDs:", platformIds);

  try {
    console.log("Querying sections...");

    const sectionsData = await sections.findAll({
      where: {
        platform_id: {
          [Op.in]: platformIds,
        },
      },
    });

    console.log("Sections Data:", sectionsData);

    if (sectionsData.length === 0) {
      throw new ValidationError(
        "SECTIONS_NOT_FOUND",
        "No sections found for the provided platform IDs."
      );
    }

    return sectionsData;
  } catch (error) {
    console.error("Error fetching sections:", error);
    throw error;
  }
};

const getMetricsBySectionsAndPlatformIds = async (sections, platformIds) => {
  try {
    const sectionIds = sections.map((section) => section.id);

    const metricsData = await metrics.findAll({
      where: {
        platform_id: {
          [Op.in]: platformIds,
        },
        section_id: {
          [Op.in]: sectionIds,
        },
      },
    });

    if (!metricsData.length) {
      throw new ValidationError(
        "METRICS_NOT_FOUND",
        "No metrics found for the provided sections and platform IDs."
      );
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
  findSectionsByPlatformIds,
  getMetricsBySectionsAndPlatformIds,
  getAllFrequency,
  getAllBrandsWithCategories,
};
