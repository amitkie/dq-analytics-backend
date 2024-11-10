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
    if (!categoryData) {
      throw new ValidationError("CATEGORY_NOT_FOUND", "Data not found.");
    }
  
    return categoryData;
  } catch (error) {
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


const getPlatformsBySectionId = async (sectionIds) => {
  console.log(sectionIds, 'sectionIds');
  try {
    // Step 1: Find the names of the sections by given sectionIds
    const initialSections = await sections.findAll({
      where: {
        id: {
          [Op.in]: sectionIds,
        },
      },
    });

    if (!initialSections || initialSections.length === 0) {
      throw new ValidationError("SECTION_NOT_FOUND", "No sections found.");
    }

    // Extract unique section names from the initial sections
    const sectionNames = [...new Set(initialSections.map(section => section.name))];

    // Step 2: Find all sections that have these names, regardless of ID
    const allMatchingSections = await sections.findAll({
      where: {
        name: {
          [Op.in]: sectionNames,
        },
      },
    });

    if (!allMatchingSections || allMatchingSections.length === 0) {
      throw new ValidationError("SECTION_NOT_FOUND", "No matching sections found.");
    }

    // Step 3: Collect all unique platform IDs associated with these sections
    const platformIds = [...new Set(allMatchingSections.map(section => section.platform_id).filter(Boolean))];

    if (platformIds.length === 0) {
      throw new ValidationError("PLATFORM_ID_NOT_FOUND", "No platform IDs associated with the sections.");
    }

    // Step 4: Fetch platform details by platformIds
    const platforms = await platform.findAll({
      where: {
        id: {
          [Op.in]: platformIds,
        },
      },
    });

    if (!platforms || platforms.length === 0) {
      throw new ValidationError("PLATFORM_NOT_FOUND", "No platforms found.");
    }

    // Map platforms by ID for easy lookup
    const platformMap = platforms.reduce((acc, p) => {
      acc[p.id] = p.toJSON();
      return acc;
    }, {});

    // Step 5: Create a flat list of section and platform details
    const result = allMatchingSections.map(section => ({
      sectionId: section.id,
      sectionName: section.name,
      platformId: section.platform_id,
      platformName: platformMap[section.platform_id] ? platformMap[section.platform_id].name : null,
    }));

    return result;
  } catch (error) {
    console.log(error);
    throw error;
  }
};


// const getPlatformsBySectionId = async (sectionIds) => {
//   console.log(sectionIds, 'sectionIds')
//   try {
//     // Step 1: Find all sections by sectionIds
//     const sectionsData = await sections.findAll({
//       where: {
//         id: {
//           [Op.in]: sectionIds,
//         },
//       },
//     });

//     console.log(sectionsData, 'sectionsData')

//     if (!sectionsData || sectionsData.length === 0) {
//       throw new ValidationError("SECTION_NOT_FOUND", "No sections found.");
//     }

//     // Step 2: Extract platform IDs from the sections
//     const platformIds = sectionsData.map(section => section.platform_id).filter(Boolean);

//     if (!platformIds.length) {
//       throw new ValidationError("PLATFORM_ID_NOT_FOUND", "No platform IDs associated with the sections.");
//     }

//     // Step 3: Fetch platform details by platformIds
//     const platforms = await platform.findAll({
//       where: {
//         id: {
//           [Op.in]: platformIds,
//         },
//       },
//     });

//     if (!platforms || platforms.length === 0) {
//       throw new ValidationError("PLATFORM_NOT_FOUND", "No platforms found.");
//     }

//     // Step 4: Structure and return the platform data with section info
//     const result = sectionsData.map(section => {
//       const platform = platforms.find(p => p.id === section.platform_id);
//       return {
//         sectionId: section.id,
//         sectionName: section.name,
//         platformId: section.platform_id,
//         platformName: platform ? platform.name : null,
//       };
//     });

//     return result;
//   } catch (error) {
//     console.log(error)
//     throw error;
//   }
// };

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

  try {

    const sectionsData = await sections.findAll({
      where: {
        platform_id: {
          [Op.in]: platformIds,
        },
      },
    });

    if (sectionsData.length === 0) {
      throw new ValidationError(
        "SECTIONS_NOT_FOUND",
        "No sections found for the provided platform IDs."
      );
    }

    return sectionsData;
  } catch (error) {
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
