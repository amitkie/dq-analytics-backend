const { getAllCategories, getAllBrands, getAllBenchmark, getAllPlatform, getAllMetrics,getAllFrequency, getAllSection, getPlatformsBySectionId, getMetricsByPlatformId, getBrandsByCategoryIds, getMetricsByPlatformIds } = require("../services/masterDataService");
const { ValidationError } = require("../handlers/errorHandler");
const { createErrorResponse } = require("../utils/errorResponse");
const { createSuccessResponse } = require("../utils/successResponse");

const getAllCategoriesController = async(req,res) => {
    try {        
        const response = await getAllCategories();
        const successResponse = createSuccessResponse(200, 'Category found successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}
const getAllBrandsController = async(req,res) => {
    try {        
        const response = await getAllBrands();
        const successResponse = createSuccessResponse(200, 'Category found successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}

const getBrandsByCategoryIdController = async(req,res) => {
    const {category_ids} = req.body;
    try {        
        const response = await getBrandsByCategoryIds(category_ids);
        const successResponse = createSuccessResponse(200, 'Brands found successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}

const getPlatformsBySectionIdController = async(req,res) => {
    const {section_id} = req.params;
    try {        
        const response = await getPlatformsBySectionId(section_id);
        const successResponse = createSuccessResponse(200, 'Platforms found successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}
const getAllBenchmarkController = async(req,res) => {
    try {        
        const response = await getAllBenchmark();
        const successResponse = createSuccessResponse(200, 'Category found successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}
const getAllPlatformController = async(req,res) => {
    try {        
        const response = await getAllPlatform();
        const successResponse = createSuccessResponse(200, 'Category found successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}
const getAllSectionController = async(req,res) => {
    try {        
        const response = await getAllSection();
        const successResponse = createSuccessResponse(200, 'Section found successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}
const getAllMetricsController = async(req,res) => {
    try {        
        const response = await getAllMetrics();
        const successResponse = createSuccessResponse(200, 'Category found successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}
const getMetricsByPlatformIdController = async(req,res) => {
    const {platform_ids} = req.body;
    try {        
        const response = await getMetricsByPlatformIds(platform_ids);
        const successResponse = createSuccessResponse(200, 'Platforms found successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}
const getAllFrequencyController = async(req,res) => {
    try {        
        const response = await getAllFrequency();
        const successResponse = createSuccessResponse(200, 'Category found successfully', response);
        return res.status(200).json(successResponse);
    } catch (error) {
        if (error instanceof ValidationError) {
            const errorResponse = createErrorResponse(400, error.code, error.message);
            return res.status(400).json(errorResponse);
          }
          const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Internal Server Error');
          return res.status(500).json(errorResponse);
        }
}



module.exports = {getAllCategoriesController, getAllBrandsController, getBrandsByCategoryIdController,getAllSectionController,getPlatformsBySectionIdController, getAllBenchmarkController, getAllPlatformController, getAllMetricsController,getMetricsByPlatformIdController, getAllFrequencyController};