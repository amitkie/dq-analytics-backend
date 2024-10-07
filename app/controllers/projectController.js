const projectService = require("../services/projectService")
const { ValidationError } = require("../handlers/errorHandler");
const { createErrorResponse } = require("../utils/errorResponse");
const { createSuccessResponse } = require("../utils/successResponse");

const createProject = async (req, res) => {
  try {
    const response = await projectService.createProject(req.body);

    const successResponse = createSuccessResponse(200, 'Project created successfully', response);
    return res.status(200).json(successResponse);
  } catch (error) {
    console.error('Error in createProject:', error);

    // Handle known validation errors
    if (error.message === 'User not found') {
      const errorResponse = createErrorResponse(400, 'VALIDATION_ERROR', error.message);
      return res.status(400).json(errorResponse);
    }
    if (error.message === 'Project name already exists. Please choose a different name.') {
      const errorResponse = createErrorResponse(400, 'VALIDATION_ERROR', error.message);
      return res.status(400).json(errorResponse);
    }

    // Handle unknown errors
    const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'Project name already exists. Please choose a different name.', error?.message);
    return res.status(500).json(errorResponse);
  }
};
const updateProject = async (req, res) => {
  try {
    const { projectId } = req.params;
    const projectData = req.body;  // The new project data from the request body

    // Call the service to update the project
    const updatedProject = await projectService.updateProject(projectId, projectData);

    // Send success response
    const successResponse = createSuccessResponse(200, 'Project updated successfully', updatedProject);
    return res.status(200).json(successResponse);

  } catch (error) {
    console.error('Error in updateProject:', error);

    // Handle specific errors (e.g., Project not found)
    if (error.message === 'Project not found') {
      const errorResponse = createErrorResponse(404, 'VALIDATION_ERROR', error.message);
      return res.status(404).json(errorResponse);
    }

    // Handle other unknown errors
    const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'An error occurred while updating the project.', error.message);
    return res.status(500).json(errorResponse);
  }
};


const deleteProject = async (req, res) => {
  try {
    const { projectId } = req.params;

    // Call the service to delete the project
    const response = await projectService.deleteProject(projectId);

    // Send success response
    const successResponse = createSuccessResponse(200, 'Project deleted successfully', response);
    return res.status(200).json(successResponse);

  } catch (error) {
    console.error('Error in deleteProject:', error);

    // Handle specific errors (Project not found, etc.)
    if (error.message === 'Project not found') {
      const errorResponse = createErrorResponse(404, 'VALIDATION_ERROR', error.message);
      return res.status(404).json(errorResponse);
    }

    // Handle other unknown errors
    const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'An error occurred while deleting the project.', error.message);
    return res.status(500).json(errorResponse);
  }
};


const checkProjectName = async (req, res) => {
  const { project_name } = req.query; // Assuming the project_name comes in the query params

  if (!project_name) {
    return res.status(400).json({ message: 'Project name is required.' });
  }

  try {
    const result = await projectService.checkProjectNameAvailability(project_name);

    if (!result.available) {
      return res.status(409).json({ message: result.message });
    }

    return res.status(200).json({ message: 'Project name is available.' });
  } catch (error) {
    return res.status(500).json({ message: `Error: ${error.message}` });
  }
};

const getProjectByIdController = async (req, res) => {
  try {
    const { project_id } = req.query;

    if (!project_id) {
      return res.status(400).json({ message: 'project_id query parameter is required' });
    }

    const project = await projectService.getProjectById(project_id);

    return res.status(200).json({ project });
  } catch (error) {
    if (error.message === 'Project not found') {
      return res.status(404).json({ message: error.message });
    }
    // if (error.message === 'Project Benchmark has already been saved, you cannot create another instance.') {
    //   return res.status(400).json({ message: error.message });
    // }

    console.error('Error fetching project:', error);
    return res.status(500).json({ message: 'Internal Server Error' });
  }
};

const getProjectByUserIdController = async (req, res) => {
  try {
    const { user_id } = req.query;

    if (!user_id) {
      return res.status(400).json({ message: 'user_id query parameter is required' });
    }

    const project = await projectService.getProjectByUserId(user_id);

    return res.status(200).json({ project });
  } catch (error) {
    if (error.message === 'Project not found') {
      return res.status(404).json({ message: error.message });
    }

    console.error('Error fetching project:', error);
    return res.status(500).json({ message: 'Internal Server Error' });
  }
};

const saveMetrics = async (req, res) => {
  try {
    // const { project_id, isOverall, isCategory, metrics, weights, benchmarks } = req.body;

    // // Validate input
    // if (!project_id || !Array.isArray(metrics) || !Array.isArray(weights) || !Array.isArray(benchmarks)) {
    //     return res.status(400).json({ message: 'Invalid input data' });
    // }

    const newMetrics = await projectService.saveMetrics(
      req.body
    );
    const successResponse = createSuccessResponse(201, 'User activity tracked successfully', newMetrics)

    return res.status(200).json(successResponse);
  } catch (error) {
    console.error('Error saving metrics:', error);
    res.status(500).json({ message: 'Internal Server Error', error: error.message });
  }
};
const removeMetricFromProject = async (req, res) => {
  try {
    const { projectId, metricId } = req.params;

    // Call the service to delete the metric from the project
    const response = await projectService.removeMetricFromProject(projectId, metricId);

    // Send success response
    const successResponse = createSuccessResponse(200, 'Metric removed successfully from the project', response);
    return res.status(200).json(successResponse);

  } catch (error) {
    console.error('Error in removeMetricFromProject:', error);

    // Handle specific errors (Project not found, Metric not found, etc.)
    if (error.message === 'Project not found' || error.message === 'Metric not found in this project') {
      const errorResponse = createErrorResponse(404, 'VALIDATION_ERROR', error.message);
      return res.status(404).json(errorResponse);
    }

    // Handle other unknown errors
    const errorResponse = createErrorResponse(500, 'INTERNAL_SERVER_ERROR', 'An error occurred while removing the metric from the project.', error.message);
    return res.status(500).json(errorResponse);
  }
};

const createUrl = async (req, res) => {
  const { userId, tabName, urls } = req.body;

  try {
    // Find or create the user URL entry

    const userUrl = await projectService.createOrUpdateUrls(userId, tabName, urls)
    res.status(200).json({
      message: 'URLs created successfully',
      urls: userUrl.urls,
    });
  } catch (error) {
    console.error('Error creating or updating URLs:', error);
    res.status(500).json({ message: 'Error creating or updating URLs', error });
  }
};

const getUrl = async (req, res) => {
  const { userId } = req.query;

  try {
    const data = await projectService.getUrlsByUserId(userId);

    res.status(200).json({
      message: 'URLs fetched successfully',
      data
    });
  } catch (error) {
    console.error('Error fetching URLs:', error);
    res.status(500).json({
      message: 'Error fetching URLs',
      error: error.message
    });
  }
};

const getProject = async (req, res) => {
  const { frequency_id, category_id, project_id } = req.query;  // Use query params instead

  try {
    const project = await projectService.getProjectByIds(frequency_id, category_id, project_id);

    if (!project) {
      return res.status(404).json({ message: "Project not found" });
    }

    res.status(200).json(project);
  } catch (error) {
    res.status(500).json({ message: "Error fetching project", error: error.message });
  }
};

const createUserProjectDQScore = async (req, res) => {
  try {
    // Extract the array of DQ scores from the request body
    const dqScores = req.body;

    // Validate input: Ensure it's an array and not empty
    if (!Array.isArray(dqScores) || dqScores.length === 0) {
        return res.status(400).json({
            message: 'Invalid input. Expecting a non-empty array of DQ scores.'
        });
    }

    // Pass the DQ score array to the service for bulk creation
    const newScores = await projectService.createUserProjectDQScore(dqScores);

    return res.status(201).json({
        message: 'User Project DQ Scores created successfully',
        data: newScores
    });

} catch (error) {
    return res.status(500).json({
        message: 'Error creating User Project DQ Scores',
        error: error.message
    });
}
};





module.exports = { getProject, createProject, updateProject, removeMetricFromProject, createUserProjectDQScore, deleteProject, getProjectByIdController, getProjectByUserIdController, saveMetrics, checkProjectName, createUrl, getUrl };
