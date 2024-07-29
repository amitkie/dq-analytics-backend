const { users, userActivities, } = require('../models'); // Assuming your models are exported correctly


async function userActivityTracker(userData) {
    const { id, tab_name } = userData;
    const user = await users.findOne({ where: { id } });

    if (!user) {
        throw new Error('User not found');
    }

    let userActivity = await userActivities.findOne({ where: { user_id: id } });

    if (!userActivity) {
        // Create a new activity record with the specified tab count set to 1
        const newActivity = {
            user_id: id,
            tab_page_speed: 0,
            tab_facebook_insight: 0,
            tab_google_analytics: 0,
            tab_google_ads: 0,
            tab_dv360: 0,
            is_db_created: false,
            is_schema_table_created: false
        };

        switch (tab_name) {
            case 'page_speed_insights':
                newActivity.tab_page_speed = 1;
                break;
            case 'facebook_insight':
                newActivity.tab_facebook_insight = 1;
                break;
            case 'google_analytics':
                newActivity.tab_google_analytics = 1;
                break;
            case 'google_ads':
                newActivity.tab_google_ads = 1;
                break;
            case 'dv360':
                newActivity.tab_dv360 = 1;
                break;
            default:
                throw new Error('Invalid tab name');
        }

        userActivity = await userActivities.create(newActivity);
    } else {
        let updatedCount;
        switch (tab_name) {
            case 'page_speed_insights':
                updatedCount = userActivity.tab_page_speed + 1;
                if (updatedCount > 3) {
                    throw new Error('Page speed insights count has reached its limit');
                }
                await userActivity.update({ tab_page_speed: updatedCount });
                break;
            case 'facebook_insight':
                updatedCount = userActivity.tab_facebook_insight + 1;
                if (updatedCount > 3) {
                    throw new Error('Facebook insight count has reached its limit');
                }
                await userActivity.update({ tab_facebook_insight: updatedCount });
                break;
            case 'google_analytics':
                updatedCount = userActivity.tab_google_analytics + 1;
                if (updatedCount > 3) {
                    throw new Error('Google Analytics count has reached its limit');
                }
                await userActivity.update({ tab_google_analytics: updatedCount });
                break;
            case 'google_ads':
                updatedCount = userActivity.tab_google_ads + 1;
                if (updatedCount > 3) {
                    throw new Error('Google Ads count has reached its limit');
                }
                await userActivity.update({ tab_google_ads: updatedCount });
                break;
            case 'dv360':
                updatedCount = userActivity.tab_dv360 + 1;
                if (updatedCount > 3) {
                    throw new Error('DV360 count has reached its limit');
                }
                await userActivity.update({ tab_dv360: updatedCount });
                break;
            default:
                throw new Error('Invalid tab name');
        }
    }

    return userActivity;
}

const updateDBCreationStatus = async (data) => {
    const { id, is_db_created } = data;

    const userActivity = await userActivities.findOne({ where: { user_id: id } });

    if (!userActivity) {
        throw new Error('User activity not found');
    }

    await userActivity.update({ is_db_created });
    return userActivity;
};

// Service to update the schema and table creation status
const updateSchemaTableCreationStatus = async (data) => {
    const { id, is_schema_table_created } = data;

    const userActivity = await userActivities.findOne({ where: { user_id: id } });

    if (!userActivity) {
        throw new Error('User activity not found');
    }

    await userActivity.update({ is_schema_table_created });
    return userActivity;
};

const getDBAndSchemaTableCreationStatusService = async (user_id) => {
    const userActivity = await userActivities.findOne({ where: { user_id } });

    if (!userActivity) {
        throw new Error('User activity not found');
    }

    return {
        is_db_created: userActivity.is_db_created,
        is_schema_table_created: userActivity.is_schema_table_created,
    };
};

module.exports = {
    userActivityTracker,
    updateDBCreationStatus,
    updateSchemaTableCreationStatus,
    getDBAndSchemaTableCreationStatusService
};
