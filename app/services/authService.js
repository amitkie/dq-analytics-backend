const bcrypt = require('bcrypt');
const { users, payments, userActivities } = require('../models/index'); // Assuming your models are exported correctly


async function registerUser(userData) {
  const { first_name, last_name, domain, email, password } = userData;

  // Check if a user with the provided email already exists
  const existingUser = await users.findOne({ where: { email } });
  if (existingUser) {
    throw new Error('User with this email already exists');
  }

  // Hash the password before storing
  const hashedPassword = await bcrypt.hash(password, 10);

  const newUser = await users.create({ first_name, last_name, domain, email, password: hashedPassword });
  await payments.create({
    subscription_name: null,
    amount: null,
    storage: null,
    connection_allowed: null,
    payment_status: 'Incomplete',
    user_id: newUser.id
});
const userActivityData = {
  user_id: newUser.id,
  tab_page_speed: 0,
  tab_facebook_insight: 0,
  tab_google_analytics: 0,
  tab_google_ads: 0,
  tab_dv360: 0,
  is_db_created: false,
  is_schema_table_created: false,
};

await userActivities.create(userActivityData);

  return newUser;
}

async function authenticateUser(userData) {
  const { email, password } = userData;
  console.log(userData, "dfudufgduhugidhgihdfidifidfidififii");

  // try {
    
  // } catch (error) {
    
  // }
  const user = await users.findOne({ where: { email:email } });
  console.log(user,'khigiguuffy');

  if (!user) {
      return null; // User not found
  }

  const isPasswordValid = await bcrypt.compare(password, user.password);
  if (!isPasswordValid) {
      return null; // Invalid password
  }
  const userId = user.id;

  return {
    userId
  };
}

async function getUserAndPaymentInfo(body) {
  const {userId} = body;
  const user = await users.findOne({ where: { id: userId } });

  if (!user) {
    throw new Error('User not found');
  }

  const paymentInfo = await payments.findOne({ where: { user_id: userId } });

  return {
    user,
    paymentInfo,
  };
}

module.exports = {
  registerUser,
  authenticateUser,
  getUserAndPaymentInfo
};
