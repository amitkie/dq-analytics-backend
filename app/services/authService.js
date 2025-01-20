const bcrypt = require('bcrypt');
const { users, payments, userActivities } = require('../models/index'); // Assuming your models are exported correctly


async function registerUser(userData) {
  const { first_name, last_name, domain, email, password } = userData;

  // Check if a user with the provided email already existsemail
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

// async function authenticateUser(userData) {
//   const { email, password } = userData;
//   console.log(userData, "dfudufgduhugidhgihdfidifidfidififii");

//   // try {

//   // } catch (error) {

//   // }
//   const user = await users.findOne({ where: { email:email } });
//   console.log(user,'khigiguuffy');

//   if (!user) {
//       return null; // User not found
//   }

//   const isPasswordValid = await bcrypt.compare(password, user.password);
//   if (!isPasswordValid) {
//       return null; // Invalid password
//   }
//   const userId = user.id;

//   return {
//     userId
//   };
// }

async function authenticateUser(userData) {
  const { email, password } = userData;

  try {
    const user = await users.findOne({ where: { email: email } });

    if (!user) {
      return { error: 'User not found' };
    }

    const isPasswordValid = await bcrypt.compare(password, user.password);
    if (!isPasswordValid) {
      return { error: 'Incorrect Credentials' }; // Invalid password
    }

    const userId = user.id;
    return {
      userId
    };

  } catch (error) {
    throw error; // Re-throw the error or handle it appropriately
  }
}


async function getUserAndPaymentInfo(body) {
  const { userId } = body;
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
async function getUserInfo(body) {
  const { userId } = body;
  const user = await users.findOne({ where: { id: userId } });

  if (!user) {
    throw new Error('User not found');
  }

  return {
    user
  };
}

const nodemailer = require("nodemailer");

const sendFeedbackEmail = async ({ firstName, lastName, email, requestType, comments }) => {
  // Configure the transporter
  const transporter = nodemailer.createTransport({
    host: "smtp.gmail.com", 
    port: 465,
    secure: true,
    auth: {
      user: "guruhari1983@gmail.com", 
      pass: "ygnp vgrn vfyr uktu", 
    },

  });

  // Email content
  const mailOptions = {
    from: "guruhari1983@gmail.com",
    //  to: "hariharan.p@kiesquare.com", // Replace with the receiving email address
     to: "suryansh.s@kiesquare.com", // Replace with the receiving email address
    subject: "New Feedback Received",
    html: `
      <!DOCTYPE html>
    <html>
      <head>
        <style>
          body {
            font - family: Arial, sans-serif;
          background-color: #f4f4f9;
          color: #333;
          margin: 0;
          padding: 0;
                }
          .email-container {
            max - width: 600px;
          margin: 20px auto;
          background: #ffffff;
          border: 1px solid #ddd;
          border-radius: 8px;
          padding: 20px;
          box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                }
          .email-header {
            background - color: #4CAF50;
          color: white;
          text-align: center;
          padding: 10px 20px;
          border-top-left-radius: 8px;
          border-top-right-radius: 8px;
                }
          .email-header h1 {
            margin: 0;
          font-size: 18px;
                }
          .email-content {
            margin: 20px 0;
                }
          .email-content p {
            margin: 10px 0;
                }
          .email-footer {
            text - align: center;
          font-size: 12px;
          color: #666;
          margin-top: 20px;
                }
        </style>
      </head>
      <body>
        <div class="email-container">
          <div class="email-header">
            <h1>New Feedback Received</h1>
          </div>
          <div class="email-content">
            <p><strong>Name:</strong> ${firstName} ${lastName}</p>
            <p><strong>Email:</strong> ${email}</p>
            <p><strong>Request Type:</strong> ${requestType}</p>
            <p><strong>Comments:</strong></p>
            <p>${comments}</p>
          </div>
          <div class="email-footer">
            <p>Thank you for sharing your feedback with us!</p>
          </div>
        </div>
      </body>
    </html>`,
  };

  // Send the email
  await transporter.sendMail(mailOptions);
};

const demoScheduler = async ({  email}) => {
  // Configure the transporter
  const transporter = nodemailer.createTransport({
    host: "smtp.gmail.com", 
    port: 465,
    secure: true,
    auth: {
      user: "guruhari1983@gmail.com", 
      pass: "ygnp vgrn vfyr uktu", 
    },

  });

  // Email content
  const mailOptions = {
    from: "guruhari1983@gmail.com",
    //  to: "hariharan.p@kiesquare.com", // Replace with the receiving email address
     to: "suryansh.s@kiesquare.com", // Replace with the receiving email address
    subject: "New Feedback Received",
    html: `
    <!DOCTYPE html>
    <html>
    <head>
      <style>
        body {
          font-family: Arial, sans-serif;
          background-color: #f4f4f9;
          color: #333;
          margin: 0;
          padding: 0;
        }
        .email-container {
          max-width: 600px;
          margin: 20px auto;
          background: #ffffff;
          border: 1px solid #ddd;
          border-radius: 8px;
          padding: 20px;
          box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .email-header {
          background-color: #007bff;
          color: white;
          text-align: center;
          padding: 10px 20px;
          border-top-left-radius: 8px;
          border-top-right-radius: 8px;
        }
        .email-header h1 {
          margin: 0;
          font-size: 20px;
        }
        .email-content {
          margin: 20px 0;
        }
        .email-content p {
          margin: 10px 0;
        }
        .email-footer {
          text-align: center;
          font-size: 12px;
          color: #666;
          margin-top: 20px;
        }
      </style>
    </head>
    <body>
      <div class="email-container">
        <div class="email-header">
          <h1>New Demo Request</h1>
        </div>
        <div class="email-content">
          <p>Hello Team,</p>
          <p>A new user has requested a demo. Here are their details:</p>
          <p><strong>Email:</strong> ${email}</p>
        </div>
        <div class="email-footer">
          <p>Best Regards,<br>Your Automated System</p>
        </div>
      </div>
    </body>
    </html>`,
  };

  // Send the email
  await transporter.sendMail(mailOptions);
};


module.exports = {
  registerUser,
  authenticateUser,
  getUserAndPaymentInfo,
  getUserInfo,
  sendFeedbackEmail,
  demoScheduler
};
