const { users, payments } = require('../models'); // Assuming your models are exported correctly

async function doPayment(userData) {
    const { id, subscription_name, amount, storage, connection_allowed, payment_status } = userData;

    const user = await users.findOne({ where: { id } });

    if (!user) {
        throw new Error('User not found');
    }

    let paymentData = await payments.findOne({ where: { user_id: id } });

    if (paymentData) {
        // Update the existing payment record
        await paymentData.update({
            subscription_name,
            amount,
            storage,
            connection_allowed,
            payment_status
        });
    } else {
        // Create a new payment record
        paymentData = await payments.create({
            subscription_name,
            amount,
            storage,
            connection_allowed,
            user_id: id,
            payment_status
        });
    }

    return paymentData;
}


module.exports = {
    doPayment
};
