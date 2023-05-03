import tensorflow as tf
import numpy as np

# Create some sample data
x_data = np.random.rand(100).astype(np.float32)
y_data = x_data * 0.1 + 0.3

# Define placeholders for the input data
x = tf.placeholder(tf.float32, shape=[None])
y = tf.placeholder(tf.float32, shape=[None])

W = tf.Variable(tf.random_normal([1]), name='weight')
b = tf.Variable(tf.zeros([1]), name='bias')

y_pred = tf.add(tf.multiply(x, W), b)

loss = tf.reduce_mean(tf.square(y_pred - y))

optimizer = tf.train.GradientDescentOptimizer(0.5)

train_op = optimizer.minimize(loss)

with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())

    # Train the model for 100 epochs
    for epoch in range(100):
        _, l = sess.run([train_op, loss], feed_dict={x: x_data, y: y_data})
        if epoch % 10 == 0:
            print('Epoch {0}: loss = {1}'.format(epoch, l))

    # Print the final model parameters
    W_, b_ = sess.run([W, b])
    print('Final model parameters: W = {0}, b = {1}'.format(W_, b_))
