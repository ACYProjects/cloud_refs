import numpy as np
import tensorflow as tf
import matplotlib.pyplot as plt

# Generate some random data
num_points = 200
dimensions = 2
points = np.random.uniform(0, 1000, [num_points, dimensions])

# Define the number of clusters to test
k_values = range(1, 10)

# Define the k-means clustering function
def k_means(points, k):
    # Define the initial centroids randomly
    centroids = tf.Variable(tf.slice(tf.random.shuffle(points), [0, 0], [k, -1]))

    # Define the k-means clustering loop
    for _ in range(100):
        # Compute the distance between each point and each centroid
        distances = tf.reduce_sum(tf.square(tf.subtract(points, tf.expand_dims(centroids, axis=1))), axis=2)

        # Determine the closest centroid for each point
        assignments = tf.argmin(distances, axis=0)

        # Compute the mean of each cluster and update the centroids
        means = []
        for i in range(k):
            mean = tf.reduce_mean(tf.gather(points, tf.reshape(tf.where(tf.equal(assignments, i)), [1, -1])), axis=[1])
            means.append(mean)
        new_centroids = tf.concat(means, axis=0)
        centroids.assign(new_centroids)

    # Compute the loss (sum of squared distances from each point to its closest centroid)
    distances = tf.reduce_sum(tf.square(tf.subtract(points, tf.expand_dims(centroids, axis=1))), axis=2)
    loss = tf.reduce_sum(tf.reduce_min(distances, axis=0))

    return centroids, assignments, loss

# Use the elbow method to determine the optimal number of clusters
loss_values = []
for k in k_values:
    _, _, loss = k_means(points, k)
    loss_values.append(loss)

# Plot the loss values as a function of the number of clusters
plt.plot(k_values, loss_values.numpy())
plt.xlabel('Number of clusters')
plt.ylabel('Loss')
plt.title('Elbow Method')
plt.show()
