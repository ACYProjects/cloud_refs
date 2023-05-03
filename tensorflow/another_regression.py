import tensorflow as tf
import numpy as np

# Generate some random data
np.random.seed(0)
X = np.random.randn(100, 3)
y = np.random.randn(100, 1)

# Define the linear regression model
model = tf.keras.Sequential([
    tf.keras.layers.Dense(1, input_shape=(3,))
])

# Compile the model
model.compile(optimizer='sgd', loss='mse')

# Train the model
history = model.fit(X, y, epochs=100, verbose=0)

# Evaluate the model
loss = model.evaluate(X, y, verbose=0)
print('Loss:', loss)
