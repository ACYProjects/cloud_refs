import tensorflow as tf
import numpy as np

# Generate some random data
np.random.seed(0)
X = np.random.randn(100, 3)
y = np.random.randint(0, 2, size=(100, 1))

# Define the logistic regression model
model = tf.keras.Sequential([
    tf.keras.layers.Dense(1, activation='sigmoid', input_shape=(3,))
])

# Compile the model
model.compile(optimizer='sgd', loss='binary_crossentropy', metrics=['accuracy'])

# Train the model
history = model.fit(X, y, epochs=100, verbose=0)

# Evaluate the model
loss, accuracy = model.evaluate(X, y, verbose=0)
print('Loss:', loss)
print('Accuracy:', accuracy)
