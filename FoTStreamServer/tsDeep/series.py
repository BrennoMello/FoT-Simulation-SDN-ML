from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler

from keras.models import Sequential
from keras.layers import Dense, LSTM
from keras import backend as K
import tensorflow as tf


from matplotlib import pyplot as plt

import numpy as np
import pandas as pd

import math

class modelLSTM(object):
	
	def __init__(self, gateway):
		print('Init objetct LSTM')
		self.lstm_model = {}
		self.indexGraph = 0
		self.gateway = gateway
		self.difference_plot = []
		self.neurons = 4
		self.batch_size = 1
		self.nb_epoch = 100
		
		self.model = Sequential()
		self.model.add(LSTM(self.neurons, batch_input_shape=(self.batch_size,
												   1,
												   1), stateful=True))
		self.model.add(Dense(2))
		self.model.add(Dense(1))
		self.model.compile(loss='mean_squared_error', optimizer='adam')
		
		
		#self.Session = K.get_session()
		#self.Graph = tf.get_default_graph()
		#self.Graph.finalize()

		
	# frame a sequence as a supervised learning problem
	def timeseries_to_supervised(self, data, lag=1):
		df = pd.DataFrame(data)
		columns = [df.shift(i) for i in range(1, lag+1)]
		columns.append(df)
		df = pd.concat(columns, axis=1)
		df.fillna(0, inplace=True)

		return df


	# create a differenced series
	def difference(self, dataset, interval=1):
		diff = list()

		for i in range(interval, len(dataset)):
			value = dataset[i] - dataset[i - interval]
			diff.append(value)

		return pd.Series(diff)


	# invert differenced value
	def inverse_difference(self, history, yhat, interval=1):
		return yhat + history[-interval]


	# scale train and test data to [-1, 1]
	def scale(self, train, test):
		# fit scaler
		scaler = MinMaxScaler(feature_range=(-1, 1))
		scaler = scaler.fit(train)

		# transform train
		train = train.reshape(train.shape[0], train.shape[1])
		train_scaled = scaler.transform(train)

		# transform test
		test = test.reshape(test.shape[0], test.shape[1])
		test_scaled = scaler.transform(test)

		return scaler, train_scaled, test_scaled


	# inverse scaling for a forecasted value
	def invert_scale(self, scaler, X, value):
		new_row = [x for x in X] + [value]
		array = np.array(new_row)
		array = array.reshape(1, len(array))
		inverted = scaler.inverse_transform(array)

		return inverted[0, -1]


	# fit an LSTM network to training data
	def fit_lstm(self, train):
		X, y = train[:, 0:-1], train[:, -1]
		X = X.reshape(X.shape[0], 1, X.shape[1])
		
				
		for i in range(self.nb_epoch):
			self.model.fit(X, y, epochs=1, batch_size=self.batch_size,
					  verbose=1, shuffle=False)
			self.model.reset_states()

		return self.model


	# make a one-step forecast
	def forecast_lstm(self, model, X):
		
		# with self.Session.as_default():
		#	with self.Graph.as_default():
		X = X.reshape(1, 1, len(X))
		yhat = model.predict(X, batch_size=self.batch_size)

		return yhat[0, 0]
	
	def calc_rmse(self, input_data):
		series = pd.DataFrame(input_data)
		
		print("transform data to be stationary")
		raw_values = series.values
		diff_values = self.difference(raw_values, 1)
		
		print("transform data to be supervised learning") 
		supervised = self.timeseries_to_supervised(diff_values, 1)
		supervised_values = supervised.values
		print("supervised ")
		print(supervised_values)
		
		print("split data into train and test-sets")
		train, test = supervised_values[0:-12], supervised_values[-12:]
		print("Train")
		print(train)
		print("Test")
		print(test)
		
		print("transform the scale of the data")
		scaler, train_scaled, test_scaled = self.scale(train, test)
		
		print("forecast the entire training dataset to build up state for forecasting")
		train_reshaped = train_scaled[:, 0].reshape(len(train_scaled), 1, 1)
		# with self.Session.as_default():
		#	with self.Graph.as_default():
		print(self.lstm_model.predict(train_reshaped, batch_size=1))

		print("walk-forward validation on the test data")
		predictions = []

		for i in range(len(test_scaled)):
			print("make one-step forecast")
			X, y = test_scaled[i, 0:-1], test_scaled[i, -1]
			yhat = self.forecast_lstm(self.lstm_model, X)

			print("invert scaling")
			yhat = self.invert_scale(scaler, X, yhat)

			print("invert differencing")
			yhat = self.inverse_difference(raw_values, yhat, len(test_scaled)+1-i)

			print("store forecast")
			predictions.append(yhat)

			expected = raw_values[len(train) + i + 1]
			
			self.difference_plot.append(abs(expected-yhat))
			
			print(f'Month={i + 1}, Predicted={yhat}, Expected={expected}')

		print("report performance")
		rmse = math.sqrt(mean_squared_error(raw_values[-12:], predictions))
		print(f'Test RMSE: {rmse:.3f}')

		print("line plot of observed vs predicted")
		#plt.plot(raw_values[-12:], label='Observed')
		#plt.plot(predictions, label='Predicted')
		plt.plot(self.difference_plot, label='Difference')
		plt.legend(loc='upper right')
		#plt.xlabel("Instances")
		#plt.ylabel("Temperature")
		plt.xlabel("Instances")
		plt.ylabel("Difference Predicted x Observed")
		
		plt.savefig(self.gateway+'.png')
		#plt.savefig(self.gateway + 't'+str(self.indexGraph)+'.png')
		#self.indexGraph = self.indexGraph + 1
		plt.clf()
		#plt.show()
		
		
		#print("transform the scale of the data")
		#scaler, train_scaled, test_scaled = self.scale(train, test)
			

	
	def create_model(self, input_data):
		# load dataset
		#series = pd.read_csv('data/input/t.csv', header=0, parse_dates=[0], index_col=0, squeeze=True)
		series = pd.DataFrame(input_data)
		
		print("transform data to be stationary")
		raw_values = series.values
		diff_values = self.difference(raw_values, 1)

		print("transform data to be supervised learning") 
		supervised = self.timeseries_to_supervised(diff_values, 1)
		supervised_values = supervised.values
		
		print("split data into train and test-sets")
		train, test = supervised_values[0:-12], supervised_values[-12:]

		print("transform the scale of the data")
		scaler, train_scaled, test_scaled = self.scale(train, test)

		print("fit the model") #train, batch_size, nb_epoch, neurons
		self.lstm_model = self.fit_lstm(train_scaled)
		#self.Graph.finalize()
		
		
		print("forecast the entire training dataset to build up state for forecasting")
		train_reshaped = train_scaled[:, 0].reshape(len(train_scaled), 1, 1)
		#with self.Session.as_default():
		#	with self.Graph.as_default():
		self.lstm_model.predict(train_reshaped, batch_size=1)

		print("walk-forward validation on the test data")
		predictions = []

		for i in range(len(test_scaled)):
			print("make one-step forecast")
			X, y = test_scaled[i, 0:-1], test_scaled[i, -1]
			yhat = self.forecast_lstm(self.lstm_model, X)

			print("invert scaling")
			yhat = self.invert_scale(scaler, X, yhat)

			print("invert differencing")
			yhat = self.inverse_difference(raw_values, yhat, len(test_scaled)+1-i)

			print("store forecast")
			predictions.append(yhat)
			
			expected = raw_values[len(train) + i + 1]
			
			self.difference_plot.append(abs(expected-yhat))
			
			print(f'Month={i + 1}, Predicted={yhat}, Expected={expected}')

		print("report performance")
		rmse = math.sqrt(mean_squared_error(raw_values[-12:], predictions))
		print(f'Test RMSE: {rmse:.3f}')

		print("line plot of observed vs predicted")
		#plt.plot(raw_values[-12:], label='Observed')
		#plt.plot(predictions, label='Predicted')
		plt.plot(self.difference_plot, label='Difference')
		plt.legend(loc='upper right')
		#plt.xlabel("Instances")
		#plt.ylabel("Temperature")
		
		plt.xlabel("Instances")
		plt.ylabel("Difference Predicted x Observed")
		plt.savefig(self.gateway+'.png')
		#plt.savefig(self.gateway + 't'+str(self.indexGraph)+'.png')
		#self.indexGraph = self.indexGraph + 1
		plt.clf()
		#plt.show()
