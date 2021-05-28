from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, r2_score


from keras.models import load_model
from keras.models import Sequential
from keras.layers import Dense, LSTM
from keras import backend as K
from datetime import datetime

import tensorflow as tf

from keras.models import model_from_json
import tensorflow as tf
from keras.models import load_model

import gc

from matplotlib import pyplot as plt
from memory_profiler import profile

import numpy as np
import pandas as pd
from pandas import concat

import time
import json
import math

class modelLSTM(object):
	
	def __init__(self, gateway):
		print('Init objetct LSTM')
		self.lstm_model = {}
		self.indexGraph = 0
		self.gateway = gateway
		self.difference_plot = []
		self.neurons = 4
		self.batch_size =1
		self.nb_epoch = 100
		self.init_model_status = False
		self.path_log = ""
		
		#global graph
		#graph = tf.get_default_graph()
		
		self.model = self.create_model_denses()
		
		self.Session = K.get_session()
		self.Graph = tf.get_default_graph()
		
		self.create_file_log()
		#self.Graph.finalize()

		
	# frame a sequence as a supervised learning problem
	def timeseries_to_supervised(self, data, lag=1):
		df = pd.DataFrame(data)
		columns = [df.shift(i) for i in range(1, lag+1)]
		columns.append(df)
		df = pd.concat(columns, axis=1)
		df.fillna(0, inplace=True)

		return df

	# convert series to supervised learning
	def series_to_supervised(self, data, n_in=1, n_out=1, dropnan=True):
		n_vars = 1 if type(data) is list else data.shape[1]
		df = pd.DataFrame(data)
		cols, names = list(), list()
		# input sequence (t-n, ... t-1)
		for i in range(n_in, 0, -1):
			cols.append(df.shift(i))
			names += [('var%d(t-%d)' % (j+1, i)) for j in range(n_vars)]
		# forecast sequence (t, t+1, ... t+n)
		for i in range(0, n_out):
			cols.append(df.shift(-i))
			if i == 0:
				names += [('var%d(t)' % (j+1)) for j in range(n_vars)]
			else:
				names += [('var%d(t+%d)' % (j+1, i)) for j in range(n_vars)]
		# put it all together
		agg = concat(cols, axis=1)
		agg.columns = names
		# drop rows with NaN values
		if dropnan:
			agg.dropna(inplace=True)
		return agg
	
	
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
	
	def create_model_denses(self):
		
		model = Sequential()
		model.add(LSTM(self.neurons, batch_input_shape=(self.batch_size,
												   1,
												   3), stateful=True))
		model.add(Dense(2))
		model.add(Dense(1))
		model.compile(loss='mean_squared_error', optimizer='adam')
		
		print ("create model")
		
		return model
	
	@profile
	def save_model(self, model, gateway_model):
		# serialize model to JSON
		#model_json = model.to_json()
		#with open("model-"+ gateway_model +".json", "w") as json_file:
		#	json_file.write(model_json)
		# serialize weights to HDF5
		#model.save_weights("model.h5")
		#with self.Session.as_default():
		#	with self.Graph.as_default():	
		#K.clear_session()
		#self.Graph = tf.get_default_graph()
		#with self.Session.as_default():
		#	with self.Graph.as_default():
				#model.save("model-" + gateway_model +".h5")
		model.save_weights("model-" + gateway_model +".h5")
		#self.Graph.finalize()
		#tf.reset_default_graph()
		K.clear_session()
		tf.reset_default_graph()
		gc.collect()
		del self.model
		del model
		gc.collect()
		
		print("Saved model to disk " + gateway_model)
		
	@profile	
	def open_model(self, gateway_model):
		# load json and create model
		# print("model-"+ gateway_model +".json")
		print("model-"+ gateway_model +".h5")
		K.clear_session()
		tf.reset_default_graph()
		
		#time.sleep(5)
		
		gc.collect()
		
		self.Graph = tf.get_default_graph()
		#with self.Session.as_default():
		#	with self.Graph.as_default():	
				#json_file = open("model-"+ gateway_model +".json", 'r')
				#loaded_model_json = json_file.read()
				#json_file.close()
				#loaded_model = model_from_json(loaded_model_json)
				#loaded_model.compile(loss='mean_squared_error', optimizer='adam')
				# load weights into new model
				#loaded_model.load_weights("model-"+ gateway_model +".h5")
		
		
		#with self.Session.as_default():
		#	with self.Graph.as_default():			
		self.model = self.create_model_denses()
				#loaded_model = load_model("model-"+ gateway_model +".h5")
		self.model.load_weights("model-"+ gateway_model +".h5")
				
		print("Loaded model from disk")
		
		
		# load model
		#model = load_model("model-" + gateway_model +".h5")
		# summarize model.
		#loaded_model.compile(loss='mean_squared_error', optimizer='adam')
		self.model.summary()
		print("open")
		return self.model

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
		

		#with self.Session.as_default():
		#	with self.Graph.as_default():		
		for i in range(self.nb_epoch):
			self.model.fit(X, y, epochs=1, batch_size=self.batch_size,verbose=1, shuffle=False)
			self.model.reset_states()
		
		
		return self.model


	# make a one-step forecast
	#@profile
	def forecast_lstm(self, X):
		X = X.reshape(1, 1, len(X))
		#K.clear_session()
		#with self.Session.as_default():
		#	with self.Graph.as_default():
		
		yhat = self.model.predict(X, batch_size=self.batch_size)

		return yhat[0, 0]
	@profile
	def clean_model(self):
		K.clear_session()
		tf.reset_default_graph()
		gc.collect()
		del self.model
		gc.collect()
	
	def create_file_log(self):
		self.path_log = self.gateway+"-server-log-lstm-"+'Timestamp-{:%Y-%m-%d-%H-%M-%S}'.format(datetime.now())
		f = open(self.path_log, "w+")
		f.close()
	
	def save_file_log(self, json_log):
		print ("save log")
		file_log = open(self.path_log, "a")
		#file_log.write(json_log)
		file_log.write(json.dumps(json_log))
		file_log.write('\n')
		#json.dump(json_log, file_log)
		file_log.close()
	
	def permance_calc(self, raw_values, predictions):
		print("report performance")
		rmse = math.sqrt(mean_squared_error(raw_values, predictions))
		print(f'Test RMSE: {rmse:.3f}')
		mae = mean_absolute_error(raw_values, predictions)
		print('calc mae: ' + str(mae)) 
		mse = mean_squared_error(raw_values, predictions)
		print('calc mse: ' + str(mse)) 
		r2 = r2_score(raw_values, predictions)
		print('r2_score: ' + str(r2))
		print("raw_values")
		print(raw_values)
		print("predictions")
		print(predictions)
		json_log = {"name_gateway": self.gateway,"rmse": rmse, "mae": mae, "mse": mse, "r2_score": r2, "raw_values": raw_values, "predictions": predictions}
		print(json_log)
		self.save_file_log(json_log)
		 
	@profile
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
		#with self.Session.as_default():
		#	with self.Graph.as_default():
		self.model = self.open_model(self.gateway)
		
		#with self.Session.as_default():
		#	with self.Graph.as_default():
		print(self.model.predict(train_reshaped, batch_size=1))

		print("walk-forward validation on the test data")
		predictions = []
		expections = []
		for i in range(len(test_scaled)):
			print("make one-step forecast")
			X, y = test_scaled[i, 0:-1], test_scaled[i, -1]
			yhat = self.forecast_lstm(X)

			print("invert scaling")
			yhat = self.invert_scale(scaler, X, yhat)

			print("invert differencing")
			yhat = self.inverse_difference(raw_values, yhat, len(test_scaled)+1-i)

			print("store forecast")
			predictions.append(yhat[0])

			expected = raw_values[len(train) + i + 1]
			expections.append(expected[0])
			
			#self.difference_plot.append(abs(expected-yhat))
			
			print(f'Month={i + 1}, Predicted={yhat}, Expected={expected}')

		
		self.permance_calc(expections, predictions)
		#print("report performance")
		#rmse = math.sqrt(mean_squared_error(raw_values[-12:], predictions))
		
		#print(f'Test RMSE: {rmse:.3f}')
		
		#self.save_model(self.model, self.gateway)
		self.clean_model()
		
		#print("line plot of observed vs predicted")
		#now = datetime.now()
		#timestamp = datetime.timestamp(now)
		print('Timestamp: {:%Y-%m-%d %H:%M:%S}'.format(datetime.now()))
		
		#plt.plot(raw_values[-12:], label='Observed')
		#plt.plot(predictions, label='Predicted')
		#plt.plot(self.difference_plot, label='Difference', marker='o')

		#plt.legend(loc='upper right')
		#plt.xlabel("Instances")
		#plt.ylabel("Temperature")
		#plt.xlabel("Instances")
		#plt.ylabel("Difference Predicted x Observed")
		
		#plt.savefig(self.gateway+'.png')
		#plt.savefig(self.gateway + 't'+str(self.indexGraph)+'.png')
		#self.indexGraph = self.indexGraph + 1
		#plt.clf()
		#plt.show()
		
		
		#print("transform the scale of the data")
		#scaler, train_scaled, test_scaled = self.scale(train, test)
			

	@profile
	def create_model(self, input_data):
		# load dataset
		#series = pd.read_csv('data/input/t.csv', header=0, parse_dates=[0], index_col=0, squeeze=True)
		#series = pd.DataFrame(input_data)
		
		#print("transform data to be stationary")
		#raw_values = series.values
		#diff_values = self.difference(raw_values, 1)

		print("transform data to be supervised learning") 
		#supervised = self.timeseries_to_supervised(diff_values, 1)
		supervised = self.series_to_supervised(input_data)
		supervised.drop(supervised.columns[[3, 4]], axis=1, inplace=True)
		print(supervised)
		supervised_values = supervised.values
		
		print("split data into train and test-sets")
		train, test = supervised_values[0:-12], supervised_values[-12:]

		print(train)

		print("transform the scale of the data")
		scaler, train_scaled, test_scaled = self.scale(train, test)
		print(train_scaled.shape)

		if(self.init_model_status):
			print("Print before of init "  + str(self.init_model_status))
			self.model = self.open_model(self.gateway)
			
			print("fit the model") #train, batch_size, nb_epoch, neurons
			self.model = self.fit_lstm(train_scaled)
		
		else:	
			#with self.Session.as_default():
			#	with self.Graph.as_default():
			self.model = self.create_model_denses()
			
			print("fit the model only") #train, batch_size, nb_epoch, neurons
			self.model = self.fit_lstm(train_scaled)
		
		print("forecast the entire training dataset to build up state for forecasting")
		train_reshaped = train_scaled[:, 0].reshape(len(train_scaled), 1, 3)
		
		self.model.predict(train_reshaped, batch_size=1)


		print("walk-forward validation on the test data")
		predictions = []
		expections = []
		self.save_model(self.model, self.gateway)
		self.open_model(self.gateway)
		for i in range(len(test_scaled)):
			print("make one-step forecast")
			X, y = test_scaled[i, 0:-1], test_scaled[i, -1]

			yhat = self.forecast_lstm(X)

			print("invert scaling")
			yhat = self.invert_scale(scaler, X, yhat)

			print("invert differencing")
			yhat = self.inverse_difference(raw_values, yhat, len(test_scaled)+1-i)

			print("store forecast")
			predictions.append(yhat[0])
			
			expected = raw_values[len(train) + i + 1]
			expections.append(expected[0])
			
			print("expected")
			print(expected)
			
			print("yhat")
			print (yhat[0])
			
			#self.difference_plot.append(abs(expected-yhat))
			
			print(f'Month={i + 1}, Predicted={yhat}, Expected={expected}')
		
		
		self.permance_calc(expections, predictions)
		#print("report performance")
		#rmse = math.sqrt(mean_squared_error(raw_values[-12:], predictions))
		#print(f'Test RMSE: {rmse:.3f}')
		
		#self.save_model(self.model, self.gateway)
		self.init_model_status = True
		self.clean_model()
		
		#print("line plot of observed vs predicted")
		
		# current date and time
		#now = datetime.now()
		#timestamp = datetime.timestamp(now)
		#print("timestamp =", timestamp)
		print('Timestamp: {:%Y-%m-%d %H:%M:%S}'.format(datetime.now()))
		#plt.plot(raw_values[-12:], label='Observed')
		#plt.plot(predictions, label='Predicted')
		#plt.plot(self.difference_plot, label='Difference', marker='o')
		#plt.legend(loc='upper right')
		#plt.xlabel("Instances")
		#plt.ylabel("Temperature")
		
		#plt.xlabel("Instances")
		#plt.ylabel("Difference Predicted x Observed")
		#plt.savefig(self.gateway+'.png')
		#plt.savefig(self.gateway + 't'+str(self.indexGraph)+'.png')
		#self.indexGraph = self.indexGraph + 1
		#plt.clf()
		#plt.show()
