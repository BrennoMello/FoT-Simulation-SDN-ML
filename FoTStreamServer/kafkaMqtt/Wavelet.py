
class Wavelet (object):
	
	def __init__ (self):
		print("Init Wavelet")
	
	def pyramid(self, data, getLevel):
		levels = [0]
		j = 0
		for i in range(getLevel):
			level = []
			while (j <= len(data)-2):
				newData = (data[j] + data[j+1])/2
				level.append(round(newData, 2))
				j = j + 2
			levels.append(level)
		
		return levels
