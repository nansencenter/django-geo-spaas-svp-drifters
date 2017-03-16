print("Read drifter data")
print("Read id, month, day, year, latitude, longitude, temperature, zonal velocity, meridional velocity, speed")

import numpy as np

# load drifter id's
id=np.loadtxt('buoydata_15001_sep16.dat',usecols=(0,))

# load drifter date (month)
mon=np.loadtxt('buoydata_15001_sep16.dat',usecols=(1,))

# load drifter date (day)
day=np.loadtxt('buoydata_15001_sep16.dat',usecols=(2,))

# load drifter date (year)
year=np.loadtxt('buoydata_15001_sep16.dat',usecols=(3,))

# load drifter location (latitude)
lat=np.loadtxt('buoydata_15001_sep16.dat',usecols=(4,))

# load drifter location (longitude)
lon=np.loadtxt('buoydata_15001_sep16.dat',usecols=(5,))

# load temperature
temp=np.loadtxt('buoydata_15001_sep16.dat',usecols=(6,))

# load zonal velocity
zon=np.loadtxt('buoydata_15001_sep16.dat',usecols=(7,))

# load meridional velocity
mer=np.loadtxt('buoydata_15001_sep16.dat',usecols=(8,))

# load speed
speed=np.loadtxt('buoydata_15001_sep16.dat',usecols=(9,))

# load error (variance in latitude)
varlat=np.loadtxt('buoydata_15001_sep16.dat',usecols=(10,))

# load error (variance in longitude)
varlon=np.loadtxt('buoydata_15001_sep16.dat',usecols=(11,))

# load error (variance in temperature)
vartemp=np.loadtxt('buoydata_15001_sep16.dat',usecols=(12,))


#print('id',id)
#a=id.shape
#print('id',a)
#print(lon.shape)
#print(lon)
