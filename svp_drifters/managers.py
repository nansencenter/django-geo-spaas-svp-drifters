import re
import numpy as np
import pandas as pd
import datetime
from dateutil.parser import parse

import pythesint as pti

from django.db import models
from django.utils import timezone
from django.contrib.gis.geos import LineString

from geospaas.utils import validate_uri, nansat_filename

from geospaas.vocabularies.models import Platform
from geospaas.vocabularies.models import Instrument
from geospaas.vocabularies.models import DataCenter
from geospaas.vocabularies.models import ISOTopicCategory
from geospaas.catalog.models import GeographicLocation
from geospaas.catalog.models import DatasetURI, Source, Dataset

# Demo uri: file://localhost/vagrant/shared/test_data/drifters/buoydata_15001_sep16.dat
class SVPDrifterManager(models.Manager):

    chunk_duration = 5

    def add_svp_drifters(self, uri_metadata, uri_data,
            time_coverage_start=None, time_coverage_end=None, maxnum=None):
        ''' Create all datasets from given file and add corresponding metadata

        Parameters:
        ----------
            uri_data : str
                URI to file
            uri_metadata : str
                URI to metadata file
            time_coverage_start : timezone.datetime object
                Optional start time for ingestion
            time_coverage_end : timezone.datetime object
                Optional end time for ingestion
        Returns:
        -------
            count : Number of ingested buoy datasets
        '''
        # set metadata
        pp = Platform.objects.get(short_name='BUOYS')
        ii = Instrument.objects.get(short_name = 'DRIFTING BUOYS')
        src = Source.objects.get_or_create(platform = pp, instrument = ii)[0]
        dc = DataCenter.objects.get(short_name = 'DOC/NOAA/OAR/AOML')
        iso = ISOTopicCategory.objects.get(name='Oceans')

        #'ID WMC_id experimentNumber BuoyType deploymentDate deploymetTime deplat deplon endDate endTime endlat endlon drogueLostDate drogueLostTime deathReason'
        #'11846540 4400770  2222    SVPB  2012/07/17 10:00   59.61   317.61 2015/11/29 15:00   57.66   352.24 2012/11/11 04:04  1\n'
        # Death reasons: 0=buoy still alive, 1=buoy ran aground, 2=picked up by
        # vessel, 3=stop transmitting, 4=sporadic transmissions, 5=bad
        # batteries, 6=inactive status

        ## Get and loop drifter identification numbers
        #id = np.loadtxt(metafile,usecols=(0,))
        #buoyType = np.loadtxt(metafile, usecols=(3,))
        ## load drifter deployment date
        #dep_date = np.loadtxt(metafile,usecols=(4,), dtype='str')
        ## load drifter deployment time
        #dep_time = np.loadtxt(metafile,usecols=(5,), dtype='str')

        metafile = nansat_filename(uri_metadata)
        data = pd.read_csv(metafile, sep="\s+", header = None, names=['id',
            'WMC_id', 'expNum', 'buoyType', 'depDate', 'depTime', 'depLat',
            'depLon', 'endDate', 'endTime', 'endLat', 'endLon',
            'drogueLostDate', 'drogueLostTime', 'deathReason'])
        ids = []
        for i in range(data.shape[0]):
            deploymentDateTime = timezone.datetime.strptime(
                                    data['depDate'][i] + 'T' +
                                    data['depTime'][i],
                                    '%Y/%m/%dT%H:%M').replace(tzinfo=
                                            timezone.utc)
            endDateTime = timezone.datetime.strptime(
                                    data['endDate'][i] + 'T' +
                                    data['endTime'][i],
                                    '%Y/%m/%dT%H:%M').replace(tzinfo=
                                            timezone.utc)
            # Skip drifter if it's not within the required timespan
            if (time_coverage_start and
                    endDateTime<time_coverage_start):
                continue
            if (time_coverage_end and
                    deploymentDateTime>time_coverage_end):
                continue
            # Split dataset in chunks
            t0 = deploymentDateTime
            while t0<endDateTime:
                # Add drifter trajectory and metadata to database
                t1 = t0 + timezone.timedelta(days=self.chunk_duration)
                ds, created = Dataset.objects.get_or_create(
                    entry_title = '%s drifter no. %d'%(data['buoyType'][i],
                        data['id'][i]),
                    ISO_topic_category = iso,
                    data_center = dc,
                    summary = '',
                    time_coverage_start = t0,
                    time_coverage_end = t1,
                    source=src)
                meta_uri, muc = DatasetURI.objects.get_or_create(uri=uri_metadata, dataset=ds)
                data_uri, duc = DatasetURI.objects.get_or_create(uri=uri_data, dataset=ds)
                t0 = t1
                if created:
                    ids.append(data['id'][i])
            if maxnum and i>=maxnum-1:
                break
            #ff.close()
        ids = list(set(ids))
        count = self.add_trajectories(ids, nansat_filename(uri_data))
        return count

    def add_trajectories(self, ids, datfile):
        ''' Add trajectories to database

        Read id, month, day, year, latitude, longitude, temperature, zonal velocity, meridional velocity, speed")
        '''
        data = pd.read_csv(datfile, sep="\s+", header = None, names=['id',
                'month', 'day', 'year', 'latitude', 'longitude', 'temp', 'u', 'v',
                'err_lat', 'err_lon', 'err_temp', 'unknown'])
        count = 0
        for id in ids:
            # Get indices of current drifter
            ind = np.where(data['id']==id)[0]
            lat = data['latitude'][ind]
            lon = data['longitude'][ind]
            year = data['year'][ind]
            month = data['month'][ind]
            day = data['day'][ind]
            hour = np.remainder(day, np.floor(day))*24
        
            # Pandas DataFrame - add lat,lon to df?
            df = pd.DataFrame({'year': year, 'month': month, 'day': np.floor(day),
                'hour': hour})
            # Create datetime64 array
            datetimes = pd.to_datetime(df)

            ## Get rid of missing data (=999.999)
            #indlon = np.where(lon<=360)[0]
            #indlat = np.where(lat<=90)[0]

            lat = lat[lon<=360]
            lon = lon[lon<=360]
            datetimes = datetimes[lon<=360]
            lat = lat[lat<=90]
            lon = lon[lat<=90]
            datetimes = datetimes[lat<=90]

            drifters = Dataset.objects.filter(entry_title__contains='drifter no. %d'%id)
            for d in drifters:
                lonlat = []
                lond = lon[datetimes>=d.time_coverage_start]
                lond = lond[datetimes<=d.time_coverage_end]
                latd = lat[datetimes>=d.time_coverage_start]
                latd = latd[datetimes<=d.time_coverage_end]

                if lond.size<=1 or latd.size<=1:
                    continue
                lonlat = zip(lond, latd)
                line1 = LineString((lonlat))
                geolocation = GeographicLocation.objects.get_or_create(geometry=line1)[0]
                d.geographic_location=geolocation
                d.save()
                count += 1
        return count

