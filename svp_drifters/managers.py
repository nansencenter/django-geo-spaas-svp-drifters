import re
import numpy as np
import pandas as pd
import datetime
import os
from dateutil.parser import parse

import pythesint as pti

from django.db import models
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

    CHUNK_DURATION = 5

    def set_metadata(self):
        pp = Platform.objects.get(short_name='BUOYS')
        ii = Instrument.objects.get(short_name='DRIFTING BUOYS')
        src = Source.objects.get_or_create(platform=pp, instrument=ii)[0]
        dc = DataCenter.objects.get(short_name='DOC/NOAA/OAR/AOML')
        iso = ISOTopicCategory.objects.get(name='Oceans')
        return src, dc, iso

    def shift_longitude(self, lon):
        return np.mod(float(lon) + 180, 360) - 180.

    def convert_datetime(self, month, daytime, year):
        daytime = float(daytime)
        day = int(daytime // 1)
        time = int((daytime % 1) / (1. / 24))
        return datetime.datetime(year=int(year), month=int(month), day=day, hour=time)

    def gen_file_name(self, metadata):
        time_start = ''.join(metadata[4:6]).replace('/', '').replace(':', '')
        time_end = ''.join(metadata[8:10]).replace('/', '').replace(':', '')
        file_name = '_'.join([metadata[3], metadata[0], time_start, time_end]) + '.csv'
        return file_name

    def read_metadata(self, path):
        arr = []
        with open(path, 'r') as meta_file:
            for line in meta_file:
                arr.append(line.strip().split())
        return arr

    def get_or_create(self, metadata_uri, data_uri,
                      time_coverage_start=None,
                      time_coverage_end=None,
                      maxnum=None, minlat=-90, maxlat=90, minlon=-180, maxlon=180,):
        """ Create all datasets from given file and add corresponding metadata

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
        """
        export_path = '/vagrant/shared/test_data'
        source, data_center, iso = self.set_metadata()
        metadata_file = nansat_filename(metadata_uri)
        data_file = nansat_filename(data_uri)
        print(metadata_file, data_file)

        convert_datetime_vctrz = np.vectorize(self.convert_datetime)
        data = []

        # Metadata info: http://www.aoml.noaa.gov/envids/gld/general_info/dir_table.php
        metadata = self.read_metadata(metadata_uri)

        with open(self.data_uri, 'r') as data_file:
            print('Open file: %s' % self.data_uri)
            for line in data_file:
                line = line.strip().split()
                if line[0] == metadata[0][0]:
                    data.append(line)
                else:
                    bouy = metadata.pop(0)
                    data = np.array(data)
                    # Export
                    file_name = self.gen_file_name(bouy)
                    print file_name
                    np.savetxt(os.path.join(export_path, file_name), data,
                               header=';'.join(self.COL_NAMES), fmt='%s', delimiter=';')
                    # Create timestamp from row data
                    timestamp = convert_datetime_vctrz(data[:, 1], data[:, 2], data[:, 3])
                    dt = datetime.timedelta(days=self.CHUNK_DURATION)
                    start = timestamp.min()
                    end = start + dt
                    while end < timestamp.max():
                        subset = data[(timestamp >= start) & (timestamp <= end)]
                        geometry = LineString(zip(data[:, 5], data[:, 4]))
                        start = end
                        end += dt

                        return subset, (start, end)

        return data, timestamp

        print 'Reading large files ...'
        names = ['id',
            'WMC_id', 'expNum', 'buoyType', 'depDate', 'depTime', 'depLat',
            'depLon', 'endDate', 'endTime', 'endLat', 'endLon',
            'drogueLostDate', 'drogueLostTime', 'deathReason']
        metadata = pd.read_csv(metafile,
                        delim_whitespace=True,
                        header = None,
                        names=names,
                        usecols=['id', 'buoyType', 'depDate', 'depTime', 'endDate', 'endTime'],
                        parse_dates={'depDateTime':['depDate', 'depTime'],
                                     'endDateTime':['endDate', 'endTime']}).to_records()
        data = pd.read_csv(datafile,
                            header=None,
                            delim_whitespace=True,
                            usecols=[0,1,2,3,4,5],
                            names=['id', 'month', 'day', 'year', 'latitude', 'longitude'],
                            ).to_records()
        longitude = np.mod(data['longitude']+180,360)-180.
        hour = np.remainder(data['day'], np.floor(data['day']))*24
        df = pd.DataFrame({'year': data['year'],
                           'month': data['month'],
                           'day': data['day'],
                           'hour': hour})
        dates = pd.to_datetime(df).as_matrix().astype('<M8[h]')
        print 'OK!'

        # set time_coverage_start/end as np.datetime64
        if time_coverage_start is None:
            time_coverage_start = metadata['depDateTime'].min()
        else:
            time_coverage_start = np.datetime64(time_coverage_start)
        if time_coverage_end is None:
            time_coverage_end = metadata['endDateTime'].max()
        else:
            time_coverage_end = np.datetime64(time_coverage_end)

        # select drifters matching given time period, i.e. which are
        # NOT taken only before or only after the given period
        ids = metadata['id'][~((metadata['endDateTime'] < time_coverage_start) +
                               (metadata['depDateTime'] > time_coverage_end))]
        cnt = 0
        for i, drifter_id in enumerate(ids[:maxnum]):
            buoyType = metadata['buoyType'][metadata['id'] == drifter_id][0]

            # find all valid drifter records for given period
            # Longitudes are shifted from range [0,360] to range [-180,180]
            # degrees
            gpi = ((data['id']==drifter_id) *
                   (longitude >= minlon) *
                   (longitude <= maxlon) *
                   (data['latitude'] >= minlat) *
                   (data['latitude'] <= maxlat) *
                   (dates >= time_coverage_start) *
                   (dates <= time_coverage_end))
            if len(gpi[gpi]) < 2:
                continue
            chunk_dates = np.arange(dates[gpi][0], dates[gpi][-1], self.CHUNK_DURATION*24)
            for j, chunk_date in enumerate(chunk_dates):
                print 'Add drifter #%d (%d/%d) on %s (%d/%d)' % (drifter_id, i, len(ids), str(chunk_date), j, len(chunk_dates))
                chunk_gpi = ((dates[gpi] >= chunk_date) *
                             (dates[gpi] < (chunk_date + self.CHUNK_DURATION*24)))
                if len(chunk_gpi[chunk_gpi]) < 2:
                    continue
                chunk_lon = longitude[gpi][chunk_gpi]
                chunk_lat = data['latitude'][gpi][chunk_gpi]
                geometry = LineString((zip(chunk_lon, chunk_lat)))
                geoloc, geo_cr = GeographicLocation.objects.get_or_create(geometry=geometry)
                if not geo_cr:
                    continue
                ds, ds_cr = Dataset.objects.get_or_create(
                    entry_title = '%s drifter no. %d' % (
                                buoyType,
                                drifter_id),
                    ISO_topic_category = iso,
                    data_center=data_center,
                    summary = '',
                    time_coverage_start = chunk_date.astype(datetime.datetime),
                    time_coverage_end = (chunk_date + self.CHUNK_DURATION*24).astype(datetime.datetime),
                    source=source,
                    geographic_location=geoloc)
                    
                if ds_cr:
                    cnt += 1
                    meta_uri, muc = DatasetURI.objects.get_or_create(uri=uri_metadata, dataset=ds)
                    data_uri, duc = DatasetURI.objects.get_or_create(uri=uri_data, dataset=ds)
        return cnt
